# -*- coding: utf-8 -*-
import serial
import time
import threading
import sys
import re  # For EasyComm Parser
import socket  # For TCP Server
# import argparse # REMOVED: No longer needed for command line args
import math
import logging
import signal  # For clean shutdown

# pynput is only needed for calibration
try:
    from pynput import keyboard

    pynput_available = True
except ImportError:
    pynput_available = False

# --- Configuration ---
# PLEASE ADJUST:
SERIAL_PORT = 'COM4'  # Serial port of your USB-RS485 adapter
SERIAL_BAUDRATE = 2400  # Baud rate for rotor communication - ADJUST!
PELCO_D_ADDRESS = 1  # Address of your rotor (1-255)

# Network server configuration for Gpredict
TCP_HOST = '0.0.0.0'  # Listen on all interfaces (or '127.0.0.1' only for local)
TCP_PORT = 4533  # Standard port for rotctld/EasyComm

# Pelco-D speeds (for calibration)
PELCO_PAN_SPEED = 0x3f
PELCO_TILT_SPEED = 0x3f

# Query interval for position (in seconds)
QUERY_INTERVAL = 0.8

# Estimated maximum movement duration (in seconds)
# Adjust this value if a 180° rotation takes longer.
ESTIMATED_MAX_MOVE_DURATION = 10.0

# --- Mapping constants based on determined values ---
# Raw values of the rotor
RAW_EL_AT_90_DEG = 0.0  # Raw value at physical 90° (Zenith)
RAW_EL_AT_20_DEG = 51.5  # Raw value at physical 20° (lower limit)
# Corresponding display values (Display)
DISP_EL_FOR_90_DEG = 90.0
DISP_EL_FOR_20_DEG = 20.0

# Calculate slope (m) and intercept (b) for Display = m * Raw + b
if abs(RAW_EL_AT_90_DEG - RAW_EL_AT_20_DEG) < 0.01:
    # Logging is not yet configured here, so use print
    print("FATAL: Raw values for 20° and 90° are (almost) identical! Mapping impossible.")
    sys.exit(1)
EL_MAP_SLOPE_M = (DISP_EL_FOR_90_DEG - DISP_EL_FOR_20_DEG) / (RAW_EL_AT_90_DEG - RAW_EL_AT_20_DEG)
EL_MAP_INTERCEPT_B = DISP_EL_FOR_90_DEG - EL_MAP_SLOPE_M * RAW_EL_AT_90_DEG

# Raw value limits for clamping
ROTOR_RAW_EL_MIN = min(RAW_EL_AT_90_DEG, RAW_EL_AT_20_DEG)
ROTOR_RAW_EL_MAX = max(RAW_EL_AT_90_DEG, RAW_EL_AT_20_DEG)
# ----------------------------------------------------------

# --- Global Variables ---
# Logger is initialized in __main__
log = None
ser = None
stop_event = threading.Event()

position_lock = threading.Lock()
command_lock = threading.Lock()  # Lock for serial commands (send/query)

raw_azimuth = 0.0
raw_elevation = 0.0
# azimuth_offset represents the RAW value when the rotor points to physical 0° (North)
azimuth_offset = 0.0  # Will be determined during calibration
display_azimuth = 0.0
display_elevation = 0.0
last_query_ok = True

# Global variables to store the last target position from Gpredict
last_target_az = None
last_target_el = None

# Timestamp until which we assume the rotor is moving
rotor_moving_until = 0.0

server_socket = None
client_threads = []


# --- Pelco-D Helper Functions ---
def calculate_checksum(message_bytes):
    """Calculates the Pelco-D checksum."""
    if len(message_bytes) < 6: return 0
    return sum(message_bytes[1:6]) % 256


def build_pelco_d_command(address, command1, command2, data1, data2):
    """Builds a 7-byte Pelco-D command."""
    msg = bytearray(7)
    msg[0] = 0xFF
    msg[1] = address
    msg[2] = command1
    msg[3] = command2
    msg[4] = data1
    msg[5] = data2
    msg[6] = calculate_checksum(msg)
    return bytes(msg)


def send_pelco_command(command):
    """Sends a Pelco-D command via the serial port. Assumes command_lock is held."""
    global ser, log
    if ser and ser.is_open:
        try:
            bytes_written = ser.write(command)
            if bytes_written == len(command):
                if log: log.debug(f"Sent: {command.hex()}")
                return True
            else:
                if log: log.warning(f"Could not send all bytes ({bytes_written}/{len(command)}): {command.hex()}")
                return False
        except serial.SerialTimeoutException:
            if log: log.warning(f"Timeout while sending: {command.hex()}")
            return False
        except serial.SerialException as e:
            if log: log.error(f"Error sending to rotor ({command.hex()}): {e}")
            return False
        except Exception as e:
            if log: log.error(f"Unexpected error while sending ({command.hex()}): {e}")
            return False
    else:
        if not stop_event.is_set() and ser:
            if log: log.warning("Attempted to send, but serial connection not ready.")
        return False


# --- Pelco-D Commands ---
CMD_STOP = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, 0x00, 0x00, 0x00)
CMD_UP = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, 0x08, 0x00, PELCO_TILT_SPEED)
CMD_DOWN = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, 0x10, 0x00, PELCO_TILT_SPEED)
CMD_LEFT = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, 0x04, PELCO_PAN_SPEED, 0x00)
CMD_RIGHT = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, 0x02, PELCO_PAN_SPEED, 0x00)

CMD_QUERY_PAN = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, 0x51, 0x00, 0x00)
CMD_QUERY_TILT = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, 0x53, 0x00, 0x00)

CMD2_SET_PAN = 0x4B
CMD2_SET_TILT = 0x4D


# --- Helper function to query rotor position (WITH COMMAND_LOCK) ---
def query_rotor_position_once():
    """Queries azimuth and elevation ONCE. Holds the command_lock."""
    global ser, command_lock, stop_event, log
    az_read = None
    el_read = None
    query_success = False
    wait_after_send = 0.15
    wait_between_queries = 0.1
    wait_before_reset = 0.02

    if stop_event.is_set():
        return None, None, False

    with command_lock:
        if not ser or not ser.is_open:
            if not stop_event.is_set():
                if log: log.warning("query_rotor_position_once: Serial connection not open.")
            return None, None, False

        # --- Query Azimuth ---
        az_success = False
        try:
            time.sleep(wait_before_reset)
            ser.reset_input_buffer()
            if send_pelco_command(CMD_QUERY_PAN):
                time.sleep(wait_after_send)
                if ser.in_waiting >= 7:
                    response = ser.read(7)
                    if log: log.debug(f"Read AZ Response: {response.hex()}")
                    if len(response) == 7 and response[0] == 0xFF and response[1] == PELCO_D_ADDRESS and response[
                        3] == 0x59:
                        checksum_received = response[6]
                        checksum_calculated = calculate_checksum(response)
                        if checksum_received == checksum_calculated:
                            pos_hundredths = (response[4] << 8) + response[5]
                            az_read = pos_hundredths / 100.0
                            az_success = True
                        else:
                            if log: log.warning(
                                f"AZ Checksum error: Got {checksum_received:02x}, expected {checksum_calculated:02x}")
                    else:
                        if log: log.warning(f"Invalid AZ response format: {response.hex()}")
        except serial.SerialException as e:
            if log: log.error(f"Serial error during AZ read: {e}")
        except Exception as e:
            if log: log.error(f"Unexpected error during AZ read: {e}")

        time.sleep(wait_between_queries)

        # --- Query Elevation ---
        el_success = False
        try:
            time.sleep(wait_before_reset)
            ser.reset_input_buffer()
            if send_pelco_command(CMD_QUERY_TILT):
                time.sleep(wait_after_send)
                if ser.in_waiting >= 7:
                    response = ser.read(7)
                    if log: log.debug(f"Read EL Response: {response.hex()}")
                    if len(response) == 7 and response[0] == 0xFF and response[1] == PELCO_D_ADDRESS and response[
                        3] == 0x5B:
                        checksum_received = response[6]
                        checksum_calculated = calculate_checksum(response)
                        if checksum_received == checksum_calculated:
                            pos_hundredths = (response[4] << 8) + response[5]
                            el_read = pos_hundredths / 100.0
                            el_success = True
                        else:
                            if log: log.warning(
                                f"EL Checksum error: Got {checksum_received:02x}, expected {checksum_calculated:02x}")
                    else:
                        if log: log.warning(f"Invalid EL response format: {response.hex()}")
        except serial.SerialException as e:
            if log: log.error(f"Serial error during EL read: {e}")
        except Exception as e:
            if log: log.error(f"Unexpected error during EL read: {e}")

        query_success = az_success and el_success

    return az_read, el_read, query_success


# --- Calibration Function ---
def calibrate_rotor():
    """Performs the interactive calibration."""
    global azimuth_offset, position_lock, stop_event, command_lock, log
    global display_azimuth, display_elevation, raw_azimuth, raw_elevation

    # --- Automatic movement to 90° Elevation ---
    if log: log.info("--- Start Calibration: Automatically moving to 90° Elevation (Zenith) ---")
    print("Automatically moving to 90° Elevation...")
    target_raw_el_for_90_disp = RAW_EL_AT_90_DEG
    if log: log.info(f"Target raw value for 90° Elevation is {target_raw_el_for_90_disp:.1f}")

    try:
        el_hundredths = int(round(target_raw_el_for_90_disp * 100))
        el_hundredths = max(0, min(35999, el_hundredths))
        el_msb = (el_hundredths >> 8) & 0xFF
        el_lsb = el_hundredths & 0xFF
        cmd_set_el_calib = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, CMD2_SET_TILT, el_msb, el_lsb)

        with command_lock:
            if log: log.info(f"Sending command to move to raw Elevation {target_raw_el_for_90_disp:.1f}°.")
            if not send_pelco_command(cmd_set_el_calib):
                if log: log.error("Error sending initial elevation command.")
                print("ERROR: Could not send initial elevation command!")
                return False
            if log: log.debug("Waiting 2 seconds after Set Elevation command...")
            time.sleep(2.0)

        wait_start_time = time.monotonic()
        wait_timeout = 45.0
        position_reached = False
        consecutive_failures = 0
        max_consecutive_failures = 8

        if log: log.info("Waiting to reach 90° Elevation...")

        while time.monotonic() - wait_start_time < wait_timeout:
            if stop_event.is_set():
                if log: log.info("Waiting for elevation aborted.")
                break
            stop_event.wait(0.6)
            if stop_event.is_set(): break

            _, current_raw_el, success = query_rotor_position_once()

            if success and current_raw_el is not None:
                consecutive_failures = 0
                if log: log.debug(
                    f"Waiting for 90° El: Current Raw={current_raw_el:.1f}, Target Raw={target_raw_el_for_90_disp:.1f}")
                if abs(current_raw_el - target_raw_el_for_90_disp) < 0.8:
                    if log: log.info("90° Elevation reached.")
                    position_reached = True
                    break
            else:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    if log: log.error("Too many consecutive query failures while waiting for elevation. Aborting wait.")
                    print("ERROR: Communication with rotor lost while waiting for elevation!")
                    break

        if not position_reached and not stop_event.is_set():
            if consecutive_failures >= max_consecutive_failures:
                pass  # Error already logged
            else:
                if log: log.warning(f"Timeout ({wait_timeout}s) reached while waiting for 90° Elevation.")
                print("WARNING: Timeout reached while waiting for 90° Elevation!")
        elif position_reached:
            print("90° Elevation reached.")

    except Exception as e:
        if log: log.exception("Error during automatic elevation movement:")
        print("ERROR during automatic elevation movement!")
        return False

    # --- Manual Azimuth Calibration ---
    if not pynput_available:
        if log: log.warning("Module 'pynput' not found. Skipping keyboard calibration for Azimuth.")
        if log: log.warning("Azimuth offset set to 0 (rotor raw value at 0°). Rotor direction might be inverted!")
        azimuth_offset = 0.0
        r_az, r_el, success = query_rotor_position_once()
        if success and r_az is not None and r_el is not None:
            with position_lock:
                raw_azimuth = r_az
                raw_elevation = r_el
                display_azimuth = (-raw_azimuth + azimuth_offset + 360) % 360
                display_elevation = EL_MAP_SLOPE_M * raw_elevation + EL_MAP_INTERCEPT_B
                display_elevation = max(min(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG),
                                        min(max(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG), display_elevation))
            if log: log.info(
                f"Initial position read: Raw Az={raw_azimuth:.1f}, Raw El={raw_elevation:.1f} -> Disp Az={display_azimuth:.1f}, Disp El={display_elevation:.1f}")
        else:
            if log: log.error("Could not read initial position after elevation movement.")
        return True

    if log: log.info("--- Start manual Azimuth calibration ---")
    print("-" * 30)
    print("Use the LEFT/RIGHT arrow keys to move the rotor to Azimuth = 0° (e.g., North).")
    print("Press ENTER when the Azimuth position is reached.")
    print("Press ESC to cancel calibration and exit the program.")
    print("Azimuth calibration active...")

    active_keys_calib = set()
    calibration_finished_event = threading.Event()

    def send_calib_command(command):
        with command_lock:
            send_pelco_command(command)

    def update_rotor_movement_calib():
        command_to_send = CMD_STOP
        left = keyboard.Key.left in active_keys_calib
        right = keyboard.Key.right in active_keys_calib
        if left and not right:
            command_to_send = CMD_LEFT
        elif right and not left:
            command_to_send = CMD_RIGHT
        send_calib_command(command_to_send)

    def on_press_calib(key):
        nonlocal active_keys_calib, calibration_finished_event
        if key in [keyboard.Key.left, keyboard.Key.right]:
            if key not in active_keys_calib:
                active_keys_calib.add(key)
                update_rotor_movement_calib()
        elif key == keyboard.Key.enter:
            if log: log.info("Azimuth calibration position confirmed. Reading current rotor position...")
            calibration_finished_event.set()
            return False
        elif key == keyboard.Key.esc:
            if log: log.info("Calibration aborted (ESC). Exiting program...")
            stop_event.set()
            calibration_finished_event.set()
            return False

    def on_release_calib(key):
        nonlocal active_keys_calib
        if key in [keyboard.Key.left, keyboard.Key.right]:
            active_keys_calib.discard(key)
            update_rotor_movement_calib()

    calib_listener = keyboard.Listener(on_press=on_press_calib, on_release=on_release_calib)
    calib_listener.start()
    calibration_finished_event.wait()
    calib_listener.stop()
    send_calib_command(CMD_STOP)
    time.sleep(0.3)
    print("\r" + " " * 60 + "\r", end="")

    if not stop_event.is_set():
        current_az, current_el, success = query_rotor_position_once()
        if success and current_az is not None and current_el is not None:
            with position_lock:
                # Set the offset to the RAW value read when physically at 0°
                azimuth_offset = current_az
                raw_azimuth = current_az
                raw_elevation = current_el
                # Calculate display azimuth using the potentially inverted formula
                display_azimuth = (-raw_azimuth + azimuth_offset + 360) % 360
                display_elevation = EL_MAP_SLOPE_M * raw_elevation + EL_MAP_INTERCEPT_B
                display_elevation = max(min(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG),
                                        min(max(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG), display_elevation))
                if log: log.info(
                    f"Rotor reports at Azimuth calibration point (Physical 0°): Raw Az={current_az:.1f}°, (Raw El={current_el:.1f}°)")
                if log: log.info(f"Azimuth offset (Raw value at 0°) set to: {azimuth_offset:.1f}°")
                if log: log.info(f"Initial display set to Az={display_azimuth:.1f}°, El={display_elevation:.1f}°")
            if log: log.info("--- Azimuth calibration completed ---")
            return True
        else:
            if log: log.error("ERROR: Could not read rotor position for Azimuth calibration!")
            if log: log.warning("Azimuth offset set to 0 (rotor raw value at 0°). Rotor direction might be inverted!")
            with position_lock:
                azimuth_offset = 0.0  # Fallback
                r_az, r_el, success_fallback = query_rotor_position_once()
                raw_azimuth = r_az if success_fallback and r_az is not None else 0.0
                raw_elevation = r_el if success_fallback and r_el is not None else 0.0
                # Calculate display azimuth using the potentially inverted formula
                display_azimuth = (-raw_azimuth + azimuth_offset + 360) % 360
                display_elevation = EL_MAP_SLOPE_M * raw_elevation + EL_MAP_INTERCEPT_B
                display_elevation = max(min(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG),
                                        min(max(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG), display_elevation))
            if log: log.warning("--- Azimuth calibration failed ---")
            return True
    else:
        return False


# --- Position Query Thread ---
def query_and_display_position_thread():
    """Thread for periodic querying and updating of the rotor position."""
    global raw_azimuth, raw_elevation, display_azimuth, display_elevation
    global azimuth_offset, last_query_ok, stop_event, position_lock, log
    global rotor_moving_until

    if log: log.info("Position query thread started.")
    last_successful_query_time = time.monotonic()
    last_fail_log_time = 0
    consecutive_query_failures = 0
    max_consecutive_query_failures = 5

    with position_lock:
        print_az = display_azimuth
        print_el = display_elevation

    print("--- Position display started (Ctrl+C to exit) ---")

    while not stop_event.is_set():
        start_time = time.monotonic()
        current_time_monotonic = start_time
        is_supposedly_moving = False

        # --- ALWAYS Query ---
        az_read, el_read, query_success = query_rotor_position_once()
        # --------------------

        with position_lock:
            is_supposedly_moving = current_time_monotonic < rotor_moving_until

            if query_success and az_read is not None and el_read is not None:
                # Successful query
                was_moving = rotor_moving_until > 0
                last_query_ok = True
                consecutive_query_failures = 0
                raw_azimuth = az_read
                raw_elevation = el_read
                # Calculate display azimuth using the potentially inverted formula
                display_azimuth = (-raw_azimuth + azimuth_offset + 360) % 360
                display_elevation = EL_MAP_SLOPE_M * raw_elevation + EL_MAP_INTERCEPT_B
                # Clamp display elevation to calibrated range
                display_elevation = max(min(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG),
                                        min(max(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG), display_elevation))
                last_successful_query_time = current_time_monotonic

                # If we had a successful query, assume the movement has finished
                rotor_moving_until = 0.0

                if log: log.debug(
                    f"Position Update: Raw Az={raw_azimuth:.1f}, Raw El={raw_elevation:.1f} -> "
                    f"Disp Az={display_azimuth:.1f}, Disp El={display_elevation:.1f}"
                )
                if was_moving:
                    if log: log.debug("Successful query received, move timer cleared.")

            else:
                # Query failed
                last_query_ok = False
                consecutive_query_failures += 1

                # Logic for error messages
                if is_supposedly_moving:
                    if consecutive_query_failures == 1 or consecutive_query_failures % 5 == 0:
                        if log: log.debug(
                            f"Query failed during expected move (Error {consecutive_query_failures}). Keeping old position.")
                else:
                    if consecutive_query_failures >= max_consecutive_query_failures and current_time_monotonic - last_fail_log_time > 5.0:
                        if log: log.error(
                            f"!!! {consecutive_query_failures} consecutive query failures outside move window !!!")
                        last_fail_log_time = current_time_monotonic
                    elif current_time_monotonic - last_successful_query_time > 5.0 and current_time_monotonic - last_fail_log_time > 10.0:
                        if log: log.warning(
                            f"Query failed (since {current_time_monotonic - last_successful_query_time:.1f}s) outside move window.")
                        last_fail_log_time = current_time_monotonic

                # IMPORTANT: Keep old values on error!

            # Determine status for display
            if not last_query_ok and is_supposedly_moving:
                final_status_char = "MOV?"
            elif last_query_ok:
                final_status_char = "OK"
            else:
                final_status_char = "ERR"

            print_az = display_azimuth
            print_el = display_elevation

        # Console output
        print(f"\rCurrent Position: AZ = {print_az:05.1f}°  EL = {print_el:05.1f}° [{final_status_char}] ", end="")

        # Sleep logic
        elapsed_time = time.monotonic() - start_time
        sleep_duration = max(0, QUERY_INTERVAL - elapsed_time)
        stop_event.wait(sleep_duration)

    print("\n--- Position display finished ---")
    if log: log.info("Position query thread finished.")


# --- EasyComm Parser ---
def parse_easycomm_az_el(command_payload):
    """Parses EasyComm 'P az el' or 'P AZ.. EL..' commands."""
    global log
    target_az = None
    target_el = None
    command_payload = command_payload.strip().replace(',', '.')
    parts = command_payload.split()
    if len(parts) == 2:
        try:
            az_val = float(parts[0])
            el_val = float(parts[1])
            target_az = az_val % 360.0
            target_el = el_val
            if log: log.debug(f"P command parsed as 'P az el': AZ={target_az}, EL={target_el}")
            return target_az, target_el
        except ValueError:
            if log: log.debug("Parsing as 'P az el' failed, trying 'P AZ... EL...'")
    az_match = re.search(r'AZ\s*(-?\d+(\.\d+)?)', command_payload, re.IGNORECASE)
    el_match = re.search(r'EL\s*(-?\d+(\.\d+)?)', command_payload, re.IGNORECASE)
    try:
        if az_match:
            target_az = float(az_match.group(1)) % 360.0
    except ValueError:
        if log: log.warning(f"Invalid Azimuth value in AZ/EL command: {az_match.group(1) if az_match else 'N/A'}")
        target_az = None
    try:
        if el_match: target_el = float(el_match.group(1))
    except ValueError:
        if log: log.warning(f"Invalid Elevation value in AZ/EL command: {el_match.group(1) if el_match else 'N/A'}")
        target_el = None
    if target_az is not None or target_el is not None:
        if log: log.debug(f"P command parsed as 'P AZ/EL': AZ={target_az}, EL={target_el}")
        return target_az, target_el
    else:
        if log: log.warning(f"Could not parse P command: {command_payload}")
        return None, None


# --- Function to send GoTo commands ---
def send_goto_position(target_display_az, target_display_el):
    """Sends Set Pan/Tilt commands to the rotor, saves targets, and sets movement flag."""
    global azimuth_offset, position_lock, command_lock, log
    global last_target_az, last_target_el
    global rotor_moving_until, last_query_ok

    with position_lock:
        az_offset_local = azimuth_offset
        if target_display_az is not None: last_target_az = target_display_az
        if target_display_el is not None: last_target_el = target_display_el
        if log: log.debug(f"Global targets saved: AZ={last_target_az}, EL={last_target_el}")

    with command_lock:
        az_sent = False
        el_sent = False
        # --- Send Azimuth ---
        if target_display_az is not None:
            # Calculate target rotor raw value using the inverted formula:
            # rotor_target_az = (offset - display_target) mod 360
            rotor_target_az = (az_offset_local - target_display_az + 360) % 360
            try:
                az_hundredths = int(round(rotor_target_az * 100))
                az_hundredths = max(0, min(35999, az_hundredths))  # Pelco-D limit
                az_msb = (az_hundredths >> 8) & 0xFF
                az_lsb = az_hundredths & 0xFF
                cmd_set_az = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, CMD2_SET_PAN, az_msb, az_lsb)
                if log: log.info(
                    f"-> Sending Set Pan Position (Display={target_display_az:.1f}°, Rotor Target Raw={rotor_target_az:.1f}°): {cmd_set_az.hex()}")
                if send_pelco_command(cmd_set_az):
                    az_sent = True
                else:
                    if log: log.error("Error sending Azimuth command.")
                time.sleep(0.3)  # Pause after Azimuth command
            except Exception as e:
                if log: log.exception(f"Error processing/sending Azimuth command for {target_display_az}:")

        # --- Send Elevation ---
        if target_display_el is not None:
            # Clamp target display elevation to calibrated range BEFORE inverse mapping
            clamped_target_display_el = max(min(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG),
                                            min(max(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG), target_display_el))
            if abs(clamped_target_display_el - target_display_el) > 0.1:
                if log: log.warning(
                    f"Gpredict target El {target_display_el:.1f}° outside calibration range. Clamping to {clamped_target_display_el:.1f}°.")

            # Inverse mapping: Raw = (Display - b) / m
            if abs(EL_MAP_SLOPE_M) < 0.01:
                if log: log.error("ERROR: Mapping slope is almost zero, inverse calculation not possible.")
                rotor_target_el = (ROTOR_RAW_EL_MIN + ROTOR_RAW_EL_MAX) / 2.0  # Fallback
            else:
                rotor_target_el = (clamped_target_display_el - EL_MAP_INTERCEPT_B) / EL_MAP_SLOPE_M

            try:
                # Clamp calculated raw target elevation to rotor's raw limits
                original_rotor_target_el = rotor_target_el
                rotor_target_el = max(ROTOR_RAW_EL_MIN, min(ROTOR_RAW_EL_MAX, rotor_target_el))
                if abs(rotor_target_el - original_rotor_target_el) > 0.01:
                    if log: log.warning(
                        f"Calculated rotor target {original_rotor_target_el:.1f}° outside raw range [{ROTOR_RAW_EL_MIN:.1f}..{ROTOR_RAW_EL_MAX:.1f}]. Clamping to {rotor_target_el:.1f}°.")

                el_hundredths = int(round(rotor_target_el * 100))
                el_hundredths = max(0, min(35999, el_hundredths))  # Pelco-D limit
                el_msb = (el_hundredths >> 8) & 0xFF
                el_lsb = el_hundredths & 0xFF
                cmd_set_el = build_pelco_d_command(PELCO_D_ADDRESS, 0x00, CMD2_SET_TILT, el_msb, el_lsb)
                if log: log.info(
                    f"-> Sending Set Tilt Position (Display={clamped_target_display_el:.1f}°, Rotor Target Raw={rotor_target_el:.1f}°): {cmd_set_el.hex()}")
                if send_pelco_command(cmd_set_el):
                    el_sent = True
                else:
                    if log: log.error("Error sending Elevation command.")
            except Exception as e:
                if log: log.exception(f"Error processing/sending Elevation command for {target_display_el}:")

        if log: log.debug(f"Set command finished (AZ Sent: {az_sent}, EL Sent: {el_sent}).")

        # Short pause after both commands
        if az_sent or el_sent:
            time.sleep(0.05)

            with position_lock:
                move_until_time = time.monotonic() + ESTIMATED_MAX_MOVE_DURATION
                if move_until_time > rotor_moving_until:
                    rotor_moving_until = move_until_time
                last_query_ok = False  # Assume movement starts, query might fail initially
                if log: log.debug(
                    f"Rotor likely moving, expecting query failures until ~{rotor_moving_until:.1f}. Set last_query_ok=False.")


# --- Client Handler for Gpredict ---
def handle_client(conn, addr):
    """Handles a single connection from Gpredict."""
    global stop_event, position_lock, display_azimuth, display_elevation, command_lock, log
    global last_target_az, last_target_el, last_query_ok
    global rotor_moving_until

    client_address_str = f"{addr[0]}:{addr[1]}"
    if log: log.info(f"New connection from Gpredict: {client_address_str}")
    conn.settimeout(0.5)

    loop_counter = 0

    try:
        while not stop_event.is_set():
            loop_counter += 1
            try:
                data = conn.recv(1024)
                if not data:
                    if log: log.info(f"Connection from {client_address_str} closed (client side, recv returned empty).")
                    break

                loop_counter = 0
                commands = data.decode('ascii', errors='ignore').strip().split('\n')

                for command_str in commands:
                    command_str = command_str.strip()
                    if not command_str: continue

                    if log: log.info(f"Received from {client_address_str}: {command_str}")
                    cmd_char_orig = command_str[0]

                    if cmd_char_orig == 'p':
                        # Get Position
                        az_to_send = 0.0
                        el_to_send = 0.0
                        log_reason = "Unknown"

                        with position_lock:
                            query_failed_recently = not last_query_ok
                            current_az = display_azimuth
                            current_el = display_elevation
                            target_az = last_target_az
                            target_el = last_target_el

                            if query_failed_recently:
                                az_to_send = target_az if target_az is not None else current_az
                                el_to_send = target_el if target_el is not None else current_el
                                log_reason = "Query failed, sending target/fallback position"
                            else:
                                az_to_send = current_az
                                el_to_send = current_el
                                log_reason = "Query OK, sending current position"

                        if log: log.info(f"{log_reason}: AZ={az_to_send:.1f}, EL={el_to_send:.1f}")
                        response = f"{az_to_send:.1f}\n{el_to_send:.1f}\n"
                        try:
                            conn.sendall(response.encode('ascii'))
                        except socket.error as e:
                            if log: log.error(f"Socket error sending (p) to {client_address_str}: {e}")
                            return

                    elif cmd_char_orig == 'P':
                        # Set Position
                        command_payload = command_str[1:]
                        target_az_parsed, target_el_parsed = parse_easycomm_az_el(command_payload)

                        if target_az_parsed is not None or target_el_parsed is not None:
                            if log: log.debug(
                                f"Calling send_goto_position for P command: AZ={target_az_parsed}, EL={target_el_parsed}")
                            send_goto_position(target_az_parsed, target_el_parsed)

                            if log: log.info(f"P processed, sending immediate RPRT 0 confirmation.")
                            response = "RPRT 0\n"
                            try:
                                conn.sendall(response.encode('ascii'))
                            except socket.error as e:
                                if log: log.error(
                                    f"Socket error sending (immediate P RPRT) to {client_address_str}: {e}")
                        else:
                            if log: log.warning(f"Invalid P command from {client_address_str}: {command_str}")

                    elif cmd_char_orig.lower() == 's':
                        # Stop
                        if log: log.info(f"-> Sending Stop command (from {client_address_str})")
                        with command_lock:
                            send_pelco_command(CMD_STOP)
                            time.sleep(0.05)
                            send_pelco_command(CMD_STOP)
                        with position_lock:
                            if rotor_moving_until > 0:
                                if log: log.debug("Stop received, cleared rotor_moving_until timer.")
                                rotor_moving_until = 0.0

                        if log: log.info(f"S processed, sending RPRT 0 confirmation.")
                        response = "RPRT 0\n"
                        try:
                            conn.sendall(response.encode('ascii'))
                        except socket.error as e:
                            if log: log.error(f"Socket error sending (S RPRT) to {client_address_str}: {e}")

                    elif cmd_char_orig == 'q':
                        # Quit
                        if log: log.info(f"Received: 'q' from {client_address_str}. Closing connection actively.")
                        return

                    else:
                        if log: log.warning(f"Unknown or unsupported command from {client_address_str}: {command_str}")

            except socket.timeout:
                continue
            except (socket.error, ConnectionResetError) as e:
                if log: log.warning(
                    f"Socket error for {client_address_str} (likely disconnect by Gpredict): {e}. Closing connection.")
                break
            except UnicodeDecodeError as e:
                if log: log.warning(f"Could not decode data from {client_address_str}: {e} - Data: {data}")
                continue
            except Exception as e:
                if log: log.exception(f"Unexpected error processing {client_address_str}:")
                break

    finally:
        if log: log.info(f"Cleaning up client thread for {client_address_str} and closing socket.")
        conn.close()
        current_thread = threading.current_thread()
        if current_thread in client_threads:
            try:
                client_threads.remove(current_thread)
                if log: log.debug(f"Client thread {current_thread.name} removed from list.")
            except ValueError:
                if log: log.warning(f"Could not remove client thread {current_thread.name} from list (already gone?).")
        else:
            if log: log.warning(f"Client thread {current_thread.name} was not in the client_threads list.")


# --- Signal Handler for clean shutdown ---
def signal_handler(sig, frame):
    """Handles Ctrl+C or termination signals for graceful shutdown."""
    global stop_event, log
    if not stop_event.is_set():
        if log: log.info(f"Signal {sig} received. Shutting down...")
        stop_event.set()


# --- Main Program --- ## MODIFIED V_FIX_AZIMUTH_DEBUG_PROMPT ##
if __name__ == "__main__":
    # --- NEW: Ask user for debug mode ---
    debug_enabled = False
    while True:
        try:
            user_input = input("DEBUG-Mode ON? (Y/N): ").strip().upper()
            if user_input == 'Y':
                debug_enabled = True
                break
            elif user_input == 'N':
                debug_enabled = False
                break
            else:
                print("Invalid input. Please enter 'Y' or 'N'.")
        except EOFError:  # Handle case where input stream is closed (e.g., piping)
            print("\nNo input received, defaulting to DEBUG=OFF.")
            debug_enabled = False
            break
        except KeyboardInterrupt:  # Handle Ctrl+C during prompt
            print("\nOperation cancelled by user.")
            sys.exit(0)

    # --- Logging configuration based on user input ---
    log_level = logging.DEBUG if debug_enabled else logging.INFO
    logging.basicConfig(level=log_level,
                        format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
    log = logging.getLogger(__name__)  # Make logger globally available
    # ----------------------------------------------------

    # --- ADJUST VERSION HERE ---
    log.info(
        "--- ACTii SC3044 Rotor Control (EasyComm Server for Gpredict) V_FIX_AZIMUTH_DEBUG_PROMPT ---")  # Version updated
    if debug_enabled:
        log.info("Debug mode is ENABLED.")
    else:
        log.info("Debug mode is DISABLED. Only INFO and higher levels will be shown.")
    # -----------------------------
    log.info(f"Rotor: {SERIAL_PORT} ({SERIAL_BAUDRATE} Baud, Address {PELCO_D_ADDRESS})")
    log.info(f"Gpredict Server: {TCP_HOST}:{TCP_PORT}")

    # --- Logging of mapping parameters ---
    log.info(f"Elevation Mapping: Display = {EL_MAP_SLOPE_M:.4f} * Raw + {EL_MAP_INTERCEPT_B:.4f}")
    log.info(f"Raw value limits for Elevation: [{ROTOR_RAW_EL_MIN:.1f}..{ROTOR_RAW_EL_MAX:.1f}]")
    log.info(f"Azimuth Mapping: Display = (-Raw + Offset) mod 360 (Offset determined during calibration)")
    # ----------------------------------------------------------

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Open serial port
        ser = serial.Serial(SERIAL_PORT, SERIAL_BAUDRATE, timeout=0.7)
        log.info(f"Serial connection {SERIAL_PORT} opened with Timeout={ser.timeout}s.")
        time.sleep(0.5)
        with command_lock:
            log.debug("Sending initial Stop command...")
            send_pelco_command(CMD_STOP)
            time.sleep(0.1)
            ser.reset_input_buffer()
            log.debug("Initial Stop sent and buffer cleared.")
    except serial.SerialException as e:
        log.critical(f"FATAL ERROR: Rotor port {SERIAL_PORT} not opened: {e}")
        sys.exit(1)
    except Exception as e:
        log.critical(f"FATAL ERROR during serial initialization: {e}")
        sys.exit(1)

    # Perform calibration
    calibration_successful = False
    try:
        calibration_successful = calibrate_rotor()
        if not calibration_successful and not stop_event.is_set():
            log.error("Calibration failed. Attempting to continue without calibration (Offset=0).")
            azimuth_offset = 0.0  # Fallback: Raw 0 = Display 0
            log.info("Attempting to read initial position after failed calibration...")
            r_az, r_el, success = query_rotor_position_once()
            if success and r_az is not None and r_el is not None:
                with position_lock:
                    raw_azimuth = r_az
                    raw_elevation = r_el
                    display_azimuth = (-raw_azimuth + azimuth_offset + 360) % 360
                    display_elevation = EL_MAP_SLOPE_M * raw_elevation + EL_MAP_INTERCEPT_B
                    display_elevation = max(min(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG),
                                            min(max(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG), display_elevation))
                log.info(
                    f"Initial position read after error: Disp Az={display_azimuth:.1f}, Disp El={display_elevation:.1f}")
                calibration_successful = True  # Mark as "successful" for server start
            else:
                log.error("Could not read initial position after calibration error either. Exiting.")
                stop_event.set()
        elif not calibration_successful and stop_event.is_set():
            log.info("Calibration aborted. Program will exit.")

    except Exception as e:
        log.exception("Unexpected error during calibration:")
        log.warning("Setting Azimuth offset to 0 and attempting to continue.")
        azimuth_offset = 0.0  # Fallback: Raw 0 = Display 0
        log.info("Attempting to read initial position after calibration error...")
        r_az, r_el, success = query_rotor_position_once()
        if success and r_az is not None and r_el is not None:
            with position_lock:
                raw_azimuth = r_az;
                raw_elevation = r_el
                display_azimuth = (-raw_azimuth + azimuth_offset + 360) % 360
                display_elevation = EL_MAP_SLOPE_M * raw_elevation + EL_MAP_INTERCEPT_B
                display_elevation = max(min(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG),
                                        min(max(DISP_EL_FOR_90_DEG, DISP_EL_FOR_20_DEG), display_elevation))
            log.info(
                f"Initial position read after error: Disp Az={display_azimuth:.1f}, Disp El={display_elevation:.1f}")
            calibration_successful = True  # Mark as "successful" for server start
        else:
            log.error("Could not read initial position after calibration error either. Exiting.")
            stop_event.set()

    # Start position query thread and TCP server (only if calibration ok or fallback worked)
    pos_thread = None
    if not stop_event.is_set() and calibration_successful:
        log.info("Starting TCP Server for Gpredict...")
        pos_thread = threading.Thread(target=query_and_display_position_thread, name="PositionQueryThread")
        pos_thread.start()

        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((TCP_HOST, TCP_PORT))
            server_socket.listen(1)
            log.info(f"Server listening on {TCP_HOST}:{TCP_PORT}")
            server_socket.settimeout(1.0)
        except socket.error as e:
            log.critical(f"FATAL ERROR: Could not create/bind server socket: {e}")
            stop_event.set()
        except Exception as e:
            log.critical(f"FATAL ERROR during server start: {e}")
            stop_event.set()

    # Main Loop: Wait for connections
    try:
        while not stop_event.is_set():
            try:
                if server_socket is None:
                    log.error("Server socket not initialized. Exiting main loop.")
                    stop_event.set()
                    break

                conn, addr = server_socket.accept()
                if len(client_threads) >= 1:
                    log.warning(f"Maximum client count (1) reached. Rejecting connection from {addr}.")
                    try:
                        conn.sendall("RPRT 1\n".encode('ascii'))
                    except socket.error:
                        pass
                    conn.close()
                    continue

                client_thread = threading.Thread(target=handle_client, args=(conn, addr),
                                                 name=f"ClientThread-{addr[0]}-{addr[1]}")
                client_threads.append(client_thread)
                client_thread.start()
            except socket.timeout:
                continue
            except OSError as e:
                if stop_event.is_set():
                    log.info("Server socket closed, exiting accept loop.")
                    break
                else:
                    log.exception("OSError in server main loop:")
                    stop_event.set()
            except AttributeError:
                if not stop_event.is_set():
                    log.error("Server socket not initialized (AttributeError). Exiting.")
                    stop_event.set()
                break
            except Exception as e:
                if not stop_event.is_set():
                    log.exception("Error in server main loop:")
                    stop_event.set()
                else:
                    log.debug(f"Error in server main loop during shutdown: {e}")
                time.sleep(0.1)

    finally:
        # Cleanup
        log.info("Cleaning up and exiting program...")
        stop_event.set()

        if server_socket:
            log.info("Closing server socket...")
            try:
                server_socket.shutdown(socket.SHUT_RDWR)
            except (OSError, socket.error) as e:
                log.debug(f"Error shutting down server socket (ignored): {e}")
            try:
                server_socket.close()
            except Exception as e:
                log.error(f"Error closing server socket: {e}")

        if pos_thread and pos_thread.is_alive():
            log.info("Waiting for position thread...")
            pos_thread.join(timeout=QUERY_INTERVAL + 1.0)
            if pos_thread.is_alive(): log.warning("Position thread could not be stopped in time.")

        active_client_threads = client_threads[:]
        if active_client_threads:
            log.info(f"Waiting briefly for {len(active_client_threads)} client thread(s)...")
            for t in active_client_threads:
                if t.is_alive(): t.join(timeout=1.0)

        if ser and ser.is_open:
            log.info("Sending final Stop command to rotor...")
            try:
                if command_lock.acquire(blocking=True, timeout=0.5):
                    try:
                        send_pelco_command(CMD_STOP)
                        time.sleep(0.05)
                        send_pelco_command(CMD_STOP)
                    finally:
                        command_lock.release()
                else:
                    log.warning("Could not acquire lock for final stop.")
                time.sleep(0.1)
                log.info(f"Closing serial connection to {SERIAL_PORT}...")
                ser.close()
            except Exception as e:
                log.error(f"Error during final stop or closing serial connection: {e}")

        log.info("Program finished.")
