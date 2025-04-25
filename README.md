# PELCO-D-Rotor-Controller-for-Gpredict
emulates an Easycomm Protocol Rotor in Gpredict 

hi this code was written by Gemini 2.5 Code assistent...because i cant code

Rotor type was ACTII AC3044

i run the script on windows and linux , works fine for me (on linux you have to adjust the port)

emulates an easycomm protocol rotor through gpredict

BAUDRATE is 2400 for my rotor but may vary for your

you need an rs485 to usb adapter to connect the rotor to your PC 

the offset from raw rotor data to 0°-360° AZ and 90° to 20° may be specific to my rotor, but you can adjust that very easy 

during calibration sequence at the start, the rotor automaticly drives the EL to "90°" wich is not really 90° because the rotor only has 
55°-90°-125° EL movement. So i used the 125° as 90° and mounted my dish at an angle to get 90°-20° out of it.

if anyone wants to improve this script, feel free and tell me if it worked

if you have any questions please ask me, but keep in mind i am not a coder
