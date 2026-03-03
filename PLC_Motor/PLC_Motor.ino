#include <Servo.h>

Servo s;
const int pinServo = 9;
const int pin0    = 2;
const int pin60   = 3;

unsigned long lastMoveTime = 0;
bool timerActive = false;
int currentAngle = -1;

void setup() {
  s.attach(pinServo);
  pinMode(pin0, INPUT_PULLUP);
  pinMode(pin60, INPUT_PULLUP);
  
  s.write(0);
  currentAngle = 0;
  delay(1000);
}

void loop() {
  int v0  = digitalRead(pin0);
  int v60 = digitalRead(pin60);

  if (v0 == LOW && currentAngle != 0) {
    s.write(0);
    currentAngle = 0;
    timerActive = false; 
    delay(200);
  } 
  else if (v60 == LOW && currentAngle != 60) {
    s.write(60);
    currentAngle = 60;
    lastMoveTime = millis();
    timerActive = true;
    delay(200);
  }

  if (timerActive && (millis() - lastMoveTime >= 1500)) {
    if (currentAngle != 0) {
      s.write(0);
      currentAngle = 0;
    }
    timerActive = false;
  }

  delay(50);
}