#include <Servo.h>

Servo s;
const int pinServo = 9;
const int pin0    = 2;
const int pin60   = 3;

unsigned long lastMoveTime = 0;
bool timerActive = false;
int currentAngle = -1; // 초기값 -1로 설정하여 첫 명령 무조건 수행

void setup() {
  s.attach(pinServo);
  pinMode(pin0, INPUT_PULLUP);
  pinMode(pin60, INPUT_PULLUP);
  
  s.write(0); // 시작 시 0도 고정
  currentAngle = 0;
  delay(1000);
}

void loop() {
  int v0  = digitalRead(pin0);
  int v60 = digitalRead(pin60);

  // 0도 명령 (신호가 LOW이고 현재 0도가 아닐 때만 1번 수행)
  if (v0 == LOW && currentAngle != 0) {
    s.write(0);
    currentAngle = 0;
    timerActive = false; 
    delay(200); // 명령 후 안정화 시간
  } 
  // 60도 명령 (신호가 LOW이고 현재 60도가 아닐 때만 1번 수행)
  else if (v60 == LOW && currentAngle != 60) {
    s.write(60);
    currentAngle = 60;
    lastMoveTime = millis();
    timerActive = true;
    delay(200); // 명령 후 안정화 시간
  }

  // 7초 자동 복귀 (7000ms)
  if (timerActive && (millis() - lastMoveTime >= 1500)) {
    if (currentAngle != 0) {
      s.write(0);
      currentAngle = 0;
    }
    timerActive = false;
  }

  delay(50); // CPU 부하 감소 및 노이즈 방지
}