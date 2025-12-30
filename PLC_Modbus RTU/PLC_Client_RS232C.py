import asyncio
from asyncua import Client, ua
from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusException
from pymodbus.payload import BinaryPayloadBuilder, Endian
import time
import json 
import sys


# 🚨 1. DB 인서터 Import (PLC_DateBase.py 파일이 같은 폴더에 있어야 합니다)
from PLC_DataBase import insert_log_sync, select_data_sync

# --- 1. 설정 정보 (사용자 환경에 맞게 반드시 수정) ---
# OPC UA 서버 설정 (기존 설정 유지)
SERVER_URL = "opc.tcp://172.30.1.61:4840/freeopcua/server/" 
OBJECT_NODE_ID = "ns=2;i=2"      
SENSOR1_METHOD_NODE_ID = "ns=2;s=write_conveyor_sensor_check"    
SENSOR2_METHOD_NODE_ID = "ns=2;s=write_robotarm_sensor_check"    

# 🚨 conveyor_move 명령 수신 노드 ID (기존 설정 유지)
CMOVE_COMMAND_NODE_ID = "ns=2;s=read_ready_state" 

# PLC Modbus 설정 (기존 설정 유지)
SERIAL_PORT = 'COM6'
BAUDRATE = 115200
PARITY = 'N'
STOPBITS = 1
CONVEYOR_SENSOR_ADDRESS = 64      # M0040 Coil Address (컨베이어 센서 - Read)
ROBOTARM_SENSOR_ADDRESS = 65      # M0041 Coil Address (로봇팔 센서 - Read)
SLAVE_ID = 3            

# Anomaly 처리 관련 설정 (기존 설정 유지)
ANOMALY_OPCUA_NODE_ID = "ns=2;s=read_ok_ng_value" 
PLC_WRITE_COIL_NG = 66      # M0042 코일 주소 (NG/불량 시 펄스)
PLC_WRITE_COIL_OK = 67      # M0043 코일 주소 (OK/정상 시 펄스)

# conveyor_move 명령 송신용 코일 (PLC Write) (기존 설정 유지)
PLC_WRITE_COIL_CONVEYOR_MOVE = 68 # M0081 Coil Address (conveyor_move 요청 신호 - Write)

# 💡 [핵심] 컨베이어 벨트 제어 D 레지스터 및 M 코일 주소
# D 레지스터 주소 (Word/정수형 하나만 사용)
D102_FREQ_ADDR_WORD = 0         # D102 주소 (Frequency, 예시: 101)
D104_ACCEL_ADDR = 2             # D104 주소 (Acceleration, 예시: 103)
D105_DECEL_ADDR = 3             # D105 주소 (Deceleration, 예시: 104)

# M 코일 주소 (정지 및 방향 명령)
M200_STOP_CMD_ADDR = 0      # M200 (정지 명령 코일)
M201_RESTART_CMD_ADDR = 3   # 💡 M201 (운행 재개 명령 코일) - M203 다음 주소로 가정
M202_FORWARD_CMD_ADDR = 1   # M202 (정방향 명령 코일)
M203_REVERSE_CMD_ADDR = 2   # M203 (역방향 명령 코일)

# OPC UA 연결 재시도 횟수 설정
MAX_RETRY = 5

# Modbus 클라이언트 객체 초기화 (기존 코드 유지)
modbus_client = ModbusSerialClient(
    port=SERIAL_PORT, 
    baudrate=BAUDRATE,
    parity=PARITY,
    stopbits=STOPBITS,
    timeout=1 
)

# -----------------------------------------------------------------------------
# 🚨 5. OPC UA Subscription Handler 클래스 (Anomaly)
# -----------------------------------------------------------------------------
class AnomalyDataHandler:
    def datachange_notification(self, node, val, data):
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # 1. 수신 로그 및 파싱
        print(f"\n--- ## OPC UA: Anomaly 상태 변화 감지 ## ---")
        print(f"[{current_time}] [OPC UA] 🔔 Anomaly 데이터 수신: {val}")
        
        is_anomaly = None
        status_code = None
        raw_val = val

        if isinstance(raw_val, str):
            status_str_raw = val.strip()
            start_index = status_str_raw.find('{')
            end_index = status_str_raw.rfind('}')
            
            if start_index != -1 and end_index != -1 and end_index > start_index:
                json_part = status_str_raw[start_index : end_index + 1]
                
                try:
                    data_dict = json.loads(json_part)
                    status_code = data_dict.get("Anomaly", None)
                    print(f"[OPC UA] ➡️ JSON 파싱 성공: {json_part}")
                except json.JSONDecodeError:
                    status_code = status_str_raw
            else:
                status_code = status_str_raw

        elif isinstance(raw_val, dict):
            status_code = raw_val.get("Anomaly", None)
        
        
        # 2. 상태 코드 확인 및 펄스 명령 준비
        if status_code is not None:
            status_str_upper = str(status_code).upper()
            
            if status_str_upper == 'OK':
                is_anomaly = False
            elif status_str_upper == 'NG':
                is_anomaly = True
        
        
        # 3. 최종 Anomaly 상태 확인 및 펄스 실행
        if is_anomaly is not None:
            state_desc = "불량 감지 (NG)" if is_anomaly else "정상 처리 (OK)"
            print(f"[OPC UA] 🔴 Anomaly 최종 상태: {state_desc}. PLC 펄스 명령 실행.")

            # 🚨 [수정된 핵심 로직] 펄스 제어와 컨베이어 재개 명령을 순차적으로 실행하는 새 태스크 정의 및 실행
            async def anomaly_and_restart_execution():
                try:
                    # 1. OK/NG 펄스 실행 (M0042 또는 M0043 신호)
                    print("[RESTART] ➡️ OK/NG 펄스 신호 전송 시작...")
                    await pulse_coil_on_anomaly(is_anomaly)
                    
                    # 2. Anomaly 처리 완료 후 컨베이어 재개 명령 실행 (M201 신호)
                    print("[RESTART] 🔄 Anomaly 처리 완료. 컨베이어 재개 (RESTART) 명령 실행.")
                    await pulse_coil_on_restart() # <--- 이 코드가 재개 명령을 보냅니다.

                except Exception as e:
                    print(f"[RESTART] ❌ Anomaly 처리 후 재개 중 오류 발생: {e}", file=sys.stderr)
                    
            asyncio.create_task(anomaly_and_restart_execution())
        else:
            print(f"[OPC UA] ⚠️ 유효하지 않은 Anomaly 상태: {status_code}. 처리 생략.")


# -----------------------------------------------------------------------------
# 🚨 5-2. conveyor_move 명령 수신 및 처리 Handler (HMI 버튼 클릭)
# -----------------------------------------------------------------------------
class CMoveDataHandler:
    def datachange_notification(self, node, val, data):
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # print(f"\n--- ## OPC UA: HMI 명령 수신 ## ---")
        # print(f"[{current_time}] [HMI SUB] 🔔 명령 데이터 수신: {val}")
        
        command_key_value = None
        
        # 1. 수신된 문자열에서 JSON 부분 추출 및 파싱
        if isinstance(val, str):
            status_str_raw = val.strip()
            start_index = status_str_raw.find('{')
            end_index = status_str_raw.rfind('}')
            
            if start_index != -1 and end_index != -1 and end_index > start_index:
                json_part = status_str_raw[start_index : end_index + 1]
                
                try:
                    data_dict = json.loads(json_part)
                    
                    if "state" in data_dict:
                        command_key_value = data_dict["state"].upper()
                    elif "move_command" in data_dict:
                        command_key_value = data_dict["move_command"].upper()
                    else:
                        command_key_value = None 

                except json.JSONDecodeError:
                    command_key_value = None
        
        # 2. 명령 종류 확인 및 처리
        if command_key_value == 'CONVEYOR_MOVE':
            # 🚨 [수정된 핵심 로직 1] '정지' 상태일 때 '수동 시작' 무시 로직 적용
            # print(f"[HMI SUB] ➡️ 명령 인식: CONVEYOR_MOVE (수동 시작). 조건부 실행.")
            asyncio.create_task(
                pulse_coil_on_conveyor_move_conditional()
            )
            
        elif command_key_value == 'CONVEYOR_STOP':
            # 🚨 [요청 반영] HMI 정지 명령은 무시하고 DB 폴링에 의한 자동 제어로 처리
            # print(f"[HMI SUB] ➡️ 명령 인식: CONVEYOR_STOP. HMI 명령 무시 (자동 제어 로직 사용).")
            pass 

        elif command_key_value == 'CONVEYOR_RESTART':
            # 🚨 [요청 반영] HMI 재개 명령은 무시하고 DB 폴링에 의한 자동 제어로 처리
            # print(f"[HMI SUB] ➡️ 명령 인식: CONVEYOR_RESTART. HMI 명령 무시 (자동 제어 로직 사용).")
            pass
            
        else:
            #  print(f"[HMI SUB] ⚠️ 알 수 없는 명령 수신: {command_key_value}. 처리 생략.")
            pass


# --- 2. Modbus RTU 통신 헬퍼 함수 (가독성 수정) ---
def _modbus_read_coil(address: int) -> int:
    """Modbus 코일의 상태를 읽어 1 또는 0을 반환합니다."""
    try:
        result = modbus_client.read_coils(address=address, count=1, slave=SLAVE_ID)
        
        if result.isError():
            print(f"[MODBUS] ❌ 코일 읽기 통신 오류 (A: {address}): {result}", file=sys.stderr)
            return -1 
        
        return 1 if result.bits[0] else 0 

    except ModbusException as e:
        print(f"[MODBUS] ❌ Modbus 예외 발생 (A: {address}, 연결 끊김 예상): {e}", file=sys.stderr)
        return -1
    except Exception as e:
        print(f"[MODBUS] ❌ 코일 읽기 예기치 않은 오류 (A: {address}): {e}", file=sys.stderr)
        return -1


def _modbus_write_coil(address: int, value: int) -> int:
    """Modbus 코일의 상태를 1 또는 0으로 설정합니다."""
    if value not in [0, 1]:
        print(f"[MODBUS] ❌ 쓰기 실패 (A: {address}): 유효하지 않은 값 ({value}). 0 또는 1만 허용됩니다.", file=sys.stderr)
        return -1

    try:
        write_value = True if value == 1 else False
        result = modbus_client.write_coil(address=address, value=write_value, slave=SLAVE_ID)
        
        if result.isError():
            print(f"[MODBUS] ❌ 코일 쓰기 통신 오류 (A: {address}): {result}", file=sys.stderr)
            return -1
        
        return 0 # 성공

    except ModbusException as e:
        print(f"[MODBUS] ❌ Modbus 예외 발생 (A: {address}, 연결 끊김 예상): {e}", file=sys.stderr)
        return -1
    except Exception as e:
        print(f"[MODBUS] ❌ 코일 쓰기 예기치 않은 오류 (A: {address}): {e}", file=sys.stderr)
        return -1

def _modbus_write_float(address: int, value: float) -> int:
    """Modbus Holding Register에 Float(실수) 값을 2개 워드(D 레지스터 2개)로 설정합니다."""
    try:
        builder = BinaryPayloadBuilder(byteorder=Endian.LITTLE, wordorder=Endian.LITTLE)
        builder.add_32bit_float(value)
        registers = builder.to_registers()
        
        result = modbus_client.write_registers(address=address, values=registers, slave=SLAVE_ID)
        
        if result.isError():
            print(f"[MODBUS] ❌ Float 쓰기 통신 오류 (A: {address}, V: {value}): {result}", file=sys.stderr)
            return -1
        
        return 0 # 성공

    except ModbusException as e:
        print(f"[MODBUS] ❌ Float Modbus 예외 발생 (A: {address}): {e}", file=sys.stderr)
        return -1
    except Exception as e:
        print(f"[MODBUS] ❌ Float 쓰기 예기치 않은 오류 (A: {address}): {e}", file=sys.stderr)
        return -1


def _modbus_write_register(address: int, value: int) -> int:
    """Modbus Holding Register에 16비트 정수(Word) 값을 설정합니다."""
    if not isinstance(value, int):
        try:
            value = int(value)
        except ValueError:
            print(f"[MODBUS] ❌ Register 쓰기 실패 (A: {address}): 정수로 변환 불가능한 값 ({value}).", file=sys.stderr)
            return -1

    try:
        result = modbus_client.write_register(address=address, value=value, slave=SLAVE_ID)
        
        if result.isError():
            print(f"[MODBUS] ❌ Register 쓰기 통신 오류 (A: {address}, V: {value}): {result}", file=sys.stderr)
            return -1
        
        return 0 # 성공

    except ModbusException as e:
        print(f"[MODBUS] ❌ Register Modbus 예외 발생 (A: {address}): {e}", file=sys.stderr)
        return -1
    except Exception as e:
        print(f"[MODBUS] ❌ Register 쓰기 예기치 않은 오류 (A: {address}): {e}", file=sys.stderr)
        return -1


# --- 3. PLC Read 함수 (기존과 동일) ---
def read_plc_m0040() -> int:
    """M0040 (Coil)의 상태를 읽습니다."""
    return _modbus_read_coil(CONVEYOR_SENSOR_ADDRESS)

def read_plc_m0041() -> int:
    """M0041 (Coil)의 상태를 읽습니다."""
    return _modbus_read_coil(ROBOTARM_SENSOR_ADDRESS)


# --- 4. Anomaly 펄스 제어 로직 (가독성 수정) ---
async def pulse_coil_on_anomaly(is_anomaly: bool):
    """
    Anomaly 상태에 따라 M0020 (불량) 또는 M0021 (정상) 코일 중 하나를 1초 동안 펄스 제어합니다.
    """
    if is_anomaly:
        target_address = PLC_WRITE_COIL_NG
        target_name = f"M{PLC_WRITE_COIL_NG} (불량/NG)"
    else:
        target_address = PLC_WRITE_COIL_OK
        target_name = f"M{PLC_WRITE_COIL_OK} (정상/OK)"
        
    print(f"[PULSE] 🚨 {target_name} 코일에 1초 펄스 명령 실행 시작.")

    def blocking_pulse():
        # 1. ON (1) 설정
        if _modbus_write_coil(target_address, 1) != 0:
            return -1
        # 2. 1초 대기
        time.sleep(1) 
        # 3. OFF (0) 설정
        if _modbus_write_coil(target_address, 0) != 0:
            return -1
        return 0

    try:
        result = await asyncio.to_thread(blocking_pulse)
        
        if result == 0:
            print(f"[PULSE] ✅ {target_name} 펄스 완료.")
            
            # print("[PULSE] 🚀 Anomaly 처리 완료. 컨베이어 재개 명령 실행 시작.")
            # await pulse_coil_on_restart() # <--- 이 함수 호출이 누락되어 있었습니다.
            
        else:
            print(f"[PULSE] ❌ {target_name} 펄스 실패 (Modbus 쓰기 오류).")
            
        return result
        
    except Exception as e:
        print(f"[PULSE] ❌ 예기치 않은 오류 발생: {e}", file=sys.stderr)
        return -1

# -----------------------------------------------------------------------------
# 🚨 5-1-1. conveyor_move 명령 처리 함수 (실제 PLC Write) (가독성 수정)
# -----------------------------------------------------------------------------
async def pulse_coil_on_conveyor_move():
    """
    [수동 시작] M0081 코일 (conveyor_move 명령)을 ON 상태로 유지하고,
    DB direction에 따라 방향 코일(M202/M203)을 제어하며, 정지 코일(M200, M201)을 OFF합니다.
    """
    target_address = PLC_WRITE_COIL_CONVEYOR_MOVE
    target_name = f"M{target_address} (Move)"
        
    # print(f"[CONVEYOR] ➡️ {target_name} 코일에 ON 명령 실행 시작.")
    
    # --- DB에서 현재 direction 값 조회 ---
    TARGET_EQ_ID = 'CONVEYOR01' 
    select_success, control_state = await asyncio.to_thread(
        select_data_sync, 'plc_control_state', ['direction'], f"equipment_id = '{TARGET_EQ_ID}'"
    )
    
    current_direction = None
    if select_success and control_state and control_state[0]:
        current_direction = control_state[0][0].upper()
        # print(f"[CONVEYOR] 🔍 DB 조회 Direction: {current_direction}")
    
    # --- 방향 코일 제어 로직 (M202/M203) ---
    if current_direction == 'FORWARD':
        # 정방향 (M202) 펄스
        await asyncio.to_thread(_modbus_write_coil, M202_FORWARD_CMD_ADDR, 1)
        time.sleep(0.05) # 짧은 펄스 유지 시간
        await asyncio.to_thread(_modbus_write_coil, M202_FORWARD_CMD_ADDR, 0)
        # print(f"[CONVEYOR] ✅ FORWARD 명령 (M{M202_FORWARD_CMD_ADDR}) 펄스 전송 완료.")
    elif current_direction == 'REVERSE':
        # 역방향 (M203) 펄스
        await asyncio.to_thread(_modbus_write_coil, M203_REVERSE_CMD_ADDR, 1)
        time.sleep(0.05) # 짧은 펄스 유지 시간
        await asyncio.to_thread(_modbus_write_coil, M203_REVERSE_CMD_ADDR, 0)
        # print(f"[CONVEYOR] ✅ REVERSE 명령 (M{M203_REVERSE_CMD_ADDR}) 펄스 전송 완료.")
    else:
        print("[CONVEYOR] ⚠️ 유효한 Direction 값을 찾지 못했습니다. 방향 제어 Skip.")


    # --- 🟢 M0081 ON & STOP/RESTART 코일 OFF ---
    # 1. M0081 ON (수동 시작)
    result = await asyncio.to_thread(_modbus_write_coil, target_address, 1)

    # 2. M200 OFF (정지 해제)
    await asyncio.to_thread(_modbus_write_coil, M200_STOP_CMD_ADDR, 0)
    # 3. M201 OFF (운행 재개 명령 해제)
    await asyncio.to_thread(_modbus_write_coil, M201_RESTART_CMD_ADDR, 0)
    
    if result == 0:
        # print(f"[CONVEYOR] ✅ {target_name} 코일 ON 명령 완료 (M{target_address}).")
        pass
    else:
        print(f"[CONVEYOR] ❌ {target_name} 코일 ON 명령 실패 (Modbus 오류).")
        
    return result

# -----------------------------------------------------------------------------
# 🚨 5-1-2. [신규] '정지' 상태 시 '수동 시작' 명령 무시 로직 (가독성 수정)
# -----------------------------------------------------------------------------
async def pulse_coil_on_conveyor_move_conditional():
    """
    HMI로부터 수동 시작 명령을 받았을 때, DB의 run_mode가 'STOP'이 아닌 경우에만 
    pulse_coil_on_conveyor_move 함수를 호출합니다.
    """
    TARGET_EQ_ID = 'CONVEYOR01' 
    select_success, control_state = await asyncio.to_thread(
        select_data_sync, 'plc_control_state', ['run_mode'], f"equipment_id = '{TARGET_EQ_ID}'"
    )
    
    current_run_mode = None
    if select_success and control_state and control_state[0]:
        current_run_mode = control_state[0][0].upper()
        # print(f"[HMI CMOVE] 🔍 DB run_mode 확인: {current_run_mode}")
        
    if current_run_mode == 'STOP':
        # 요청 1: '정지' 상태일 때 수동 시작 버튼 눌러도 작동 안 됨
        # print("[HMI CMOVE] 🛑 DB run_mode가 'STOP'이므로, 수동 시작 명령을 무시합니다.")
        return -1
    else:
        # 'STOP' 상태가 아니면 실제 컨베이어 시작 로직 실행
        # print("[HMI CMOVE] 🚀 DB run_mode가 'STOP'이 아니므로, 수동 시작 명령을 실행합니다.")
        return await pulse_coil_on_conveyor_move()

# -----------------------------------------------------------------------------
# 🚨 5-2. conveyor_stop 명령 처리 함수 (정지) (가독성 수정)
# -----------------------------------------------------------------------------
async def pulse_coil_on_conveyor_stop():
    """
    [정지] M200 (STOP 명령) 코일을 ON 상태로 유지하고,
    M0081 (conveyor_move) 및 M201 (RESTART) 코일을 OFF 상태로 설정합니다.
    """
    
    # print(f"[CONVEYOR] 🛑 STOP 명령 실행: M{M200_STOP_CMD_ADDR} ON, M{PLC_WRITE_COIL_CONVEYOR_MOVE}/M{M201_RESTART_CMD_ADDR} OFF")

    # 1. M200 (STOP) ON 상태로 유지
    result_stop = await asyncio.to_thread(_modbus_write_coil, M200_STOP_CMD_ADDR, 1)
    
    # 2. PLC_WRITE_COIL_CONVEYOR_MOVE (M0081) OFF (conveyor_move 요청 해제)
    result_move_off = await asyncio.to_thread(_modbus_write_coil, PLC_WRITE_COIL_CONVEYOR_MOVE, 0)
    
    # 3. M201 (RESTART) OFF
    result_restart_off = await asyncio.to_thread(_modbus_write_coil, M201_RESTART_CMD_ADDR, 0)

    if result_stop == 0 and result_move_off == 0 and result_restart_off == 0:
        # print(f"[CONVEYOR] ✅ STOP 명령 완료.")
        return 0
    else:
        print(f"[CONVEYOR] ❌ STOP 명령 실패 (Modbus 오류).")
        return -1

# -----------------------------------------------------------------------------
# 🚨 5-3. 운행재개(RESTART) 명령 처리 함수 (가독성 수정)
# -----------------------------------------------------------------------------
async def pulse_coil_on_restart():
    """
    [운행재개] M201 (RESTART 명령) 코일을 ON 상태로 유지하고,
    M200 (STOP) 및 M0081 (conveyor_move) 코일을 OFF 상태로 설정합니다.
    """
    
    print(f"[CONVEYOR] 🔄 RESTART 명령 실행: M{M201_RESTART_CMD_ADDR} ON, M{M200_STOP_CMD_ADDR}/M{PLC_WRITE_COIL_CONVEYOR_MOVE} OFF")

    # 1. M201 (RESTART) ON 상태로 유지
    result_restart = await asyncio.to_thread(_modbus_write_coil, M201_RESTART_CMD_ADDR, 1)
    
    # 2. M200 (STOP) OFF (정지 상태 해제)
    result_stop_off = await asyncio.to_thread(_modbus_write_coil, M200_STOP_CMD_ADDR, 0)
    
    # 3. M0081 (conveyor_move) OFF
    result_move_off = await asyncio.to_thread(_modbus_write_coil, PLC_WRITE_COIL_CONVEYOR_MOVE, 0)

    if result_restart == 0 and result_stop_off == 0 and result_move_off == 0:
        print(f"[CONVEYOR] ✅ RESTART 명령 완료.")
        return 0
    else:
        print(f"[CONVEYOR] ❌ RESTART 명령 실패 (Modbus 오류).")
        return -1


# --- 6. OPC UA Method 호출 로직 (가독성 수정) ---
async def call_method_with_plc_data(client: Client, method_node_id: str, input_value):
    """
    M0040/M0041 상태(Boolean)를 OPC UA 서버에 Method로 전송합니다.
    """
    if isinstance(input_value, bool):
        variant_type = ua.VariantType.Boolean
    else:
        # 0 또는 1이 들어오면 bool(input_value)로 변환됩니다.
        variant_type = ua.VariantType.Boolean 
        input_value = bool(input_value) # 1 -> True, 0 -> False

    method_name = method_node_id.split(';')[-1] # 메서드 이름 추출

    try:
        obj_node = client.get_node(OBJECT_NODE_ID)
        method_node = client.get_node(method_node_id) 

        # Method 호출을 위한 입력 인자 설정
        arguments = [
            ua.Variant(input_value, variant_type) 
        ]
        
        result = await obj_node.call_method(method_node, *arguments)
        
        return method_node_id, result

    except Exception as e:
        error_message = f"❌ OPC UA Method 호출 중 오류 발생: {e.__class__.__name__} - {e}"
        return method_node_id, (False, error_message)


# --- 7. 메인 실행 함수 (가독성 수정) ---
async def main():
    opcua_client = Client(url=SERVER_URL)
    
    print(f"--- ## 시스템 초기화 시작 ## ---")
    print(f"[CONNECT] OPC UA 서버 접속 시도: {SERVER_URL}")

    # 🚨 센서 상태 변화 감지를 위한 last_value 변수 초기화
    last_m0040_value = -1 
    last_m0041_value = -1 

    # 🚨 [요청 반영] 프로그램 시작 시 run_mode의 초기값은 'STOP'으로 간주하여 자동 실행 방지
    last_run_mode = "STOP" 
    last_direction = ""

    is_first_run = True

    try:
        # 1. OPC UA 연결 시도 (재시도 로직)
        connected = False
        for retry_count in range(MAX_RETRY):
            try:
                await opcua_client.connect()
                print("[CONNECT] ✅ OPC UA 서버 연결 성공!")
                connected = True
                break
            except (ConnectionRefusedError, TimeoutError, Exception) as e:
                print(f"[CONNECT] ❌ OPC UA 연결 실패 (시도 {retry_count + 1}/{MAX_RETRY}): {e.__class__.__name__}", file=sys.stderr)
                if retry_count < MAX_RETRY - 1:
                    print("          -> 5초 후 재시도합니다.")
                    await asyncio.sleep(5)
                else:
                    print(f"[CONNECT] ❌ OPC UA 연결 재시도 횟수({MAX_RETRY}회) 초과. 프로그램 종료.", file=sys.stderr)
                    return

        if not connected:
            return

        # 2. Modbus 클라이언트 연결 시도 
        if not modbus_client.connect():
            print("[CONNECT] ❌ Modbus 연결 실패: PLC 연결를 확인하세요.", file=sys.stderr)
            return  
        print("[CONNECT] ✅ Modbus 연결 성공.")

        # ---------------------------------------------------------------------
        # 3. OPC UA 구독 시작: Anomaly 상태 및 HMI 명령
        # ---------------------------------------------------------------------
        
        # 3-1. Anomaly 상태 구독 (기존 로직 유지)
        handler_anomaly = AnomalyDataHandler()
        sub_anomaly = await opcua_client.create_subscription(100, handler_anomaly)
        try:
            anomaly_node = opcua_client.get_node(ANOMALY_OPCUA_NODE_ID)
            await sub_anomaly.subscribe_data_change(anomaly_node)
            print(f"[OPC UA] ✅ Anomaly 구독 시작: {anomaly_node.nodeid}")
                    
        except Exception as e:
             print(f"[OPC UA] ❌ Anomaly 구독 실패: {e.__class__.__name__}", file=sys.stderr)

        # 3-2. HMI 명령 (conveyor_move, STOP, RESTART) 구독 추가
        handler_move = CMoveDataHandler()
        conveyor_move = await opcua_client.create_subscription(100, handler_move)
        
        try:
             conveyor_move_node = opcua_client.get_node(CMOVE_COMMAND_NODE_ID)
             await conveyor_move.subscribe_data_change(conveyor_move_node)
             print(f"[OPC UA] ✅ HMI 명령 구독 시작: {conveyor_move_node.nodeid}")
        except Exception as e:
             print(f"[OPC UA] ❌ HMI 명령 구독 실패: {e.__class__.__name__}", file=sys.stderr)

        print(f"--- ## PLC/DB 폴링 루프 시작 (0.2초 주기) ## ---")
        
        # 0.2초마다 PLC 데이터 읽기 (폴링 루프 시작)
        while True:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")

            # =================================================================
            # 0. 🚨 PLC 제어 상태 (패널 설정값) DB에서 SELECT
            # =================================================================
            TARGET_EQ_ID = 'CONVEYOR01' 
            target_columns = ['run_mode', 'direction', 'frequency', 'acceleration', 'deceleration']
            select_condition = f"equipment_id = '{TARGET_EQ_ID}'"

            # **select_data_sync 함수 호출**
            select_success, control_state = await asyncio.to_thread(
                select_data_sync, 'plc_control_state', target_columns, select_condition
            )

            # 1. 조회된 상태 데이터 처리
            if select_success and control_state:
                state_data = control_state[0] 
                                
                current_run_mode = state_data[0].upper()   # run_mode (인덱스 0)
                current_direction = state_data[1].upper()  # direction (인덱스 1)
                current_frequency = state_data[2]          # frequency (인덱스 2)
                
                if len(state_data) >= 5:
                    current_accelerate = state_data[3]     # acceleration (인덱스 3 -> D104)
                    current_decelerate = state_data[4]     # deceleration (인덱스 4 -> D105)
                else:
                    print(f"[{current_time}] [DB CONTROL] ⚠️ DB 컬럼 부족 ({len(state_data)}개). 가감속 제어 Skip.")
                    current_accelerate = 0
                    current_decelerate = 0
                    
                
                # --- 🚨 DB 값을 기반으로 PLC/Client 로직 수행 ---
                
                # =================================================================
                # (예시 1) [요청 반영] run_mode 변경 감지 로직 (DB 폴링을 통한 자동 정지/재개)
                # =================================================================
                if current_run_mode != last_run_mode:
                    
                    if is_first_run:
                        # print(f"[{current_time}] [DB AUTO] ℹ️ 초기 실행 감지. DB run_mode({current_run_mode})를 무시하고 상태만 갱신.")
                        last_run_mode = current_run_mode # 상태만 갱신하고 명령 실행은 건너뜀
                        is_first_run = False
                        continue # 다음 루프로 이동

                    if current_run_mode == 'STOP':
                        # DB의 run_mode가 'STOP'으로 변경되면 정지 명령 자동 실행
                        # print(f"[{current_time}] [DB AUTO] 🛑 STOP 감지. CONVEYOR_STOP 명령 자동 실행.")
                        asyncio.create_task(pulse_coil_on_conveyor_stop()) 
                        
                    elif current_run_mode == 'RESTART':
                        # DB의 run_mode가 'RESTART'로 변경되면 운행 재개 명령 자동 실행
                        # print(f"[{current_time}] [DB AUTO] 🔄 RESTART 감지. CONVEYOR_RESTART 명령 자동 실행.")
                        asyncio.create_task(pulse_coil_on_restart())
                        
                    elif current_run_mode == 'RUN' or current_run_mode == 'MOVE' or current_run_mode == 'READY':
                         # DB의 run_mode가 RUN/MOVE/READY 상태로 변경되면 운전 명령 자동 실행
                        # print(f"[{current_time}] [DB AUTO] 🚀 RUN/MOVE 감지. CONVEYOR_MOVE 명령 자동 실행.")

                        log_desc = "Conveyor START"
                        asyncio.create_task(
                            asyncio.to_thread(insert_log_sync, 'CONVEYOR01', 'PLC', log_desc)
                        )
                        
                        asyncio.create_task(pulse_coil_on_conveyor_move()) 
                        
                    # 엣지 감지를 위해 현재 상태 업데이트
                    last_run_mode = current_run_mode
                
                # (예시 2) DB에 설정된 주파수/가감속 값을 PLC D 레지스터에 상시 적용
                
                # 💡 [핵심] 모든 설정값을 정수형으로 변환 및 스케일링
                scaled_frequency = current_frequency * 100
                int_frequency = int(scaled_frequency)
                
                scaled_accelerate = current_accelerate
                int_accelerate = int(scaled_accelerate)
                
                scaled_decelerate = current_decelerate
                int_decelerate = int(scaled_decelerate)
                
                # **Modbus D 레지스터에 쓰기**
                await asyncio.to_thread(_modbus_write_register, D102_FREQ_ADDR_WORD, int_frequency)
                await asyncio.to_thread(_modbus_write_register, D104_ACCEL_ADDR, int_accelerate)
                await asyncio.to_thread(_modbus_write_register, D105_DECEL_ADDR, int_decelerate)
                
                # print(f"[{current_time}] [DB CONTROL] ➡️ 주파수/가감속 D 레지스터 ({int_frequency}/{int_accelerate}/{int_decelerate}) 업데이트.")

            elif select_success:
                pass # DB에 데이터가 없거나 조회 실패

            # =================================================================
            # 2. M0040 상태 변화 감지 로직 (컨베이어 센서)
            # =================================================================    
            # 1. PLC 데이터 읽기
            current_m0040_value = await asyncio.to_thread(read_plc_m0040)
            current_m0041_value = await asyncio.to_thread(read_plc_m0041)
            
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")

            if current_m0040_value != -1 and current_m0040_value != last_m0040_value:
                
                # 🚨 [수정된 핵심 로직: 1이든 0이든 Method 호출]
                sensor_check_conveyor = (current_m0040_value == 1)
                state_desc = "ON (감지)" if sensor_check_conveyor else "OFF (대기)"
                
                print(f"\n--- ## 센서 감지: M0040 (컨베이어) ## ---")
                print(f"[{current_time}] [SENSOR] 🔔 M0040 상태 변화: {state_desc}")
                
                # DB 로그는 1(ON) 상태일 때만 기록하도록 유지 (선택 사항)
                if sensor_check_conveyor:
                    log_desc = "Conveyor_Sensor_Check OK"
                    asyncio.create_task(
                        asyncio.to_thread(insert_log_sync, 'SENSER01', 'PLC', log_desc)
                    )
                    time.sleep(0.05)
                    # 🚨 DB 로그에 "Conveyor STOP" 기록: PLC 로직이 정지를 수행할 때를 대비
                    log_desc = "Conveyor STOP"
                    asyncio.create_task(
                        asyncio.to_thread(insert_log_sync, 'CONVEYOR01', 'PLC', log_desc)
                    )
                
                # Method 호출 및 결과 수신 (True/False 값 전송)
                method_node_id, result = await call_method_with_plc_data(opcua_client, SENSOR1_METHOD_NODE_ID, sensor_check_conveyor)
                
                is_success, status_message = result
                method_name = SENSOR1_METHOD_NODE_ID.split(';')[-1]
                
                if is_success:
                    print(f"[OPC UA] ✅ M0040 상태 OPC UA 호출 성공 ({method_name})")
                else:
                    print(f"[OPC UA] ❌ M0040 상태 OPC UA 호출 실패 ({method_name})")
                    print(f"         -> 오류 상세: {status_message}", file=sys.stderr)

                last_m0040_value = current_m0040_value
                
            # =================================================================
            # 3. M0041 상태 변화 감지 로직 (로봇팔 센서)
            # =================================================================
            if current_m0041_value != -1 and current_m0041_value != last_m0041_value:
                
                # 🚨 [수정된 핵심 로직: 1이든 0이든 Method 호출]
                sensor_check_robotarm = (current_m0041_value == 1)
                state_desc = "ON (감지)" if sensor_check_robotarm else "OFF (대기)"

                print(f"\n--- ## 센서 감지: M0041 (로봇팔) ## ---")
                print(f"[{current_time}] [SENSOR] 🔔 M0041 상태 변화: {state_desc}")
                
                # DB 로그는 1(ON) 상태일 때만 기록하도록 유지 (선택 사항)
                if sensor_check_robotarm:
                    log_desc = "RobotArm_Sensor_Check OK"
                    asyncio.create_task(
                        asyncio.to_thread(insert_log_sync, 'SENSER02', 'PLC', log_desc)
                    )
                    time.sleep(0.05)
                    # 🚨 DB 로그에 "Conveyor STOP" 기록: PLC 로직이 정지를 수행할 때를 대비
                    log_desc = "Conveyor STOP"
                    asyncio.create_task(
                        asyncio.to_thread(insert_log_sync, 'CONVEYOR01', 'PLC', log_desc)
                    )

                # Method 호출 및 결과 수신 (True/False 값 전송)
                method_node_id, result = await call_method_with_plc_data(opcua_client, SENSOR2_METHOD_NODE_ID, sensor_check_robotarm) 
                
                is_success, status_message = result
                method_name = SENSOR2_METHOD_NODE_ID.split(';')[-1]
                
                if is_success:
                    print(f"[OPC UA] ✅ M0041 상태 OPC UA 호출 성공 ({method_name})")
                else:
                    print(f"[OPC UA] ❌ M0041 상태 OPC UA 호출 실패 ({method_name})")
                    print(f"         -> 오류 상세: {status_message}", file=sys.stderr)
                
                last_m0041_value = current_m0041_value

            # 짧은 대기 시간 설정 (0.2초)
            await asyncio.sleep(0.2)

    except ConnectionRefusedError:
        print(f"--- ## 프로그램 종료 ## ---")
        print(f"[ERROR] 🚨 OPC UA 연결 거부: 서버 주소 {SERVER_URL}를 확인하세요.", file=sys.stderr)
    except Exception as e:
        print(f"--- ## 프로그램 오류 ## ---")
        print(f"[ERROR] 🚨 예상치 못한 오류 발생: {e.__class__.__name__} - {e}", file=sys.stderr)
    finally:
        # 연결 종료
        print(f"--- ## 연결 해제 및 종료 ## ---")
        try:
            if 'sub_anomaly' in locals():
                await sub_anomaly.delete()
                print("[CLEANUP] OPC UA Anomaly 구독 해지.")
            if 'conveyor_move' in locals():
                await conveyor_move.delete()
                print("[CLEANUP] OPC UA HMI 명령 구독 해지.")
            
            await opcua_client.disconnect()
            print("[CLEANUP] OPC UA 연결 종료.")
        except Exception:
            pass
            
        try:
            modbus_client.close()
            print("[CLEANUP] Modbus 연결 종료.")
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] 프로그램이 사용자 요청으로 종료되었습니다.")