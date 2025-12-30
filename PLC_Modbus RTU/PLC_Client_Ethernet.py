import asyncio
from asyncua import Client, ua
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException
import time
import json 
# from asyncua.common.methods import call_method # OPC UA Method 호출 시 필요

# --- 1. 설정 정보 (사용자 환경에 맞게 반드시 수정) ---
# OPC UA 서버 설정
SERVER_URL = "opc.tcp://172.30.1.61:0630/freeopcua/server/" 
OBJECT_NODE_ID = "ns=2;i=2"      # Method가 속한 Object Node ID
SENSOR1_METHOD_NODE_ID = "ns=2;s=write_conveyor_sensor_check"    # 컨베이어 Method Node ID (write_conveyor_sensor_check)
SENSOR2_METHOD_NODE_ID = "ns=2;s=write_robotarm_sensor_check"    # 로봇팔 Method Node ID (write_robotarm_sensor_check)

# PLC Modbus 설정
PLC_IP = '192.168.1.2'  # PLC의 실제 IP 주소로 변경
PLC_PORT = 502          # Modbus TCP 기본 포트
M0010_ADDRESS = 64      # M0010 접점의 Modbus Coil Address (컨베이어)
M0011_ADDRESS = 65      # M0011 접점의 Modbus Coil Address (로봇팔)
SLAVE_ID = 3            # Modbus Slave ID

# Anomaly 처리 관련 설정
# 🚨 ANOMALY 상태를 수신하는 OPC UA Node ID (AMR 구독 노드)
ANOMALY_OPCUA_NODE_ID = "ns=2;s=read_ok_ng_value" 

# 코일 주소 정의 (M0020/M0021)
PLC_WRITE_COIL_NG = 66      # M0020 코일 주소 (NG/불량 시 펄스)
PLC_WRITE_COIL_OK = 68      # M0021 코일 주소 (OK/정상 시 펄스)

# OPC UA 연결 재시도 횟수 설정
MAX_RETRY = 5

# Modbus 클라이언트 객체 초기화
modbus_client = ModbusTcpClient(PLC_IP, port=PLC_PORT)

# -----------------------------------------------------------------------------
# 🚨 5. OPC UA Subscription Handler 클래스 (OK/NG 기반 JSON 파싱 로직 적용)
# -----------------------------------------------------------------------------
class AnomalyDataHandler:
    """OPC UA 서버로부터 Anomaly 상태 변화를 수신하는 핸들러."""
    def datachange_notification(self, node, val, data):
        """Node 값이 변경될 때마다 호출됩니다."""
        # val은 구독된 변수의 최신 값입니다.
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"\n*** [{current_time}] 🔴 OPC UA 구독 변화 감지: 수신된 값='{val}' ***")
        
        is_anomaly = None
        status_code = None
        raw_val = val

        if isinstance(raw_val, str):
            status_str_raw = raw_val.strip()
            
            # 1. 수신된 문자열에서 JSON 부분 ({...})만 강제로 추출 시도
            start_index = status_str_raw.find('{')
            end_index = status_str_raw.rfind('}')
            
            if start_index != -1 and end_index != -1 and end_index > start_index:
                json_part = status_str_raw[start_index : end_index + 1]
                
                try:
                    # 2. 추출된 JSON 부분 파싱
                    data_dict = json.loads(json_part)
                    status_code = data_dict.get("Anomaly", None)
                    print(f"[ANOMALY_SUB] ❗ 상태 문자열에서 JSON 부분만 추출하여 파싱 성공: {json_part}")
                except json.JSONDecodeError:
                    # 3. 추출된 부분도 JSON이 아닌 경우, 원본 문자열을 상태 코드로 간주 (예: "Ready")
                    status_code = status_str_raw
            else:
                # 3. JSON 구조가 전혀 없는 경우, 문자열 자체를 상태 코드로 간주 (예: "Ready")
                status_code = status_str_raw

        elif isinstance(raw_val, dict):
            # 딕셔너리로 수신된 경우
            status_code = raw_val.get("Anomaly", None)
        
        # 4. 상태 코드 (OK/NG)를 불량(True) 또는 정상(False)으로 형 변환
        if status_code is not None:
            status_str_upper = str(status_code).upper()
            
            if status_str_upper == 'OK':
                is_anomaly = False  # OK는 정상 (M0021 펄스)
            elif status_str_upper == 'NG':
                is_anomaly = True   # NG는 불량 (M0020 펄스)
            # 다른 문자열(예: 'Ready', 'Error')은 처리하지 않음
        
        
        # 최종 Anomaly 상태 확인 및 펄스 실행
        if is_anomaly is not None:
            state_desc = "불량 감지 (NG)" if is_anomaly else "정상 처리 (OK)"
            print(f"*** [{current_time}] 🔴 Anomaly 상태 변화 처리 시작: {state_desc} ***")

            # 펄스 제어 로직을 별도 태스크로 실행 (메인 루프를 블록하지 않도록)
            asyncio.create_task(pulse_coil_on_anomaly(is_anomaly))
        else:
             print(f"[ANOMALY_SUB] 유효한 상태 값('OK', 'NG')을 찾지 못했습니다. 최종 상태: {status_code}. 처리 생략.")


# --- 2. Modbus TCP 통신 헬퍼 함수 ---
def _modbus_read_coil(address: int) -> int:
    """
    Modbus 코일의 상태를 읽어 1 또는 0을 반환하는 내부 헬퍼 함수.
    """
    try:
        result = modbus_client.read_coils(address=address, count=1, slave=SLAVE_ID)
        
        if result.isError():
            print(f"[MODBUS] 코일 읽기 통신 오류 (A:{address}): {result}")
            return -1 
        
        return 1 if result.bits[0] else 0 

    except ModbusException as e:
        print(f"[MODBUS] 코일 Modbus 예외 발생 (A:{address}, 연결 끊김 예상): {e}")
        return -1
    except Exception as e:
        print(f"[MODBUS] 코일 예기치 않은 오류 발생 (A:{address}): {e}")
        return -1


def _modbus_read_holding_register(address: int) -> int:
    """
    Modbus TCP를 사용하여 Holding Register의 값을 읽어 반환합니다.
    """
    try:
        # Holding Register (HR)를 읽습니다.
        result = modbus_client.read_holding_registers(address=address, count=1, slave=SLAVE_ID)
        
        if result.isError():
            print(f"[MODBUS] HR 읽기 통신 오류 (A:{address}): {result}")
            return -1 
        
        # 읽은 값을 반환
        return result.registers[0] if result.registers else -1

    except ModbusException as e:
        print(f"[MODBUS] HR Modbus 예외 발생 (A:{address}, 연결 끊김 예상): {e}")
        return -1
    except Exception as e:
        print(f"[MODBUS] HR 예기치 않은 오류 발생 (A:{address}): {e}")
        return -1


def _modbus_write_coil(address: int, value: int) -> int:
    """
    Modbus 코일의 상태를 1 또는 0으로 설정하는 내부 헬퍼 함수.
    """
    if value not in [0, 1]:
        print(f"[MODBUS] 쓰기 실패 (A:{address}): 유효하지 않은 값 ({value}). 0 또는 1만 허용됩니다.")
        return -1

    try:
        write_value = True if value == 1 else False
        # FIX: pymodbus ModbusTcpClient에서 write_single_coil 대신 write_coil을 사용하도록 수정
        result = modbus_client.write_coil(address=address, value=write_value, slave=SLAVE_ID)
        
        if result.isError():
            print(f"[MODBUS] 코일 쓰기 통신 오류 (A:{address}): {result}")
            return -1
        
        return 0 # 성공

    except ModbusException as e:
        print(f"[MODBUS] 코일 Modbus 예외 발생 (A:{address}, 연결 끊김 예상): {e}")
        return -1
    except Exception as e:
        # 이 예외는 ModbusTcpClient 객체에 해당 메서드가 없을 때도 발생합니다.
        print(f"[MODBUS] 코일 예기치 않은 오류 발생 (A:{address}): {e}")
        return -1


# --- 3. PLC Coil Read 함수 (기존 함수 유지) ---
def read_plc_m0010() -> int:
    """
    Modbus TCP를 사용하여 M0010 (Coil)의 상태를 읽어 1 또는 0을 반환합니다.
    """
    return _modbus_read_coil(M0010_ADDRESS)

def read_plc_m0011() -> int:
    """
    Modbus TCP를 사용하여 M0011 (Coil)의 상태를 읽어 1 또는 0을 반환합니다.
    """
    return _modbus_read_coil(M0011_ADDRESS)


# --- 4. Anomaly 펄스 제어 로직 ---
async def pulse_coil_on_anomaly(is_anomaly: bool):
    """
    Anomaly 상태에 따라 M0020 (불량) 또는 M0021 (정상) 코일 중 하나를 1초 동안 펄스 제어합니다.
    :param is_anomaly: True=불량 (NG) -> M0020 펄스, False=정상 (OK) -> M0021 펄스
    """
    if is_anomaly:
        target_address = PLC_WRITE_COIL_NG
        target_name = "M0020 (불량/NG)"
    else:
        target_address = PLC_WRITE_COIL_OK
        target_name = "M0021 (정상/OK)"
        
    print(f"\n[ANOMALY_PULSE] 🚨 {target_name} 코일에 1초 펄스 명령 실행 시작.")

    # ON (1) 설정 및 1초 대기 로직을 별도의 스레드에서 실행하는 래퍼 함수
    def blocking_pulse():
        # 1. ON (1) 설정
        if _modbus_write_coil(target_address, 1) != 0:
            return -1 # 쓰기 실패

        # 2. 1초 대기 (이 부분이 메인 루프를 블록하지 않도록 to_thread로 감싸짐)
        time.sleep(1) 

        # 3. OFF (0) 설정
        if _modbus_write_coil(target_address, 0) != 0:
            return -1 # 쓰기 실패
            
        return 0

    try:
        # Modbus 통신과 time.sleep()이 포함된 blocking_pulse를 별도 스레드에서 실행
        result = await asyncio.to_thread(blocking_pulse)
        
        if result == 0:
            print(f"[ANOMALY_PULSE] ✅ {target_name} 펄스 완료.")
        else:
            print(f"[ANOMALY_PULSE] ❌ {target_name} 펄스 실패 (Modbus 쓰기 오류).")
            
        return result
        
    except Exception as e:
        print(f"[ANOMALY_PULSE] ❌ 예기치 않은 오류 발생: {e}")
        return -1


# --- 6. OPC UA Method 호출 로직 (기존 함수 유지) ---
async def call_method_with_plc_data(client: Client, method_node_id: str, sensor_check: bool):
    """M0010/M0011 상태를 OPC UA 서버에 Method로 전송합니다."""
    try:
        obj_node = client.get_node(OBJECT_NODE_ID)
        method_node = client.get_node(method_node_id) 

        # Method 호출을 위한 입력 인자 설정 (Boolean 값)
        arguments = [
            ua.Variant(sensor_check, ua.VariantType.Boolean) 
        ]
        
        # Method 호출
        result = await obj_node.call_method(method_node, *arguments)
        
        # 성공 시: Method Node ID와 서버 응답 결과 반환
        return method_node_id, result

    except Exception as e:
        # 오류 발생 시: Method Node ID와 (False, 오류 메시지) 구조를 반환
        error_message = f"❌ OPC UA 호출 중 오류 발생: {e.__class__.__name__} - {e}"
        # 서버에서 응답이 없거나 예외 발생 시 (False, 오류 메시지)를 반환
        return method_node_id, (False, error_message)


# --- 7. 메인 실행 함수 (OPC UA 구독 로직 적용) ---
async def main():
    opcua_client = Client(url=SERVER_URL)
    
    print(f"OPC UA 서버 접속 시도: {SERVER_URL}")

    last_m0010_value = -2  # 이전 M0010 값을 저장 (컨베이어)
    last_m0011_value = -2  # 이전 M0011 값을 저장 (로봇팔)

    try:
        # 1. OPC UA 연결 시도 (재시도 로직 추가)
        connected = False
        for retry_count in range(MAX_RETRY):
            try:
                await opcua_client.connect()
                print("🎉 OPC UA 서버 연결 성공!")
                connected = True
                break
            except (ConnectionRefusedError, TimeoutError, Exception) as e:
                print(f"🚨 OPC UA 연결 실패 (시도 {retry_count + 1}/{MAX_RETRY}): {e.__class__.__name__} - {e}")
                if retry_count < MAX_RETRY - 1:
                    print("   -> 5초 후 재시도합니다.")
                    await asyncio.sleep(5)
                else:
                    print(f"🚨 OPC UA 연결 재시도 횟수({MAX_RETRY}회) 초과. 프로그램 종료.")
                    return # 연결 실패 시 메인 함수 종료

        if not connected:
            return # 연결 실패 시 종료

        # 2. Modbus 클라이언트 연결 시도 
        if not modbus_client.connect():
            print(f"🚨 Modbus 연결 실패: {PLC_IP}:{PLC_PORT}를 확인하세요.")
            return  # 연결 실패 시 메인 함수 종료
        print(f"🎉 Modbus 연결 성공: {PLC_IP}:{PLC_PORT}")

        # ---------------------------------------------------------------------
        # 🚨 3. OPC UA Anomaly 상태 구독 시작 (비동기 데이터 수신)
        # ---------------------------------------------------------------------
        handler = AnomalyDataHandler()
        sub = await opcua_client.create_subscription(100, handler) # 100ms 샘플링 간격
        
        # AMR 구독 노드 가져오기
        try:
            # amr_subscriber에서 사용된 경로를 직접 탐색하는 방식
            anomaly_node = await opcua_client.nodes.root.get_child([
                "0:Objects",
                "2:PLC",
                "2:read_ok_ng_value"
            ])
            print(f"✅ AMR 노드 경로 탐색 성공: {await anomaly_node.read_browse_name()}")
            
        except Exception as e:
            # 예시 ID를 사용하여 노드 가져오기 시도
            try:
                 anomaly_node = opcua_client.get_node(ANOMALY_OPCUA_NODE_ID)
                 print(f"✅ ANOMALY_OPCUA_NODE_ID ({ANOMALY_OPCUA_NODE_ID})로 노드 가져오기 성공.")
            except Exception:
                 print(f"❌ AMR 노드 탐색 실패: AMR 경로 및 ANOMALY_OPCUA_NODE_ID ({ANOMALY_OPCUA_NODE_ID}) 모두 유효하지 않습니다.")
                 print(f"   오류 상세: {e.__class__.__name__} - {e}")
                 # 구독을 시작하지 않고 메인 루프 계속
                 anomaly_node = None


        if anomaly_node:
            await sub.subscribe_data_change(anomaly_node)
            # FIX: read_node_id 대신 .nodeid 속성 사용 (AttributeError 해결)
            print(f"✅ OPC UA 구독 시작: {anomaly_node.nodeid}")
        else:
            print("⚠️ OPC UA 구독을 시작할 유효한 노드를 찾지 못했습니다. Anomaly 펄스 기능이 작동하지 않습니다.")
        # ---------------------------------------------------------------------

        # 0.2초마다 PLC 데이터 읽기 (M0010/M0011 폴링 유지)
        while True:
            # 1. PLC 데이터 읽기
            current_m0010_value = await asyncio.to_thread(read_plc_m0010)
            current_m0011_value = await asyncio.to_thread(read_plc_m0011)
            
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")

            # =================================================================
            # 2. M0010 상태 변화 감지 로직 (컨베이어 센서)
            # =================================================================
            if current_m0010_value != -1 and current_m0010_value != last_m0010_value:
                
                sensor_check_conveyor = current_m0010_value == 1
                state_desc = "ON (True)" if current_m0010_value == 1 else "OFF (False)"
                
                print(f"\n*** [{current_time}] 🔔 컨베이어 상태 변화 감지: M0010 -> {state_desc} ***")
                
                # Method 호출 및 결과 수신
                method_node_id, result = await call_method_with_plc_data(opcua_client, SENSOR1_METHOD_NODE_ID, sensor_check_conveyor)
                
                # M0010 전용 출력 로직
                is_success, status_message = result
                method_name = SENSOR1_METHOD_NODE_ID.split(';')[-1]
                
                if is_success:
                    print(f"✅ M0010 -> OPC UA 호출 성공 ({method_name})")
                    print(f"   -> 서버 응답: Success={is_success}, Message='{status_message}'")
                else:
                    print(f"❌ M0010 -> OPC UA 호출 실패 ({method_node_id})")
                    print(f"   -> 오류 상세: {status_message}")

                # 이전 상태 업데이트
                last_m0010_value = current_m0010_value
                
            # =================================================================
            # 3. M0011 상태 변화 감지 로직 (로봇팔 센서)
            # =================================================================
            if current_m0011_value != -1 and current_m0011_value != last_m0011_value:
                
                sensor_check_robotarm = current_m0011_value == 1
                state_desc = "ON (True)" if current_m0011_value == 1 else "OFF (False)"

                print(f"\n*** [{current_time}] 🔔 로봇팔 상태 변화 감지: M0011 -> {state_desc} ***")
                
                # Method 호출 및 결과 수신
                method_node_id, result = await call_method_with_plc_data(opcua_client, SENSOR2_METHOD_NODE_ID, sensor_check_robotarm) 
                
                # M0011 전용 출력 로직
                is_success, status_message = result
                method_name = SENSOR2_METHOD_NODE_ID.split(';')[-1]
                
                if is_success:
                    print(f"✅ M0011 -> OPC UA 호출 성공 ({method_name})")
                    print(f"   -> 서버 응답: Success={is_success}, Message='{status_message}'")
                else:
                    print(f"❌ M0011 -> OPC UA 호출 실패 ({method_node_id})")
                    print(f"   -> 오류 상세: {status_message}")
                
                # 이전 상태 업데이트
                last_m0011_value = current_m0011_value
            
            # =================================================================
            # 4. Anomaly 상태 감지 로직 (Modbus 폴링 제거됨 - 이제 구독이 처리)
            # =================================================================

            # 짧은 대기 시간 설정 (0.2초)
            await asyncio.sleep(0.1)

    except ConnectionRefusedError:
        print(f"🚨 OPC UA 연결 거부: 서버 주소 {SERVER_URL}를 확인하세요.")
    except Exception as e:
        print(f"🚨 예상치 못한 오류 발생: {e.__class__.__name__} - {e}")
    finally:
        # 연결 종료
        try:
            # 구독 해지
            if 'sub' in locals():
                await sub.delete()
                print("\nOPC UA 구독 해지.")
            
            await opcua_client.disconnect()
            print("OPC UA 연결 종료.")
        except Exception:
            pass
            
        try:
            modbus_client.close()
            print("Modbus 연결 종료.")
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n프로그램을 종료합니다.")