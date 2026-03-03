import asyncio
import json         
from asyncua import Server, ua
from datetime import datetime
import numpy as np
import base64
import cv2
import logging
import time 
import sys
import os

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='opcua_server.log',  # 로그 파일로 출력
                    filemode='a')
logger = logging.getLogger('OPCUA_SERVER')

# --- 전역 설정 ---
node_id_type = ua.NodeIdType.String

# --- Modbus TCP 설정 ---
MODBUS_REGISTERS = {
    80 : 0
}

image_data_var = None

# ------------------------------------------------------------------------------------- #

# OPC UA 메소드 구현을 위한 비동기 클래스 정의
class ServerMethods:
    def __init__(self, server_instance, idx):
        self.server = server_instance
        self.idx = idx
        self.objects_node = self.server.nodes.objects
        self.read_amr_go_move_node = None                   
        self.read_amr_go_positions_node = None              
        self.read_amr_mission_state_node = None             

        self.read_converyor_sensor_check_node = None        
        self.read_ok_ng_value_node = None                   
        self.read_robotarm_sensor_check_node = None         
        self.read_ready_state_node = None                   

        self.read_send_arm_json_node = None                 
        self.read_arm_go_move_node = None                   
        self.read_arm_place_single_node = None              
        self.read_arm_place_completed_node = None           

        self.read_send_arm_img_node = None                  

    async def _reset_variable_after_delay(self, variable_node, delay=1, reset_value="Ready"):
        """Method 응답과 독립적으로 일정 시간 후 변수를 초기화하는 백그라운드 태스크"""
        await asyncio.sleep(delay)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] [OPCUA][TASK] 노드 초기화 대기 종료. ID: {variable_node.nodeid.Identifier}")
        
        # 노드 초기화
        await variable_node.write_value(reset_value)
        print(f"[{current_time}] [OPCUA][TASK] ✅ 노드 '{variable_node.nodeid.Identifier}'를 '{reset_value}'로 복원 완료.")

    async def init_nodes(self):
        """데이터를 수신 시스템에 노출하기 위한 Read 전용 노드 및 Object 정의"""

        # 1. Object 그룹 정의
        synchrobots_AMR = await self.objects_node.add_object(self.idx, "AMR")
        synchrobots_PLC = await self.objects_node.add_object(self.idx, "PLC")
        synchrobots_ARM = await self.objects_node.add_object(self.idx, "ARM")
        synchrobots_IMG = await self.objects_node.add_object(self.idx, "IMG")

        # 2. Variable 노드 정의 (노드 등록 로직은 가독성 유지를 위해 생략)
        
        # ----------------------------------------------------------------------
        # AMR 그룹
        # ----------------------------------------------------------------------
        self.read_amr_go_move_node = await synchrobots_AMR.add_variable(
            ua.NodeId("read_amr_go_move", self.idx, node_id_type), "read_amr_go_move", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))
        self.read_amr_go_positions_node = await synchrobots_AMR.add_variable(
            ua.NodeId("read_amr_go_positions", self.idx, node_id_type), "read_amr_go_positions", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))
        self.read_amr_mission_state_node = await synchrobots_AMR.add_variable(
            ua.NodeId("read_amr_mission_state", self.idx, node_id_type), "read_amr_mission_state", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))

        # ----------------------------------------------------------------------
        # PLC 그룹
        # ----------------------------------------------------------------------
        self.read_converyor_sensor_check_node = await synchrobots_PLC.add_variable(
            ua.NodeId("read_conveyor_sensor_check", self.idx, node_id_type), "read_conveyor_sensor_check", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))
        global MODBUS_REGISTERS
        self.read_ok_ng_value_node = await synchrobots_PLC.add_variable(
            ua.NodeId("read_ok_ng_value", self.idx, node_id_type), "read_ok_ng_value", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))
        self.read_robotarm_sensor_check_node = await synchrobots_PLC.add_variable(
            ua.NodeId("read_robotarm_sensor_check", self.idx, node_id_type), "read_robotarm_sensor_check", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))
        self.read_ready_state_node = await synchrobots_PLC.add_variable(
            ua.NodeId("read_ready_state", self.idx, node_id_type), "read_ready_state", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))

        # ----------------------------------------------------------------------
        # ARM 그룹
        # ----------------------------------------------------------------------
        self.read_send_arm_json_node = await synchrobots_ARM.add_variable(
            ua.NodeId("read_send_arm_json", self.idx, node_id_type), "read_send_arm_json", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))
        self.read_arm_go_move_node = await synchrobots_ARM.add_variable(
            ua.NodeId("read_arm_go_move", self.idx, node_id_type), "read_arm_go_move", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))
        self.read_arm_place_single_node = await synchrobots_ARM.add_variable(
            ua.NodeId("read_arm_place_single", self.idx, node_id_type), "read_arm_place_single", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))
        self.read_arm_place_completed_node = await synchrobots_ARM.add_variable(
            ua.NodeId("read_arm_place_completed", self.idx, node_id_type), "read_arm_place_completed", "Ready", datatype=ua.NodeId(ua.ObjectIds.String))

        # ----------------------------------------------------------------------
        # IMG 그룹
        # ----------------------------------------------------------------------
        global image_data_var
        self.read_send_arm_img_node = await synchrobots_IMG.add_variable(
            ua.NodeId("read_send_arm_img", self.idx, node_id_type), "read_send_arm_img", b'', datatype=ua.NodeId(ua.ObjectIds.ByteString))
        image_data_var = self.read_send_arm_img_node

        return {
            "AMR": synchrobots_AMR,
            "PLC": synchrobots_PLC,
            "ARM": synchrobots_ARM,
            "IMG": synchrobots_IMG
        }
    
    # -----------------------------------------------------
    # AMR_001 인터페이스 로직 (Web PC -> AMR)
    # -----------------------------------------------------
    async def call_amr_go_move(self, parent_node, json_amr_go_move_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- [METHOD INVOKE] AMR_001: write_amr_go_move ---")
        print(f"[{current_time}] [AMR] ➡️ 호출 수신: AMR 이동 명령")

        if isinstance(json_amr_go_move_data_str, ua.Variant):
            command_str = json_amr_go_move_data_str.Value
        else:
            command_str = json_amr_go_move_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore")

        if not isinstance(command_str, str):
            command_str = str(command_str)

        print(f"[{current_time}] [AMR] 🔍 수신 데이터: {command_str!r}")

        amr_success = False
        amr_message = ""
        
        try:
            json.loads(command_str)

            await self.read_amr_go_move_node.write_value(command_str)

            print(f"[{current_time}] [AMR] ✅ 노드 쓰기 성공. ID: {self.read_amr_go_move_node.nodeid.Identifier}")
            print(f"[{current_time}] [OPCUA][TASK] 3초 후 노드 자동 초기화 태스크 생성.")
            asyncio.create_task(self._reset_variable_after_delay(self.read_amr_go_move_node))

            amr_success = True
            amr_message = f"Command '{command_str}' received and routed. Reset scheduled."
            print(f"[AMR] 🚀 처리 완료: {amr_message}")

        except json.JSONDecodeError as e:
            amr_success = False
            amr_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [AMR][ERROR] ❌ JSON 디코딩 오류: {amr_message}", file=sys.stderr)
        except Exception as e:
            amr_success = False
            amr_message = f"AMR 통신 또는 처리 실패. Details: {e}"
            print(f"[{current_time}] [AMR][ERROR] ❌ 일반 오류: {amr_message}", file=sys.stderr)

        return [
            ua.Variant(amr_success, ua.VariantType.Boolean),
            ua.Variant(amr_message, ua.VariantType.String)
        ]
    
    # -----------------------------------------------------
    # AMR_002 인터페이스 로직 (Web -> AMR)
    # -----------------------------------------------------
    async def call_amr_go_position(self, parent_node, json_amr_go_position_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- [METHOD INVOKE] AMR_002: write_amr_go_positions ---")
        print(f"[{current_time}] [AMR] ➡️ 호출 수신: AMR 위치 명령")

        if isinstance(json_amr_go_position_data_str, ua.Variant):
            position_str = json_amr_go_position_data_str.Value
        else:
            position_str = json_amr_go_position_data_str

        if isinstance(position_str, bytes):
            position_str = position_str.decode("utf-8", errors="ignore")

        if not isinstance(position_str, str):
            position_str = str(position_str)

        print(f"[{current_time}] [AMR] 🔍 수신 데이터: {position_str!r}")
        
        amr_success = False
        amr_message = ""
        
        try:
            json.loads(position_str)

            await self.read_amr_go_positions_node.write_value(position_str)

            print(f"[{current_time}] [AMR] ✅ 노드 쓰기 성공. ID: {self.read_amr_go_positions_node.nodeid.Identifier}")
            print(f"[{current_time}] [OPCUA][TASK] 3초 후 노드 자동 초기화 태스크 생성.")
            asyncio.create_task(self._reset_variable_after_delay(self.read_amr_go_positions_node))

            amr_success = True
            amr_message = f"Command '{position_str}' received and routed. Reset scheduled."
            print(f"[AMR] 🚀 처리 완료: {amr_message}")

        except json.JSONDecodeError as e:
            amr_success = False
            amr_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [AMR][ERROR] ❌ JSON 디코딩 오류: {amr_message}", file=sys.stderr)
            
        except ValueError as e:
            amr_success = False
            amr_message = f"Error: JSON data validation failed. Details: {e}"
            print(f"[{current_time}] [AMR][ERROR] ❌ 데이터 유효성 오류: {amr_message}", file=sys.stderr)
            
        except Exception as e:
            amr_success = False
            amr_message = f"AMR 통신 또는 처리 실패. Details: {e}"
            print(f"[{current_time}] [AMR][ERROR] ❌ 일반 오류: {amr_message}", file=sys.stderr)
 
        return [
            ua.Variant(amr_success, ua.VariantType.Boolean),
            ua.Variant(amr_message, ua.VariantType.String)
        ]

    # -----------------------------------------------------
    # AMR_003 인터페이스 로직 (AMR -> WEB)
    # -----------------------------------------------------
    async def call_amr_mission_state(self, parent_node, json_amr_mission_state_query_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- [METHOD INVOKE] AMR_003: write_amr_mission_state ---")
        print(f"[{current_time}] [AMR] ➡️ 호출 수신: AMR 임무 상태 보고")

        if isinstance(json_amr_mission_state_query_str, ua.Variant):
            query_str = json_amr_mission_state_query_str.Value
        else:
            query_str = json_amr_mission_state_query_str

        if isinstance(query_str, bytes):
            query_str = query_str.decode("utf-8", errors="ignore")

        if not isinstance(query_str, str):
            query_str = str(query_str)

        print(f"[{current_time}] [AMR] 🔍 수신 데이터: {query_str!r}")

        amr_success = False
        amr_message = ""
        
        try:
            json.loads(query_str)

            await self.read_amr_mission_state_node.write_value(query_str)

            print(f"[{current_time}] [AMR] ✅ 노드 쓰기 성공. ID: {self.read_amr_mission_state_node.nodeid.Identifier}")
            print(f"[{current_time}] [OPCUA][TASK] 3초 후 노드 자동 초기화 태스크 생성.")
            asyncio.create_task(self._reset_variable_after_delay(self.read_amr_mission_state_node))

            amr_success = True
            amr_message = f"Command '{query_str}' received and routed. Reset scheduled."
            print(f"[AMR] 🚀 처리 완료: {amr_message}")

        except json.JSONDecodeError as e:
            amr_success = False
            amr_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [AMR][ERROR] ❌ JSON 디코딩 오류: {amr_message}", file=sys.stderr)
        except Exception as e:
            amr_success = False
            amr_message = f"AMR 통신 또는 처리 실패. Details: {e}"
            print(f"[{current_time}] [AMR][ERROR] ❌ 일반 오류: {amr_message}", file=sys.stderr)

        return [
            ua.Variant(amr_success, ua.VariantType.Boolean),
            ua.Variant(amr_message, ua.VariantType.String)
        ]
    
    # -----------------------------------------------------
    # PLC_001 (PLC -> WEB)
    # -----------------------------------------------------
    async def call_conveyor_sensor_check(self, parent_node, json_conveyor_sensor_check_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] [OPCUA][SERVER] call_conveyor_sensor_check called")

        if isinstance(json_conveyor_sensor_check_data_str, ua.Variant):
            raw_value = json_conveyor_sensor_check_data_str.Value
        else:
            raw_value = json_conveyor_sensor_check_data_str

        if isinstance(raw_value, (int, float)):
            is_sensor_ok = (raw_value != 0)
        else:
            is_sensor_ok = bool(raw_value)

        print(f"[{current_time}] [OPCUA][SERVER] conveyor_senser_check : {is_sensor_ok}")

        if is_sensor_ok:
            status_message = "Check OK"
        else:
            status_message = "Ready"

        await self.read_converyor_sensor_check_node.set_value(status_message)
        print(f"[{current_time}] [OPCUA][SERVER] read_conveyor_sensor_check 노드 갱신: {status_message}")
        print(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ")
        
        return [
            ua.Variant(True, ua.VariantType.Boolean),
            ua.Variant("Success: Sensor check signal processed.", ua.VariantType.String),
        ]

    # -----------------------------------------------------
    # PLC_002 (WEB -> PLC)
    # -----------------------------------------------------
    async def call_ok_ng_value(self, parent_node, json_ok_ng_value_data_str):
        global MODBUS_REGISTERS, modbus_context
        modbus_register_address = 80
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"\n--- [METHOD INVOKE] PLC_002: write_ok_ng_value ---")
        print(f"[{current_time}] [PLC] ➡️ 호출 수신: OK/NG 판별 값")

        if isinstance(json_ok_ng_value_data_str, ua.Variant):
            command_str = json_ok_ng_value_data_str.Value
        else:
            command_str = json_ok_ng_value_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore")

        if not isinstance(command_str, str):
            command_str = str(command_str)
        
        command_str = command_str.strip()
        
        print(f"[{current_time}] [PLC] 🔍 수신 데이터: {command_str!r}")
        
        result_code = 1
        result_message = ""
        modbus_value = 0 # 0: OK(정상), 1: NG(불량)

        try:
            anomaly_data = json.loads(command_str)
            
            if "Anomaly" in anomaly_data:
                anomaly_status = anomaly_data["Anomaly"]
                status_str_upper = str(anomaly_status).upper()
                
                if status_str_upper == 'NG':
                    modbus_value = 1 # NG = 1 (불량)
                    status_message = "NG" # 수정: 'NG'만 전송
                elif status_str_upper == 'OK':
                    modbus_value = 0 # OK = 0 (정상)
                    status_message = "OK" # 수정: 'OK'만 전송
                else:
                    raise ValueError(f"'Anomaly' key value must be 'OK' or 'NG', received: {anomaly_status}")
            else:
                status_message = "Anomaly key not found. Modbus Value: 0 (Default)"
                modbus_value = 0

            await self.read_ok_ng_value_node.set_value(status_message)
            
            print(f"[{current_time}] [PLC] ✅ 노드 갱신 완료. ID: {self.read_ok_ng_value_node.nodeid.Identifier}")
            print(f"[{current_time}] [OPCUA][TASK] 3초 후 노드 자동 초기화 태스크 생성.")
            asyncio.create_task(self._reset_variable_after_delay(self.read_ok_ng_value_node, reset_value="Ready"))

            result_code = 0
            # result_message = f"Command successfully processed. Status: {status_message}"
            result_message = f"Command successfully processed. Status: {status_str_upper} (Modbus: {modbus_value})"

        except json.JSONDecodeError:
            result_code = 1
            result_message = "Error: Input string is not a valid JSON."
            print(f"[{current_time}] [PLC][ERROR] ❌ JSON 디코딩 오류: {result_message}", file=sys.stderr)
            await self.read_ok_ng_value_node.set_value(f"JSON ERROR: {command_str}")
        except ValueError as e:
            result_code = 1
            result_message = f"Error: Validation failed. Details: {e}"
            print(f"[{current_time}] [PLC][ERROR] ❌ 데이터 유효성 오류: {result_message}", file=sys.stderr)
            await self.read_ok_ng_value_node.set_value(f"VALIDATION ERROR: {command_str}")
        except Exception as e:
            result_code = 1
            result_message = f"Modbus 통신 또는 처리 실패. Details: {e}"
            print(f"[{current_time}] [PLC][ERROR] ❌ 일반 오류: {result_message}", file=sys.stderr)
            await self.read_ok_ng_value_node.set_value(f"GENERAL ERROR: {command_str}")
        
        return [
            ua.Variant(result_code, ua.VariantType.Int32),
            ua.Variant(result_message, ua.VariantType.String)
        ]

    # -----------------------------------------------------
    # PLC_003 (PLC -> WEB)
    # -----------------------------------------------------
    async def call_robotarm_sensor_check(self, parent_node, json_robotarm_sensor_check_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 💡 실시간 반영
        print(f"[{current_time}] [OPCUA][SERVER] call_robotarm_sensor_check called")
        
        if isinstance(json_robotarm_sensor_check_data_str, ua.Variant):
            raw_value = json_robotarm_sensor_check_data_str.Value
        else:
            raw_value = json_robotarm_sensor_check_data_str

        if isinstance(raw_value, (int, float)):
            is_sensor_ok = (raw_value != 0)
        else:
            is_sensor_ok = bool(raw_value)

        print(f"[{current_time}] [OPCUA][SERVER] robotarm_sensor_check : {is_sensor_ok}")

        if is_sensor_ok:
            status_message = "Check OK"
        else:
            status_message = "Ready"

        await self.read_robotarm_sensor_check_node.set_value(status_message)
        print(f"[{current_time}] [OPCUA][SERVER] read_robotarm_sensor_check 노드 갱신: {status_message}")
        print(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ")
        
        return [
            ua.Variant(True, ua.VariantType.Boolean),
            ua.Variant("Success: Sensor check signal processed.", ua.VariantType.String),
        ]
    
    # -----------------------------------------------------
    # PLC_004 (WEB -> PLC)
    # -----------------------------------------------------
    async def call_ready_state(self, parent_node, json_ready_state_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- [METHOD INVOKE] PLC_004: write_ready_state ---")
        print(f"[{current_time}] [PLC] ➡️ 호출 수신: 준비 완료 명령 (HMI)")

        if isinstance(json_ready_state_data_str, ua.Variant):
            command_str = json_ready_state_data_str.Value
        else:
            command_str = json_ready_state_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore")

        if not isinstance(command_str, str):
            command_str = str(command_str)
        
        command_str = command_str.strip()

        print(f"[{current_time}] [PLC] 🔍 수신 데이터: {command_str!r}")

        plc_success = False
        plc_message = ""
        
        try:
            ready_data = json.loads(command_str)
            
            if ready_data.get("state", "").upper() == "CONVEYOR_MOVE":
                status_message = "CONVEYOR_MOVE Command Received"
            else:
                status_message = f"Received state: {ready_data.get('state')}"

            await self.read_ready_state_node.write_value(command_str)

            print(f"[{current_time}] [PLC] ✅ 노드 갱신 완료. ID: {self.read_ready_state_node.nodeid.Identifier}")
            print(f"[{current_time}] [OPCUA][TASK] 3초 후 노드 자동 초기화 태스크 생성.")
            asyncio.create_task(self._reset_variable_after_delay(self.read_ready_state_node))

            plc_success = True
            plc_message = f"Command '{status_message}' received and routed. Reset scheduled."
            print(f"[PLC] 🚀 처리 완료: {plc_message}")

        except json.JSONDecodeError as e:
            plc_success = False
            plc_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [PLC][ERROR] ❌ JSON 디코딩 오류: {plc_message}", file=sys.stderr)
        except Exception as e:
            plc_success = False
            plc_message = f"Processing failed. Details: {e}"
            print(f"[{current_time}] [PLC][ERROR] ❌ 일반 오류: {plc_message}", file=sys.stderr)

        return [
            ua.Variant(plc_success, ua.VariantType.Boolean),
            ua.Variant(plc_message, ua.VariantType.String)
        ]

    # -----------------------------------------------------
    # ARM_001 (ARM -> WEB)
    # -----------------------------------------------------
    async def call_send_arm_json(self, parent, json_arm_img_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        result_code = ua.Variant(0, ua.VariantType.Int32)
        result_message = ua.Variant("Success", ua.VariantType.String)
        content_to_write = ""

        print(f"\n--- [METHOD INVOKE] ARM_001: write_send_arm_json (JSON/Image) ---")
        print(f"[{current_time}] [ARM] ➡️ 호출 수신: JSON + Base64 이미지 데이터")

        try:
            if isinstance(json_arm_img_data_str, ua.Variant):
                raw_content = json_arm_img_data_str.Value
            else:
                raw_content = json_arm_img_data_str

            if isinstance(raw_content, bytes):
                content_to_write = raw_content.decode("utf-8", errors="ignore")
            elif isinstance(raw_content, str):
                content_to_write = raw_content
            else:
                content_to_write = str(raw_content)

            print(f"[{current_time}] [ARM] 🔍 수신 데이터 (앞 100자): {content_to_write[:100]}...")

            data = json.loads(content_to_write)
            
            base64_img_str = data.get("img")
            if base64_img_str:
                try:
                    img_bytes = base64.b64decode(base64_img_str)
                    np_arr = np.frombuffer(img_bytes, np.uint8)
                    decoded_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                    output_filename = "received_arm_json_image.jpg"
                    cv2.imwrite(output_filename, decoded_img)
                    print(f"[{current_time}] [ARM] ✅ 이미지 복원 및 저장 완료: {output_filename}")
                except Exception as e:
                    print(f"[{current_time}] [ARM][ERROR] ⚠️ 이미지 복원 중 오류: {e}")

            if 'status' in data:
                print(f"[{current_time}] [ARM] ℹ️ 미션 상태 보고 (Status): {data['status']}")
            elif 'module_type' in data:
                print(f"[{current_time}] [ARM] ℹ️ 비전 결과 보고 (Module Type): {data.get('module_type')}")
            else:
                print(f"[{current_time}] [ARM] ⚠️ 알 수 없는 데이터 구조 수신 (Keys: {list(data.keys())})")
            
            await self.read_send_arm_json_node.write_value(content_to_write)

            print(f"[{current_time}] [ARM] ✅ 노드 갱신 완료. ID: {self.read_send_arm_json_node.nodeid.Identifier}")
            print(f"[{current_time}] [OPCUA][TASK] 3초 후 노드 자동 초기화 태스크 생성.")
            asyncio.create_task(self._reset_variable_after_delay(self.read_send_arm_json_node))
            
            result_code = ua.Variant(0, ua.VariantType.Int32)
            result_message = ua.Variant("Data processed and written to Variable", ua.VariantType.String)
            print(f"[ARM] 🚀 처리 완료.")

        except json.JSONDecodeError:
            result_code = ua.Variant(2, ua.VariantType.Int32)
            result_message = ua.Variant("JSON Decode Error", ua.VariantType.String)
            print(f"[{current_time}] [ARM][ERROR] ❌ JSON 디코딩 오류 발생. 수신 데이터: {content_to_write[:100]}...", file=sys.stderr)
        except Exception as e:
            result_code = ua.Variant(5, ua.VariantType.Int32)
            result_message = ua.Variant(f"Unknown Error: {e}", ua.VariantType.String)
            print(f"[{current_time}] [ARM][ERROR] ❌ 알 수 없는 오류 발생: {e}", file=sys.stderr)
            
        return [result_code, result_message]
    
    # -----------------------------------------------------
    # ARM_002 (WEB -> ARM)
    # -----------------------------------------------------
    async def call_arm_go_move(self, parent_node, json_arm_go_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- [METHOD INVOKE] ARM_002: write_arm_go_move ---")
        print(f"[{current_time}] [ARM] ➡️ 호출 수신: 로봇팔 이동 명령")

        if isinstance(json_arm_go_data_str, ua.Variant):
            command_str = json_arm_go_data_str.Value
        else:
            command_str = json_arm_go_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore") 

        if not isinstance(command_str, str):
            command_str = str(command_str)

        print(f"[{current_time}] [ARM] 🔍 수신 데이터: {command_str!r}")

        arm_success = False
        arm_message = ""
        
        try:
            json.loads(command_str)

            await self.read_arm_go_move_node.write_value(command_str)

            print(f"[{current_time}] [ARM] ✅ 노드 쓰기 성공. ID: {self.read_arm_go_move_node.nodeid.Identifier}")
            print(f"[{current_time}] [OPCUA][TASK] 3초 후 노드 자동 초기화 태스크 생성.")
            asyncio.create_task(self._reset_variable_after_delay(self.read_arm_go_move_node))

            arm_success = True
            arm_message = f"Command '{command_str}' received and routed. Reset scheduled."
            print(f"[ARM] 🚀 처리 완료: {arm_message}")

        except json.JSONDecodeError as e:
            arm_success = False
            arm_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [ARM][ERROR] ❌ JSON 디코딩 오류: {arm_message}", file=sys.stderr)
        except Exception as e:
            arm_success = False
            arm_message = f"ARM 통신 또는 처리 실패. Details: {e}"
            print(f"[{current_time}] [ARM][ERROR] ❌ 일반 오류: {arm_message}", file=sys.stderr)

        return [
            ua.Variant(arm_success, ua.VariantType.Boolean),
            ua.Variant(arm_message, ua.VariantType.String)
        ]
    
    # -----------------------------------------------------
    # ARM_003 (ARM -> WEB)
    # -----------------------------------------------------
    async def call_arm_place_single(self, parent_node, json_arm_place_single_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- [METHOD INVOKE] ARM_003: write_arm_place_single ---")
        print(f"[{current_time}] [ARM] ➡️ 호출 수신: 단일 배치 완료 신호")

        if isinstance(json_arm_place_single_data_str, ua.Variant):
            command_str = json_arm_place_single_data_str.Value
        else:
            command_str = json_arm_place_single_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore")

        if not isinstance(command_str, str):
            command_str = str(command_str)

        print(f"[{current_time}] [ARM] 🔍 수신 데이터: {command_str!r}")

        arm_success = False
        arm_message = ""

        try:
            json.loads(command_str)

            await self.read_arm_place_single_node.write_value(command_str)

            print(f"[{current_time}] [ARM] ✅ 노드 쓰기 성공. ID: {self.read_arm_place_single_node.nodeid.Identifier}")
            print(f"[{current_time}] [OPCUA][TASK] 3초 후 노드 자동 초기화 태스크 생성.")
            asyncio.create_task(self._reset_variable_after_delay(self.read_arm_place_single_node))

            arm_success = True
            arm_message = f"Command '{command_str}' received and routed. Reset scheduled."
            print(f"[ARM] 🚀 처리 완료: {arm_message}")

        except json.JSONDecodeError as e:
            arm_success = False
            arm_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [ARM][ERROR] ❌ JSON 디코딩 오류: {arm_message}", file=sys.stderr)
        except AttributeError:
            arm_success = False
            arm_message = f"Error: ARM 노드 접근 불가 (self.read_arm_place_single_node 정의 확인 필요)."
            print(f"[{current_time}] [ARM][ERROR] ❌ 노드 접근 오류: {arm_message}", file=sys.stderr)
        except Exception as e:
            arm_success = False
            arm_message = f"ARM 통신 또는 처리 실패. Details: {e}"
            print(f"[{current_time}] [ARM][ERROR] ❌ 일반 오류: {arm_message}", file=sys.stderr)

        return [
            ua.Variant(arm_success, ua.VariantType.Boolean),
            ua.Variant(arm_message, ua.VariantType.String)
        ]
        
    # -----------------------------------------------------
    # ARM_004 (ARM -> WEB)
    # -----------------------------------------------------
    async def call_arm_place_completed(self, parent_node, json_arm_place_completed_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- [METHOD INVOKE] ARM_004: write_arm_place_completed ---")
        print(f"[{current_time}] [ARM] ➡️ 호출 수신: 전체 배치 완료 신호")

        if isinstance(json_arm_place_completed_data_str, ua.Variant):
            command_str = json_arm_place_completed_data_str.Value
        else:
            command_str = json_arm_place_completed_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore") 

        if not isinstance(command_str, str):
            command_str = str(command_str)

        print(f"[{current_time}] [ARM] 🔍 수신 데이터: {command_str!r}")

        arm_success = False
        arm_message = ""
        
        try:
            json.loads(command_str)

            await self.read_arm_place_completed_node.write_value(command_str)

            print(f"[{current_time}] [ARM] ✅ 노드 쓰기 성공. ID: {self.read_arm_place_completed_node.nodeid.Identifier}")
            print(f"[{current_time}] [OPCUA][TASK] 3초 후 노드 자동 초기화 태스크 생성.")
            asyncio.create_task(self._reset_variable_after_delay(self.read_arm_place_completed_node))

            arm_success = True
            arm_message = f"Command '{command_str}' received and routed. Reset scheduled."
            print(f"[ARM] 🚀 처리 완료: {arm_message}")

        except json.JSONDecodeError as e:
            arm_success = False
            arm_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [ARM][ERROR] ❌ JSON 디코딩 오류: {arm_message}", file=sys.stderr)
        except Exception as e:
            arm_success = False
            arm_message = f"ARM 통신 또는 처리 실패. Details: {e}"
            print(f"[{current_time}] [ARM][ERROR] ❌ 일반 오류: {arm_message}", file=sys.stderr)

        return [
            ua.Variant(arm_success, ua.VariantType.Boolean),
            ua.Variant(arm_message, ua.VariantType.String)
        ]

    # -----------------------------------------------------
    # IMG_001 (ARM -> WEB)
    # -----------------------------------------------------
    async def call_send_arm_img(parent, image_bytes_variant):
        global image_data_var
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        result_code = ua.Variant(0, ua.VariantType.Int32)
        result_message = ua.Variant("Success", ua.VariantType.String)

        print(f"\n--- [METHOD INVOKE] IMG_001: write_send_arm_img (Image ByteString) ---")
        print(f"[{current_time}] [IMG] ➡️ 호출 수신: 이미지 바이트 데이터")

        try:            
            if not isinstance(image_bytes_variant, ua.Variant) or image_bytes_variant.VariantType != ua.VariantType.ByteString:
                result_code = ua.Variant(2, ua.VariantType.Int32)
                result_message = ua.Variant("Error: Input must be ByteString Variant.", ua.VariantType.String)
                raise TypeError("Input must be ByteString Variant.")

            img_bytes = image_bytes_variant.Value
            
            if image_data_var is None:
                result_code = ua.Variant(3, ua.VariantType.Int32)
                result_message = ua.Variant("Server Variable Not Initialized", ua.VariantType.String)
                raise Exception("Server image_data_var Not Initialized")

            if not img_bytes:
                result_code = ua.Variant(4, ua.VariantType.Int32)
                result_message = ua.Variant("Empty Image Data (ByteString)", ua.VariantType.String)
                raise Exception("Empty Image Data")

            await image_data_var.write_value(img_bytes)
            
            print(f"[{current_time}] [IMG] ✅ 이미지 데이터 노드 쓰기 성공. 크기: {len(img_bytes)} bytes")
            result_message = ua.Variant("JPG data successfully written to OPC UA ByteString Variable", ua.VariantType.String)

        except Exception as e:
            print(f"[{current_time}] [IMG][ERROR] ❌ 처리 중 오류 발생: {e}", file=sys.stderr)
            if result_code.Value == 0:
                 result_code = ua.Variant(5, ua.VariantType.Int32)
            if not isinstance(result_message.Value, str) or 'Error' not in result_message.Value:
                result_message = ua.Variant(f"Unknown Error: {e}", ua.VariantType.String)
            
        return [result_code, result_message]

# -----------------------------------------------------
# Helper 함수: Method Arguments 정의 (가독성 유지를 위해 변경 없음)
# -----------------------------------------------------
def define_amr_001_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "json_command_str"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.String)
    input_arg.ValueRank = -1
    input_arg.Description = ua.LocalizedText("AMR 이동 명령을 담은 JSON 문자열 (예: {'move_command': 'go_home'})")
    
    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.ValueRank = -1
    output_arg_1.Description = ua.LocalizedText("처리 결과 코드 (0: 성공, 1: 오류)")
    
    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.ValueRank = -1
    output_arg_2.Description = ua.LocalizedText("처리 상세 메시지")
    
    return [input_arg], [output_arg_1, output_arg_2]

def define_amr_002_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "json_object_info_str"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.String)
    input_arg.ValueRank = -1
    input_arg.Description = ua.LocalizedText("오브젝트 정보 리스트를 포함하는 JSON 문자열 (e.g., {'object_info': ['item1', 'item2']})")
    
    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.ValueRank = -1
    output_arg_1.Description = ua.LocalizedText("처리 결과 코드 (0: 성공, 1: 오류)")
    
    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.ValueRank = -1
    output_arg_2.Description = ua.LocalizedText("처리 상세 메시지")
    
    return [input_arg], [output_arg_1, output_arg_2]

def define_amr_003_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "json_mission_state_str"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.String)
    input_arg.ValueRank = -1
    input_arg.Description = ua.LocalizedText("AMR 임무 상태 정보 JSON 문자열 (e.g., {'equipment_id': 'AMR_1', 'status': 'DONE'})")
    
    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.ValueRank = -1
    output_arg_1.Description = ua.LocalizedText("처리 결과 코드 (0: 성공, 1: 오류)")
    
    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.ValueRank = -1
    output_arg_2.Description = ua.LocalizedText("처리 상세 메시지")
    
    return [input_arg], [output_arg_1, output_arg_2]

def define_plc_001_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "conveyorSensor_check"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.Boolean)
    input_arg.ValueRank = -1
    input_arg.Description = ua.LocalizedText("PLC 센서 감지 신호 (True/False)")

    output_arg_1 = ua.Argument()
    output_arg_1.Name = "Success"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Boolean)
    output_arg_1.ValueRank = -1
    output_arg_1.Description = ua.LocalizedText("Method 호출 성공 여부")
    
    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.ValueRank = -1
    output_arg_2.Description = ua.LocalizedText("처리 상세 메시지")

    return [input_arg], [output_arg_1, output_arg_2]

def define_plc_002_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "json_anomaly_str"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.String)
    input_arg.ValueRank = -1
    input_arg.Description = ua.LocalizedText("이상 유무 판별 결과를 담은 JSON 문자열 (예: {'Anomaly': true})")

    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.ValueRank = -1
    output_arg_1.Description = ua.LocalizedText("처리 결과 코드 (0: 성공, 1: 오류)")
    
    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.ValueRank = -1
    output_arg_2.Description = ua.LocalizedText("처리 상세 메시지")

    return [input_arg], [output_arg_1, output_arg_2]

def define_plc_003_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "robotArmSensor_check"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.Boolean)
    input_arg.ValueRank = -1
    input_arg.Description = ua.LocalizedText("PLC 로봇 팔 센서 감지 신호 (True/False)")

    output_arg_1 = ua.Argument()
    output_arg_1.Name = "Success"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Boolean)
    output_arg_1.ValueRank = -1
    output_arg_1.Description = ua.LocalizedText("Method 호출 성공 여부")
    
    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.ValueRank = -1
    output_arg_2.Description = ua.LocalizedText("처리 상세 메시지")

    return [input_arg], [output_arg_1, output_arg_2]

def define_plc_004_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "json_state_str"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.String)
    input_arg.ValueRank = -1
    input_arg.Description = ua.LocalizedText("로봇 팔 동작 완료 명령을 담은 JSON 문자열 (e.g., {'state': 'CYCLE_COMPLETE'})")

    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.ValueRank = -1
    output_arg_1.Description = ua.LocalizedText("처리 결과 코드 (0: 성공, 1: 오류)")
    
    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.ValueRank = -1
    output_arg_2.Description = ua.LocalizedText("처리 상세 메시지")

    return [input_arg], [output_arg_1, output_arg_2]

def define_arm_001_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "json_img_data_str"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.String)
    input_arg.Description = ua.LocalizedText("Base64 이미지 포함 JSON 문자열")

    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.Description = ua.LocalizedText("처리 결과 코드 (0: 성공, 1: 오류)")

    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.Description = ua.LocalizedText("처리 상세 메시지")
    
    return [input_arg], [output_arg_1, output_arg_2]

def define_arm_002_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "json_img_data_str"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.String)
    input_arg.Description = ua.LocalizedText("Base64 인코딩된 이미지 데이터를 포함하는 JSON 문자열")

    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.Description = ua.LocalizedText("HTTP POST 결과 코드 (0: 성공, 1: 오류)")

    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.Description = ua.LocalizedText("HTTP POST 상세 메시지")
    
    return [input_arg], [output_arg_1, output_arg_2]

def define_arm_003_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "json_img_data_str"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.String)
    input_arg.Description = ua.LocalizedText("Base64 인코딩된 이미지 데이터를 포함하는 JSON 문자열")

    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.Description = ua.LocalizedText("HTTP POST 결과 코드 (0: 성공, 1: 오류)")

    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.Description = ua.LocalizedText("HTTP POST 상세 메시지")
    
    return [input_arg], [output_arg_1, output_arg_2]

def define_arm_004_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "json_img_data_str"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.String)
    input_arg.Description = ua.LocalizedText("Base64 인코딩된 이미지 데이터를 포함하는 JSON 문자열")

    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.Description = ua.LocalizedText("HTTP POST 결과 코드 (0: 성공, 1: 오류)")

    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.Description = ua.LocalizedText("HTTP POST 상세 메시지")
    
    return [input_arg], [output_arg_1, output_arg_2]

def define_img_001_arguments():
    input_arg = ua.Argument()
    input_arg.Name = "image_bytes"
    input_arg.DataType = ua.NodeId(ua.ObjectIds.ByteString)
    input_arg.Description = ua.LocalizedText("JPG 이미지 바이트 배열")

    output_arg_1 = ua.Argument()
    output_arg_1.Name = "ResultCode"
    output_arg_1.DataType = ua.NodeId(ua.ObjectIds.Int32)
    output_arg_1.Description = ua.LocalizedText("처리 결과 코드 (0: 성공, 1: 오류)")

    output_arg_2 = ua.Argument()
    output_arg_2.Name = "ResultMessage"
    output_arg_2.DataType = ua.NodeId(ua.ObjectIds.String)
    output_arg_2.Description = ua.LocalizedText("처리 상세 메시지")
    
    return [input_arg], [output_arg_1, output_arg_2]

async def main():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] [MAIN] 🚀 main() 함수 실행 시작")

    server = Server()
    server_ip = "opc.tcp://172.30.1.61:4840/freeopcua/server/"
    
    print(f"[{current_time}] [OPCUA] 서버 초기화 진행...")
    await server.init()
    
    server.set_endpoint(server_ip)
    server.set_server_name("SynchroBots_OPCUA Server")

    uri = "http://examples.freeopcua.github.io"
    idx = await server.register_namespace(uri)
    print(f"[{current_time}] [OPCUA] ✅ 네임스페이스 등록 완료: idx={idx}")
    
    methods = ServerMethods(server, idx)
    synchrobots_objects = await methods.init_nodes()
    print(f"[{current_time}] [OPCUA] ✅ Variable 및 Object 노드 구조 생성 완료")

    await synchrobots_objects["AMR"].add_method(
        ua.NodeId("write_amr_go_move", idx, node_id_type),
        "write_amr_go_move",
        methods.call_amr_go_move)
    
    await synchrobots_objects["AMR"].add_method(
        ua.NodeId("write_amr_go_positions", idx, node_id_type), 
        "write_amr_go_positions",
        methods.call_amr_go_position)
    
    await synchrobots_objects["AMR"].add_method(
        ua.NodeId("write_amr_mission_state", idx, node_id_type), 
        "write_amr_mission_state",
        methods.call_amr_mission_state)
    
    await synchrobots_objects["PLC"].add_method(
        ua.NodeId("write_conveyor_sensor_check", idx, node_id_type), 
        "write_conveyor_sensor_check", 
        methods.call_conveyor_sensor_check)
    
    await synchrobots_objects["PLC"].add_method(
        ua.NodeId("write_ok_ng_value", idx, node_id_type), 
        "write_ok_ng_value",
        methods.call_ok_ng_value)
    
    await synchrobots_objects["PLC"].add_method(
        ua.NodeId("write_robotarm_sensor_check", idx, node_id_type), 
        "write_robotarm_sensor_check", 
        methods.call_robotarm_sensor_check)
    
    await synchrobots_objects["PLC"].add_method(
        ua.NodeId("write_ready_state", idx, node_id_type), 
        "write_ready_state",
        methods.call_ready_state)
    
    await synchrobots_objects["ARM"].add_method(
        ua.NodeId("write_send_arm_json", idx, node_id_type), 
        "write_send_arm_json", 
        methods.call_send_arm_json)
    
    await synchrobots_objects["ARM"].add_method(
        ua.NodeId("write_arm_go_move", idx, node_id_type), 
        "write_arm_go_move", 
        methods.call_arm_go_move)
    
    await synchrobots_objects["ARM"].add_method(
        ua.NodeId("write_arm_place_single", idx, node_id_type),
        "write_arm_place_single", 
        methods.call_arm_place_single)
    
    await synchrobots_objects["ARM"].add_method(
        ua.NodeId("write_arm_place_completed", idx, node_id_type),
        "write_arm_place_completed", 
        methods.call_arm_place_completed)
    
    await synchrobots_objects["IMG"].add_method(
        ua.NodeId("write_send_arm_img", idx, node_id_type),
        "write_send_arm_img", 
        methods.call_send_arm_img)

    try:
        await server.start()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [OPCUA] 🟢 서버 실행 성공! 포트 4840 오픈.")
        
        last_heartbeat = time.time()
        
        while True:
            await asyncio.sleep(1)
            
            if time.time() - last_heartbeat > 10:
                raise Exception("서버 응답 지연 감지 (Watchdog Timeout)")
            
            last_heartbeat = time.time()

    except asyncio.CancelledError:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [SYSTEM] 비동기 작업 취소 감지 (CancelledError).")
        raise 
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [OPCUA][ERROR] 서버 장애 발생: {e}")
        raise e
    finally:
        await server.stop()

if __name__ == "__main__":
    RESTART_DELAY_SECONDS = 3
    if os.name == 'nt': os.system('color')

    while True:
        try:
            print(f"\n\033[92m{'-'*50}\n[SYSTEM] 서버 엔진 가동 시도...\n{'-'*50}\033[0m")
            asyncio.run(main())
            
        except KeyboardInterrupt:
            print(f"\n[SYSTEM] 사용자에 의해 종료되었습니다.")
            break
            
        except (Exception, asyncio.CancelledError) as e:
            curr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print("\n" + "=" * 70)
            print(f"\033[41m\033[37m[{curr}] [CRITICAL] 서버 다운/지연 감지! 에러: {e} \033[0m")
            print(f"\033[93m[{curr}] [SYSTEM] {RESTART_DELAY_SECONDS}초 대기 후 자동으로 서버를 재시작합니다...\033[0m")
            print("=" * 70 + "\n")
            
            logger.error(f"Server crashed or timed out: {e}. Restarting in {RESTART_DELAY_SECONDS}s.")
            time.sleep(RESTART_DELAY_SECONDS)