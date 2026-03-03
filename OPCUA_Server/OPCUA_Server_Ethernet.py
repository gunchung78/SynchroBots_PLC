import asyncio
import json
from asyncua import Server, ua
from pymodbus.server import StartTcpServer
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext, ModbusSequentialDataBlock
import threading
from datetime import datetime
import numpy as np
import base64
import cv2
import logging

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='opcua_server.log',
                    filemode='a')
logger = logging.getLogger('OPCUA_SERVER')

# --- 전역 설정 ---
node_id_type = ua.NodeIdType.String

# --- Modbus TCP 설정 ---
MODBUS_REGISTERS = {
    80 : 0
}
store = ModbusSlaveContext(
    hr=ModbusSequentialDataBlock(0, [0] * 100)
)
modbus_context = ModbusServerContext(slaves=store, single=True)

image_data_var = None

# ------------------------------------------------------------------------------------- #

class ServerMethods:
    def __init__(self, server_instance, idx):
        self.server = server_instance
        self.idx = idx
        self.objects_node = self.server.nodes.objects
        self.read_amr_go_move_node = None                   # AMR_001 결과 반영 노드
        self.read_amr_go_positions_node = None              # AMR_002 결과 반영 노드
        self.read_amr_mission_state_node = None             # AMR_003 결과 반영 노드

        self.read_converyor_sensor_check_node = None        # PLC_001 결과 반영 노드
        self.read_ok_ng_value_node = None                   # PLC_002 결과 반영 노드
        self.read_robotarm_sensor_check_node = None         # PLC_003 결과 반영 노드
        self.read_ready_state_node = None                   # PLC_004 결과 반영 노드

        self.read_send_arm_json_node = None                 # ARM_01 결과 반영 노드
        self.read_arm_go_move_node = None                   # ARM_02 결과 반영 노드
        self.read_arm_place_single_node = None              # ARM_03 결과 반영 노드
        self.read_arm_place_completed_node = None           # ARM_04 결과 반영 노드

        self.read_send_arm_img_node = None                  # IMG_001 결과 반양 노드

    async def _reset_variable_after_delay(self, variable_node, delay=3, reset_value="Ready"):
        await asyncio.sleep(delay)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] [OPCUA][SERVER] Resetting node: {variable_node.nodeid.Identifier}")
        await variable_node.write_value(reset_value)
        print(f"[{current_time}] [OPCUA][SERVER] Node {variable_node.nodeid.Identifier} reset to '{reset_value}' completed.")

    async def init_nodes(self):

        synchrobots_AMR = await self.objects_node.add_object(self.idx, "AMR")
        synchrobots_PLC = await self.objects_node.add_object(self.idx, "PLC")
        synchrobots_ARM = await self.objects_node.add_object(self.idx, "ARM")
        synchrobots_IMG = await self.objects_node.add_object(self.idx, "IMG")

        # ----------------------------------------------------------------------
        # PLC 그룹 (고정 ID 사용)
        # ----------------------------------------------------------------------

        # --- [AMR_001] read_amr_go_move ---
        self.read_amr_go_move_node = await synchrobots_AMR.add_variable(
            ua.NodeId("read_amr_go_move", self.idx, node_id_type), 
            "read_amr_go_move", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))

        # --- [AMR_002] read_amr_go_positions ---
        self.read_amr_go_positions_node = await synchrobots_AMR.add_variable(
            ua.NodeId("read_amr_go_positions", self.idx, node_id_type),
            "read_amr_go_positions", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))
        
        # --- [AMR_003] read_amr_mission_state ---
        self.read_amr_mission_state_node = await synchrobots_AMR.add_variable(
            ua.NodeId("read_amr_mission_state", self.idx, node_id_type),
            "read_amr_mission_state", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))

        # ----------------------------------------------------------------------
        # PLC 그룹 (고정 ID 사용)
        # ----------------------------------------------------------------------
        
        # --- [PLC_001] read_conveyor_sensor_check ---
        self.read_converyor_sensor_check_node = await synchrobots_PLC.add_variable(
            ua.NodeId("read_conveyor_sensor_check", self.idx, node_id_type),
            "read_conveyor_sensor_check", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))

        # --- [PLC_002] read_ok_ng_value ---
        global MODBUS_REGISTERS
        self.read_ok_ng_value_node = await synchrobots_PLC.add_variable(
            ua.NodeId("read_ok_ng_value", self.idx, node_id_type),
            "read_ok_ng_value", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))

        # --- [PLC_003] read_robotarm_sensor_check ---
        self.read_robotarm_sensor_check_node = await synchrobots_PLC.add_variable(
            ua.NodeId("read_robotarm_sensor_check", self.idx, node_id_type),
            "read_robotarm_sensor_check", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))

        # --- [PLC_004] read_ready_state ---
        self.read_ready_state_node = await synchrobots_PLC.add_variable(
            ua.NodeId("read_ready_state", self.idx, node_id_type),
            "read_ready_state", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))

        # ----------------------------------------------------------------------
        # ARM 그룹 (고정 ID 사용)
        # ----------------------------------------------------------------------
        
        # --- [ARM_001] read_send_arm_json --- 
        self.read_send_arm_json_node = await synchrobots_ARM.add_variable(
            ua.NodeId("read_send_arm_json", self.idx, node_id_type),
            "read_send_arm_json", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))
        
        # --- [ARM_002] read_arm_go_move ---
        self.read_arm_go_move_node = await synchrobots_ARM.add_variable(
            ua.NodeId("read_arm_go_move", self.idx, node_id_type),
            "read_arm_go_move", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))

        # --- [ARM_003] read_arm_place_single ---
        self.read_arm_place_single_node = await synchrobots_ARM.add_variable(
            ua.NodeId("read_arm_place_single", self.idx, node_id_type),
            "read_arm_place_single", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))

        # --- [ARM_004] read_arm_place_completed ---
        self.read_arm_place_completed_node = await synchrobots_ARM.add_variable(
            ua.NodeId("read_arm_place_completed", self.idx, node_id_type),
            "read_arm_place_completed", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.String))

        # ----------------------------------------------------------------------
        # IMG 그룹 (고정 ID 사용)
        # ----------------------------------------------------------------------
        
        self.read_send_arm_img_node = await synchrobots_IMG.add_variable(
            ua.NodeId("read_send_arm_img", self.idx, node_id_type),
            "read_send_arm_img", 
            "Ready", 
            datatype=ua.NodeId(ua.ObjectIds.ByteString))

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
        print(f"[{current_time}] [OPCUA][SERVER] amr_go_move 호출")

        if isinstance(json_amr_go_move_data_str, ua.Variant):
            command_str = json_amr_go_move_data_str.Value
        else:
            command_str = json_amr_go_move_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore")

        if not isinstance(command_str, str):
            command_str = str(command_str)

        print(f"[{current_time}] [OPCUA][SERVER] json_command_str = {command_str!r}")

        amr_success = False
        amr_message = ""
        
        try:
            json.loads(command_str)

            await self.read_amr_go_move_node.write_value(command_str)

            print(f"[{current_time}] [OPCUA][SERVER] Command written. Creating 3s reset task for AMR_001...")
            asyncio.create_task(self._reset_variable_after_delay(self.read_amr_go_move_node))

            amr_success = True
            amr_message = f"AMR Command '{command_str}' received and stored. Reset scheduled."
            print(f"[OPCUA][SERVER] Command successfully routed to AMR: {command_str!r}")

        except json.JSONDecodeError as e:
            amr_success = False
            amr_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[OPCUA][SERVER][ERROR] JSON Decode Error: {amr_message}")
        except Exception as e:
            amr_success = False
            amr_message = f"AMR communication or processing failed. Details: {e}"
            print(f"[OPCUA][SERVER][ERROR] General Error: {amr_message}")

        return [
            ua.Variant(amr_success, ua.VariantType.Boolean),
            ua.Variant(amr_message, ua.VariantType.String)
        ]
    
    # -----------------------------------------------------
    # AMR_002 인터페이스 로직 (Web -> AMR)
    # -----------------------------------------------------
    async def call_amr_go_position(self, parent_node, json_amr_go_position_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] [OPCUA][SERVER] call_amr_go_position 호출")

        if isinstance(json_amr_go_position_data_str, ua.Variant):
            position_str = json_amr_go_position_data_str.Value
        else:
            position_str = json_amr_go_position_data_str

        if isinstance(position_str, bytes):
            position_str = position_str.decode("utf-8", errors="ignore")

        if not isinstance(position_str, str):
            position_str = str(position_str)

        print(f"[{current_time}] [OPCUA][SERVER] json_position_str = {position_str!r}")
        
        amr_success = False
        amr_message = ""
        
        try:
            json.loads(position_str)

            await self.read_amr_go_positions_node.write_value(position_str)

            print(f"[{current_time}] [OPCUA][SERVER] Command written. Creating 3s reset task for AMR_002...")
            asyncio.create_task(self._reset_variable_after_delay(self.read_amr_go_positions_node))

            amr_success = True
            amr_message = f"AMR Command '{position_str}' received and stored. Reset scheduled."
            print(f"[{current_time}] [OPCUA][Server] Command successfully routed to AMR: {position_str!r}")

        except json.JSONDecodeError as e:
            amr_success = False
            amr_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[OPCUA][SERVER][ERROR] JSON Decode Error: {amr_message}")
            
        except ValueError as e:
            amr_success = False
            amr_message = f"Error: JSON data validation failed. Details: {e}"
            print(f"[OPCUA][SERVER][ERROR] Validation Error: {amr_message}")
            
        except Exception as e:
            amr_success = False
            amr_message = f"AMR communication or processing failed. Details: {e}"
            print(f"[OPCUA][SERVER][ERROR] General Error: {amr_message}")
 
        return [
            ua.Variant(amr_success, ua.VariantType.Boolean),
            ua.Variant(amr_message, ua.VariantType.String)
        ]

    # -----------------------------------------------------
    # AMR_003 인터페이스 로직 (AMR -> WEB)
    # -----------------------------------------------------
    async def call_amr_mission_state(self, parent_node, json_amr_mission_state_query_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] [OPCUA][SERVER] call_amr_mission_state called")

        if isinstance(json_amr_mission_state_query_str, ua.Variant):
            query_str = json_amr_mission_state_query_str.Value
        else:
            query_str = json_amr_mission_state_query_str

        if isinstance(query_str, bytes):
            query_str = query_str.decode("utf-8", errors="ignore")

        if not isinstance(query_str, str):
            query_str = str(query_str)

        print(f"[{current_time}] [OPCUA][SERVER] json_query_str = {query_str!r}")

        amr_success = False
        amr_message = ""
        
        try:
            json.loads(query_str)

            await self.read_amr_mission_state_node.write_value(query_str)

            print(f"[{current_time}] [OPCUA][SERVER] Command written. Creating 3s reset task for AMR_003...")
            asyncio.create_task(self._reset_variable_after_delay(self.read_amr_mission_state_node))

            amr_success = True
            amr_message = f"AMR Command '{query_str}' received and stored. Reset scheduled."
            print(f"[{current_time}] [OPCUA][SERVER] Command successfully routed to AMR: {query_str!r}")

        except json.JSONDecodeError as e:
            amr_success = False
            amr_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[OPCUA][SERVER][ERROR] JSON Decode Error: {amr_message}")
        except Exception as e:
            amr_success = False
            amr_message = f"AMR communication or processing failed. Details: {e}"
            print(f"[OPCUA][SERVER][ERROR] General Error: {amr_message}")

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

        print(f"[{current_time}] [OPCUA][SERVER] call_ok_ng_value called")

        if isinstance(json_ok_ng_value_data_str, ua.Variant):
            command_str = json_ok_ng_value_data_str.Value
        else:
            command_str = json_ok_ng_value_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore")

        if not isinstance(command_str, str):
            command_str = str(command_str)
        
        command_str = command_str.strip()
        
        print(f"[{current_time}] [OPCUA][SERVER] ok_ng_vlaue = {command_str!r}")
        
        result_code = 1
        result_message = ""
        modbus_value = 0

        try:
            anomaly_data = json.loads(command_str)
            
            status_message = f"JSON Received: {command_str}"

            if "Anomaly" in anomaly_data:
                anomaly_status = anomaly_data["Anomaly"]
                status_str_upper = str(anomaly_status).upper()
                
                if status_str_upper == 'NG':
                    modbus_value = 1 
                    status_message += " -> NG DETECTED. Modbus Value: 1 (Anomaly)"
                elif status_str_upper == 'OK':
                    modbus_value = 0 
                    status_message += " -> OK DETECTED. Modbus Value: 0 (Normal)"
                else:
                    raise ValueError(f"'Anomaly' key value must be 'OK' or 'NG', received: {anomaly_status}")
            else:
                status_message += " -> 'Anomaly' key not found. Modbus Value: 0 (Default)"
                modbus_value = 0

            slave_id = 0x03
            modbus_context[slave_id].setValues(3, modbus_register_address, [modbus_value])

            await self.read_ok_ng_value_node.set_value({status_message})

            print(f"[{current_time}] [OPCUA][SERVER] Command written. Creating 3s reset task for PLC_002...")
            asyncio.create_task(self._reset_variable_after_delay(self.read_ok_ng_value_node))

            result_message = True
            result_message = f"PLC Command '{status_message}' received and stored. Reset scheduled."
            print(f"[{current_time}] [OPCUA][SERVER] Command successfully routed to PLC: {status_message!r}")

        except json.JSONDecodeError:
            result_code = 1
            result_message = "Error: Input string is not a valid JSON."
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] JSON Decode Error: {result_message}")
            await self.read_ok_ng_value_node.set_value(f"JSON ERROR: {command_str}")
        except ValueError as e:
            result_code = 1
            result_message = f"Error: Validation failed. Details: {e}"
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] Validation Error: {result_message}")
            await self.read_ok_ng_value_node.set_value(f"VALIDATION ERROR: {command_str}")
        except Exception as e:
            result_code = 1
            result_message = f"Modbus communication or processing failed. Details: {e}"
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] General Error: {result_message}")
            await self.read_ok_ng_value_node.set_value(f"GENERAL ERROR: {command_str}")
        
        return [
            ua.Variant(result_code, ua.VariantType.Int32),
            ua.Variant(result_message, ua.VariantType.String)
        ]

    # -----------------------------------------------------
    # PLC_003 (PLC -> WEB)
    # -----------------------------------------------------
    async def call_robotarm_sensor_check(self, parent_node, json_robotarm_sensor_check_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        print(f"[{current_time}] [OPCUA][SERVER] call_ready_state called")

        try:
            state_data = json.loads(json_ready_state_data_str)
            state_command = state_data.get("state")
        except json.JSONDecodeError:
            msg = "Error: Invalid JSON format received."
            await self.read_ready_state_node.set_value(msg)
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] JSON Decode Error: {msg}")
            return (
                ua.Variant(1, ua.VariantType.Int32),
                ua.Variant(msg, ua.VariantType.String),
            )
        except Exception:
            msg = "Error: Missing 'state' key."
            await self.read_ready_state_node.set_value(msg)
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] Key Missing Error: {msg}")
            return (
                ua.Variant(1, ua.VariantType.Int32),
                ua.Variant(msg, ua.VariantType.String),
            )

        if state_command not in ["CYCLE_COMPLETE", "CONTINUE", "PAUSE"]:
            msg = f"Error: Invalid state command: {state_command}"
            await self.read_ready_state_node.set_value(msg)
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] Invalid Command: {msg}")
            return (
                ua.Variant(1, ua.VariantType.Int32),
                ua.Variant(msg, ua.VariantType.String),
            )

        if state_command == "CYCLE_COMPLETE":
            status_message = "ARM_CYCLE_COMPLETE. PLC: START CONVEYOR"
        else:
            status_message = f"Received Command: {state_command}"

        await self.read_ready_state_node.set_value(f"Processing Command: {status_message}")
        print(f"[{current_time}] [OPCUA][SERVER] Processing Command: {status_message}")

        await asyncio.sleep(0.3)
        await self.read_ready_state_node.set_value(status_message)

        msg = f"Success: State '{state_command}' relayed to PLC."
        print(f"[{current_time}] [OPCUA][SERVER] Success: {msg}")
        return (
            ua.Variant(0, ua.VariantType.Int32),
            ua.Variant(msg, ua.VariantType.String),
        )

    # -----------------------------------------------------
    # ARM_001 (ARM -> WEB)
    # -----------------------------------------------------
    async def call_send_arm_json(self, parent, json_arm_img_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        result_code = ua.Variant(0, ua.VariantType.Int32)
        result_message = ua.Variant("Success", ua.VariantType.String)
        content_to_write = ""

        try:
            print(f"\n[{current_time}] [OPCUA][SERVER] call_send_arm_json calld") 
            
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

            print(f"[{current_time}] [OPCUA][SERVER] 수신된 JSON 데이터: {content_to_write[:100]}...")

            data = json.loads(content_to_write)
            
            base64_img_str = data.get("img")
            if base64_img_str:
                try:
                    img_bytes = base64.b64decode(base64_img_str)
                    np_arr = np.frombuffer(img_bytes, np.uint8)
                    decoded_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                    output_filename = "received_arm_json_image.jpg"
                    cv2.imwrite(output_filename, decoded_img)
                    print(f"[{current_time}] [OPCUA][SERVER] 서버에서 이미지 복원 및 저장 완료: {output_filename}")
                except Exception as e:
                    print(f"[{current_time}] [SERVER] :x: 이미지 복원 중 오류: {e}")

            if 'status' in data:
                print(f"[{current_time}] [OPCUA][SERVER] 미션 상태 보고 : {data['status']}")
                print("--- 미션 데이터 로깅 완료 ---")
            elif 'module_type' in data:
                print(f"[{current_time}] [SERVER] :eye: 비전 결과 보고 (Vision Result):")
                print(f" - Module Type: {data.get('module_type')}")
                print(f" - Confidence: {data.get('classification_confidence')}")
                print(f" - Pick Coord: {data.get('pick_coord')}")
                print("--- 비전 데이터 로깅 완료 ---")
            else:
                print(f"[{current_time}] [SERVER] :question: 알 수 없는 데이터 구조 수신 (JSON Keys: {list(data.keys())})")
            
            print(f"[{current_time}] [OPCUA][SERVER] Command written. Creating 3s reset task for ARM_002...")
            asyncio.create_task(self._reset_variable_after_delay(self.read_arm_go_move_node))
            
            result_code = ua.Variant(0, ua.VariantType.Int32)
            result_message = ua.Variant("Data processed and written to Variable", ua.VariantType.String)
            print(f"[OPCUA][SERVER] Command successfully routed to ARM: {content_to_write[:100]}")

        except json.JSONDecodeError:
            result_code = ua.Variant(2, ua.VariantType.Int32)
            result_message = ua.Variant("JSON Decode Error", ua.VariantType.String)
            print(f"[{current_time}] [SERVER] :x: JSON 디코딩 오류 발생. 수신된 데이터: {content_to_write[:100]}...")
        except Exception as e:
            result_code = ua.Variant(5, ua.VariantType.Int32)
            result_message = ua.Variant(f"Unknown Error: {e}", ua.VariantType.String)
            print(f"[{current_time}] [SERVER] :x: 알 수 없는 오류 발생: {e}")
            
        return [result_code, result_message]
    
    # -----------------------------------------------------
    # ARM_002 (WEB -> WEB)
    # -----------------------------------------------------
    async def call_arm_go_move(self, parent_node, json_arm_go_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] [OPCUA][SERVER] call_arm_go_move called")

        if isinstance(json_arm_go_data_str, ua.Variant):
            command_str = json_arm_go_data_str.Value
        else:
            command_str = json_arm_go_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore") 

        if not isinstance(command_str, str):
            command_str = str(command_str)

        print(f"[{current_time}] [OPCUA][SERVER] amr_go_move_command = {command_str!r}")

        amr_success = False
        amr_message = ""
        
        try:
            json.loads(command_str)

            await self.read_arm_go_move_node.write_value(command_str)

            print(f"[{current_time}] [OPCUA][SERVER] Command written. Creating 3s reset task for ARM_002...")
            asyncio.create_task(self._reset_variable_after_delay(self.read_arm_go_move_node))

            amr_success = True
            amr_message = f"AMR Command '{command_str}' received and stored. Reset scheduled."
            print(f"[{current_time}] [OPCUA][SERVER] Command successfully routed to AMR: {command_str!r}")

        except json.JSONDecodeError as e:
            amr_success = False
            amr_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] JSON Decode Error: {amr_message}")
        except Exception as e:
            amr_success = False
            amr_message = f"AMR communication or processing failed. Details: {e}"
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] General Error: {amr_message}")

        return [
            ua.Variant(amr_success, ua.VariantType.Boolean),
            ua.Variant(amr_message, ua.VariantType.String)
        ]
    
    # -----------------------------------------------------
    # ARM_003 (ARM -> WEB)
    # -----------------------------------------------------
    async def call_arm_place_single(self, parent_node, json_arm_place_single_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] [OPCUA][SERVER] call_arm_place_single called")

        if isinstance(json_arm_place_single_data_str, ua.Variant):
            command_str = json_arm_place_single_data_str.Value
        else:
            command_str = json_arm_place_single_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore")

        if not isinstance(command_str, str):
            command_str = str(command_str)

        print(f"[{current_time}] [OPCUA][SERVER] json_command_str = {command_str!r}")

        arm_success = False
        arm_message = ""

        try:
            json.loads(command_str)

            await self.read_arm_place_single_node.write_value(command_str)

            print(f"[{current_time}] [OPCUA][SERVER] Command written. Creating 3s reset task for ARM_003...")
            asyncio.create_task(self._reset_variable_after_delay(self.read_arm_place_single_node))

            arm_success = True
            arm_success = f"ARM Command '{command_str}' received and stored. Reset scheduled."
            print(f"[{current_time}] [OPCUA][SERVER] Command successfully routed to ARM: {command_str!r}")

        except json.JSONDecodeError as e:
            arm_success = False
            arm_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] JSON Decode Error: {arm_message}")
        except AttributeError:
            arm_success = False
            arm_message = f"Error: ARM node not found or accessible (Is 'self.read_arm_place_single_node' defined?)."
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] Node Access Error: {arm_message}")
        except Exception as e:
            arm_success = False
            arm_message = f"ARM communication or processing failed. Details: {e}"
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] General Error: {arm_message}")

        return [
            ua.Variant(arm_success, ua.VariantType.Boolean),
            ua.Variant(arm_message, ua.VariantType.String)
        ]
        
    # -----------------------------------------------------
    # ARM_004 (ARM -> WEB)
    # -----------------------------------------------------
    async def call_arm_place_completed(self, parent_node, json_arm_place_completed_data_str):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] [OPCUA][SERVER] call_arm_place_completed called")

        if isinstance(json_arm_place_completed_data_str, ua.Variant):
            command_str = json_arm_place_completed_data_str.Value
        else:
            command_str = json_arm_place_completed_data_str

        if isinstance(command_str, bytes):
            command_str = command_str.decode("utf-8", errors="ignore") 

        if not isinstance(command_str, str):
            command_str = str(command_str)

        print(f"[{current_time}] [OPCUA][SERVER] arm_place_completed_node = {command_str!r}")

        amr_success = False
        amr_message = ""
        
        try:
            json.loads(command_str)

            await self.read_arm_place_completed_node.write_value(command_str)

            print(f"[{current_time}] [OPCUA][SERVER] Command written. Creating 3s reset task for ARM_004...")
            asyncio.create_task(self._reset_variable_after_delay(self.read_arm_place_completed_node))

            amr_success = True
            amr_success = f"AMR Command '{command_str}' received and stored. Reset scheduled."
            print(f"[OPCUA][SERVER] Command successfully routed to ARM: {command_str!r}")

        except json.JSONDecodeError as e:
            amr_success = False
            amr_message = f"Error: Input string is not a valid JSON. Details: {e}"
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] JSON Decode Error: {amr_message}")
        except Exception as e:
            amr_success = False
            amr_message = f"AMR communication or processing failed. Details: {e}"
            print(f"[{current_time}] [OPCUA][SERVER][ERROR] General Error: {amr_message}")

        return [
            ua.Variant(amr_success, ua.VariantType.Boolean),
            ua.Variant(amr_message, ua.VariantType.String)
        ]

    # -----------------------------------------------------
    # IMG_001 (ARM -> WEB)
    # -----------------------------------------------------
    async def call_send_arm_img(parent, image_bytes_variant):
        global image_data_var
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        result_code = ua.Variant(0, ua.VariantType.Int32)
        result_message = ua.Variant("Success", ua.VariantType.String)

        try:            
            if not isinstance(image_bytes_variant, ua.Variant) or image_bytes_variant.VariantType != ua.VariantType.ByteString:
                result_code = ua.Variant(2, ua.VariantType.Int32)
                result_message = ua.Variant("Error: Input must be ByteString Variant.", ua.VariantType.String)
                return [result_code, result_message]

            img_bytes = image_bytes_variant.Value
            
            if image_data_var is None:
                result_code = ua.Variant(3, ua.VariantType.Int32)
                result_message = ua.Variant("Server Variable Not Initialized", ua.VariantType.String)
                return [result_code, result_message]

            if not img_bytes:
                result_code = ua.Variant(4, ua.VariantType.Int32)
                result_message = ua.Variant("Empty Image Data (ByteString)", ua.VariantType.String)
                return [result_code, result_message]

            await image_data_var.write_value(img_bytes)
            
            result_message = ua.Variant("JPG data successfully written to OPC UA ByteString Variable", ua.VariantType.String)

        except Exception as e:
            result_code = ua.Variant(5, ua.VariantType.Int32)
            result_message = ua.Variant(f"Unknown Error: {e}", ua.VariantType.String)
            
        return [result_code, result_message]

# -----------------------------------------------------
# Helper 함수: Method Arguments 정의
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


# --- Modbus TCP Server 시작 함수 ---
def start_modbus_server():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        StartTcpServer(context=modbus_context, host='192.168.1.2', port=502)
        print(f"[{current_time}] [MODBUS] Modbus TCP Server Started on 192.168.1.2:502")
    except Exception as e:
        print(f"[{current_time}] [MODBUS] Modbus TCP Server Failed to Start: {e}")

async def main():
    # -----------------------------------------------------
    # 1. OPC UA Server 설정
    # -----------------------------------------------------
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] [MAIN] main() start")

    server = Server()

    server_ip = "opc.tcp://172.30.1.61:0630/freeopcua/server/"
    print(f"[{current_time}] [OPCUA] init server...")
    await server.init()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] [OPCUA] server.init() OK")

    server.set_endpoint(server_ip)
    server.set_server_name("SynchroBots_OPCUA Server")

    uri = "http://examples.freeopcua.github.io"
    idx = await server.register_namespace(uri)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] [OPCUA] namespace registered: idx={idx}")
    
    methods = ServerMethods(server, idx)
    
    synchrobots_objects = await methods.init_nodes()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] [OPCUA] nodes & methods initialized")


    # -----------------------------------------------------
    # 2. AMR_001 (amr_go_move) 메소드 등록  
    # -----------------------------------------------------
    await synchrobots_objects["AMR"].add_method(
        ua.NodeId("write_amr_go_move", idx, node_id_type), 
        "write_amr_go_move",
        methods.call_amr_go_move,
        # (Input/Output Arguments는 methods.py에 정의되어 있음)
    )

    # -----------------------------------------------------
    # 3. AMR_002 (amr_go_positions) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["AMR"].add_method(
        ua.NodeId("write_amr_go_positions", idx, node_id_type), 
        "write_amr_go_positions",
        methods.call_amr_go_position,
    )

    # -----------------------------------------------------
    # 4. AMR_003 (amr_mission_state) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["AMR"].add_method(
        ua.NodeId("write_amr_mission_state", idx, node_id_type), 
        "write_amr_mission_state",
        methods.call_amr_mission_state,
    )

    # -----------------------------------------------------
    # 5. PLC_001 (set_conveyorSensor_check) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["PLC"].add_method(
        ua.NodeId("write_conveyor_sensor_check", idx, node_id_type), 
        "write_conveyor_sensor_check",
        methods.call_conveyor_sensor_check,
    )
    
    # -----------------------------------------------------
    # 6. PLC_002 (OK_NG_Value) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["PLC"].add_method(
        ua.NodeId("write_ok_ng_value", idx, node_id_type), 
        "write_ok_ng_value",
        methods.call_ok_ng_value,
    )

    # -----------------------------------------------------
    # 7. PLC_003 (set_robotArmSensor_check) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["PLC"].add_method(
        ua.NodeId("write_robotarm_sensor_check", idx, node_id_type), 
        "write_robotarm_sensor_check",
        methods.call_robotarm_sensor_check,
    )

    # -----------------------------------------------------
    # 8. PLC_004 (Ready_State) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["PLC"].add_method(
        ua.NodeId("write_ready_state", idx, node_id_type), 
        "write_ready_state",
        methods.call_ready_state,
    )
    
    # -----------------------------------------------------
    # 9. ARM_001 (arm_img) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["ARM"].add_method(
        ua.NodeId("write_send_arm_json", idx, node_id_type), 
        "write_send_arm_json",
        methods.call_send_arm_json,
    )
    
    # -----------------------------------------------------
    # 9. ARM_002 (arm_go_move) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["ARM"].add_method(
        ua.NodeId("write_arm_go_move", idx, node_id_type), 
        "write_arm_go_move",
        methods.call_arm_go_move,
    )

    # -----------------------------------------------------
    # 9. ARM_003 (arm_place_single) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["ARM"].add_method(
        ua.NodeId("write_arm_place_single", idx, node_id_type), 
        "write_arm_place_single",
        methods.call_arm_place_single,
    )

    # -----------------------------------------------------
    # 9. ARM_004 (arm_place_completed) 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["ARM"].add_method(
        ua.NodeId("write_arm_place_completed", idx, node_id_type), 
        "write_arm_place_completed",
        methods.call_arm_place_completed,
    )
    
    # -----------------------------------------------------
    # 10. IMG_001 () 메소드 등록
    # -----------------------------------------------------
    await synchrobots_objects["IMG"].add_method(
        ua.NodeId("write_send_arm_img", idx, node_id_type), 
        "write_send_arm_img",
        methods.call_send_arm_img,
    )

    # -----------------------------------------------------
    # 10. 서버 실행
    # -----------------------------------------------------
    async with server:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] [OPCUA] Server started at {server_ip}")
        await asyncio.get_running_loop().create_future() 


if __name__ == "__main__":
    try:
        threading.Thread(target=start_modbus_server, daemon=True).start()
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[MAIN] KeyboardInterrupt - shutting down...")
    except Exception as e:
        print(f"[MAIN] Unexpected error: {e}")