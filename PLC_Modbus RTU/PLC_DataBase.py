# DB_INSERTER.py
import pymysql
import time
from typing import List, Tuple, Any

# --- 데이터베이스 연결 정보 설정 (반드시 수정하세요) ---
DB_HOST = '172.30.1.29'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'SynchroBots'
DB_PORT = 3306

def insert_log_sync(equipment_id: str, source: str, description: str) -> bool:
    """
    동기(Blocking) 방식으로 데이터베이스에 로그를 삽입하는 함수.
    이 함수는 asyncio.to_thread로 감싸져 메인 루프를 블록하지 않도록 호출됩니다.
    """
    conn = None
    try:
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # 1. DB 연결
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            port=DB_PORT,
            charset='utf8'
        )
        cursor = conn.cursor()

        # 2. INSERT 쿼리 (current_timestamp(6) 사용)
        sql = """
            INSERT INTO synchrobots.mission_plc_logs
            (
                equipment_id,
                source,
                description,
                created_at
            )
            VALUES (%s, %s, %s, current_timestamp(6))
        """
        
        # 3. 쿼리에 전달할 값
        data_to_insert = (equipment_id, "PLC", description)

        # 4. 쿼리 실행
        cursor.execute(sql, data_to_insert)

        # 5. 변경사항 커밋
        conn.commit()
        # print(f"[{current_time}] [DB LOG] ✅ 성공 - EQ: {equipment_id}, Desc: {description}\n")
        return True

    except pymysql.err.MySQLError as e:
        print(f"[{current_time}] [DB LOG] ❌ MySQL 오류 발생: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        print(f"[{current_time}] [DB LOG] ❌ 예기치 않은 오류 발생: {e}")
        if conn:
            conn.rollback()
        return False
            
    finally:
        # 6. DB 연결 종료
        if conn:
            conn.close()

def select_data_sync(table_name: str, columns: List[str], condition: str = "1=1") -> Tuple[bool, List[Tuple[Any, ...]]]:
    """
    동기(Blocking) 방식으로 데이터베이스에서 데이터를 조회하는 함수.
    
    Args:
        table_name: 조회할 테이블 이름. (예: 'plc_control_state')
        columns: 조회할 컬럼 이름 리스트. (예: ['run_mode', 'frequency'])
        condition: WHERE 절에 들어갈 조건 문자열. (예: "equipment_id = 'EQ_01'")

    Returns:
        (성공 여부, 조회된 레코드 리스트). 레코드는 튜플의 리스트 형태입니다.
    """
    conn = None
    results = []
    success = False
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")

    try:
        # 1. DB 연결
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            port=DB_PORT,
            charset='utf8'
        )
        cursor = conn.cursor()

        # 2. SELECT 쿼리 생성
        columns_str = ", ".join(columns)
        # DB_NAME을 붙이는 것은 이미 connect 시점에 정의했으므로, 테이블 이름만 사용합니다.
        sql = f"""
            SELECT {columns_str}
            FROM {table_name}
            WHERE {condition}
        """
        
        # 3. 쿼리 실행
        cursor.execute(sql)

        # 4. 결과 가져오기
        results = cursor.fetchall()
        

        
        success = True
        # print(f"[{current_time}] [DB SELECT] ✅ 성공 - Table: {table_name}, {len(results)}개 레코드 조회. 조건: {condition}")
        
    except pymysql.err.MySQLError as e:
        print(f"[{current_time}] [DB SELECT] ❌ MySQL 오류 발생: {e}")
    except Exception as e:
        print(f"[{current_time}] [DB SELECT] ❌ 예기치 않은 오류 발생: {e}")
            
    finally:
        # 5. DB 연결 종료
        if conn:
            conn.close()
            
    return success, results



if __name__ == "__main__":
    # ... (기존 insert 테스트는 그대로 유지) ...

    # plc_control_state SELECT 테스트 실행 예시
    print("\n[plc_control_state] SELECT 테스트 실행...")
    
    # 1. 특정 장비 ID ('EQ_A001' 가정)의 현재 제어 상태를 조회
    # (실제 DB에 존재하는 equipment_id로 변경해야 합니다)
    
    # 조회하고 싶은 컬럼 리스트
    target_columns = ['run_mode', 'direction', 'frequency', 'acceleration', 'deceleration']
    
    select_success, control_state = select_data_sync(
        table_name='plc_control_state',
        columns=target_columns,
        condition="equipment_id = 'CONVEYOR01'" 
    )
    
    print(f"제어 상태 조회 테스트 결과: {'성공' if select_success else '실패'}")
    if select_success and control_state:
        # control_state는 단일 행을 가정하므로 [0]을 사용하여 첫 번째 튜플을 가져옵니다.
        state_data = control_state[0] 
        print(f"조회된 제어 상태:")
        print(f"  - Run Mode: {state_data[0]}") # run_mode
        print(f"  - Direction: {state_data[1]}") # direction
        print(f"  - Frequency: {state_data[2]}") # frequency
        # 패널 제어 로직에 이 데이터를 활용할 수 있습니다.
        
    elif select_success:
        print("조회 조건에 맞는 장비 제어 상태가 없습니다.")