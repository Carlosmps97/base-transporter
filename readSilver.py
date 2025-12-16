# Databricks notebook source
import os
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field, asdict

# PySpark imports
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    IntegerType, LongType, DoubleType, BooleanType, MapType
)

# COMMAND ----------

# =============================================================================
# DATA CLASSES PARA LA ESTRUCTURA JERÁRQUICA
# =============================================================================

@dataclass
class ToolInfo:
    """Información de una herramienta (function call)"""
    tool_id: str = ""
    tool_name: str = ""
    tool_type: str = ""  # function, search, etc.
    tool_description: str = ""
    start_time: str = ""
    end_time: str = ""
    duration_ms: float = 0.0
    input_full: str = ""
    output_full: str = ""
    status: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RunStep:
    """Un paso dentro de un Run"""
    step_id: str = ""
    step_type: str = ""  # tool_calls, message_creation
    start_time: str = ""
    end_time: str = ""
    duration_ms: float = 0.0
    status: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    tools: List[ToolInfo] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Run:
    """Representa una ejecución del agente (un run)"""
    run_id: str = ""
    thread_id: str = ""
    agent_id: str = ""
    model: str = ""
    start_time: str = ""
    end_time: str = ""
    duration_ms: float = 0.0
    status: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    input_message: str = ""
    output_message: str = ""
    run_steps: List[RunStep] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Thread:
    """Representa un hilo de conversación"""
    thread_id: str = ""
    created_at: str = ""
    start_time: str = ""
    end_time: str = ""
    duration_ms: float = 0.0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_tokens: int = 0
    runs: List[Run] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class User:
    """Representa un usuario del sistema"""
    user_id: str = ""
    threads: List[Thread] = field(default_factory=list)
    total_interactions: int = 0
    total_tokens: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

# COMMAND ----------

# =============================================================================
# CLASE PRINCIPAL DEL ANALIZADOR
# =============================================================================

class TelemetryAnalyzer:
    """
    Analizador de telemetría de Azure AI Agents usando PySpark.
    
    Lee archivos CSV de Azure Log Analytics y construye una estructura
    jerárquica de User -> Thread -> Run -> RunStep -> Tool
    """
    
    def __init__(self, data_path: str = "."):
        """
        Inicializa el analizador con PySpark.
        
        Args:
            data_path: Ruta donde se encuentran los archivos CSV
        """
        self.data_path = data_path
        self.spark = self._create_spark_session()
        
        # DataFrames cargados (Spark DataFrames)
        self.df_dependencies: Optional[SparkDataFrame] = None
        self.df_requests: Optional[SparkDataFrame] = None
        self.df_traces: Optional[SparkDataFrame] = None
        
        # Estructura de datos procesados
        self.users: Dict[str, User] = {}
        self.threads: Dict[str, Thread] = {}
        self.runs: Dict[str, Run] = {}
    
    def _create_spark_session(self):
        """Crea y configura la sesión de Spark."""
        return (SparkSession.builder
                .appName("TelemetryAnalyzer")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.log.level", "ERROR")
                .getOrCreate())
    
    def _safe_parse_json(self, json_str: str) -> Dict[str, Any]:
        """Parsea JSON de forma segura, retornando dict vacío en caso de error."""
        if not json_str or json_str == "null":
            return {}
        try:
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def _extract_nested_json(self, properties: Dict, key: str) -> Dict[str, Any]:
        """Extrae JSON anidado de un campo de properties."""
        value = properties.get(key, "")
        if isinstance(value, str):
            return self._safe_parse_json(value)
        return value if isinstance(value, dict) else {}
    
    def load_csv_files(self) -> None:
        """Carga los tres archivos CSV en DataFrames de Spark."""
        print("=" * 60)
        print("CARGANDO ARCHIVOS CSV CON SPARK")
        print("=" * 60)
        
        # Opciones de lectura CSV para manejar comillas escapadas correctamente
        csv_options = {
            "header": "true",
            "inferSchema": "false",
            "multiLine": "true",
            "escape": '"',
            "quote": '"'
        }
        
        # Cargar app_dependencies.csv
        dependencies_path = os.path.join(self.data_path, "app_dependencies.csv")
        if os.path.exists(dependencies_path):
            self.df_dependencies = self.spark.read.options(**csv_options).csv(dependencies_path)
            print(f"✓ app_dependencies.csv: {self.df_dependencies.count()} registros")
        else:
            print(f"✗ No se encontró: {dependencies_path}")
        
        # Cargar app_requests.csv
        requests_path = os.path.join(self.data_path, "app_requests.csv")
        if os.path.exists(requests_path):
            self.df_requests = self.spark.read.options(**csv_options).csv(requests_path)
            print(f"✓ app_requests.csv: {self.df_requests.count()} registros")
        else:
            print(f"✗ No se encontró: {requests_path}")
        
        # Cargar app_traces.csv
        traces_path = os.path.join(self.data_path, "app_traces.csv")
        if os.path.exists(traces_path):
            self.df_traces = self.spark.read.options(**csv_options).csv(traces_path)
            print(f"✓ app_traces.csv: {self.df_traces.count()} registros")
        else:
            print(f"✗ No se encontró: {traces_path}")
        
        print()
    
    def _parse_properties_column(self, df: SparkDataFrame) -> List[Dict]:
        """
        Convierte el Spark DataFrame a lista de diccionarios con Properties parseado.
        """
        parsed_rows = []
        
        # Convertir Spark DataFrame a lista de filas
        rows = df.collect()
        columns = df.columns
        
        for row in rows:
            row_dict = {col: row[col] for col in columns}
            # Parsear el campo Properties si existe
            if "Properties" in row_dict and row_dict["Properties"]:
                row_dict["Properties_parsed"] = self._safe_parse_json(str(row_dict["Properties"]))
            else:
                row_dict["Properties_parsed"] = {}
            parsed_rows.append(row_dict)
        
        return parsed_rows
    
    def extract_users_and_threads(self) -> None:
        """Extrae usuarios y threads desde app_requests."""
        print("=" * 60)
        print("EXTRAYENDO USUARIOS Y THREADS")
        print("=" * 60)
        
        if self.df_requests is None:
            print("✗ No hay datos de requests")
            return
        
        # Filtrar solo las solicitudes de chat usando Spark
        chat_requests = self.df_requests.filter(
            F.col("Name").contains("/chat")
        )
        
        requests_data = self._parse_properties_column(chat_requests)
        
        for req in requests_data:
            props = req.get("Properties_parsed", {})
            
            user_id = props.get("user.id", "unknown")
            thread_id = props.get("azure.thread_id", "")
            run_id = props.get("azure.run_id", "")
            
            if not thread_id:
                continue
            
            # Crear usuario si no existe
            if user_id not in self.users:
                self.users[user_id] = User(user_id=user_id)
            
            # Crear thread si no existe
            if thread_id not in self.threads:
                thread = Thread(thread_id=thread_id)
                self.threads[thread_id] = thread
                self.users[user_id].threads.append(thread)
            
            # Crear run si no existe
            if run_id and run_id not in self.runs:
                run = Run(
                    run_id=run_id,
                    thread_id=thread_id,
                    start_time=req.get("TimeGenerated", ""),
                    duration_ms=float(req.get("DurationMs", 0))
                )
                self.runs[run_id] = run
                self.threads[thread_id].runs.append(run)
        
        print(f"✓ Usuarios encontrados: {len(self.users)}")
        print(f"✓ Threads encontrados: {len(self.threads)}")
        print(f"✓ Runs encontrados: {len(self.runs)}")
        print()
    
    def _safe_float(self, value: Any) -> float:
        """Convierte valor a float de forma segura."""
        if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_int(self, value: Any) -> int:
        """Convierte valor a int de forma segura."""
        if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
            return 0
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 0
    
    def _is_success(self, value: Any) -> bool:
        """Determina si el valor indica éxito."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() == "true"
        return bool(value)

    def extract_run_details(self) -> None:
        """Extrae detalles de runs, run_steps y tools desde app_dependencies."""
        print("=" * 60)
        print("EXTRAYENDO DETALLES DE RUNS, RUN STEPS Y TOOLS")
        print("=" * 60)
        
        # Diccionario para agrupar tools por run_id
        tools_by_run: Dict[str, List[ToolInfo]] = {}
        run_steps_by_run: Dict[str, List[RunStep]] = {}
        
        # Mapeo de OperationId -> run_id para correlacionar tools
        operation_to_run: Dict[str, str] = {}
        
        if self.df_dependencies is not None:
            deps_data = self._parse_properties_column(self.df_dependencies)
            
            # PRIMERA PASADA: Crear mapeo OperationId -> run_id
            for dep in deps_data:
                props = dep.get("Properties_parsed", {})
                run_id = str(props.get("gen_ai.thread.run.id", "") or "")
                operation_id = str(dep.get("OperationId", "") or "")
                
                if run_id and operation_id:
                    operation_to_run[operation_id] = run_id
            
            # SEGUNDA PASADA: Procesar todos los registros
            for dep in deps_data:
                props = dep.get("Properties_parsed", {})
                name = str(dep.get("Name", ""))
                dep_type = str(dep.get("DependencyType", ""))
                operation_id = str(dep.get("OperationId", "") or "")
                
                # Extraer IDs
                run_id = str(props.get("gen_ai.thread.run.id", "") or "")
                thread_id = str(props.get("gen_ai.thread.id", "") or "")
                operation_name = str(props.get("gen_ai.operation.name", "") or "")
                
                # Si no hay run_id, intentar obtenerlo del OperationId
                if not run_id and operation_id and operation_id in operation_to_run:
                    run_id = operation_to_run[operation_id]
                
                # Si aún no hay run_id, intentar obtenerlo del thread
                if not run_id and thread_id:
                    for rid, r in self.runs.items():
                        if r.thread_id == thread_id:
                            run_id = rid
                            break
                
                if not run_id:
                    continue
                
                # Crear run si no existe
                if run_id not in self.runs:
                    run = Run(run_id=run_id, thread_id=thread_id)
                    self.runs[run_id] = run
                    if thread_id in self.threads:
                        # Verificar que no esté duplicado
                        if not any(r.run_id == run_id for r in self.threads[thread_id].runs):
                            self.threads[thread_id].runs.append(run)
                
                run = self.runs[run_id]
                
                # Actualizar información del run
                if props.get("gen_ai.agent.id"):
                    run.agent_id = str(props.get("gen_ai.agent.id", ""))
                if props.get("gen_ai.response.model"):
                    run.model = str(props.get("gen_ai.response.model", ""))
                if props.get("gen_ai.thread.run.status"):
                    run.status = str(props.get("gen_ai.thread.run.status", ""))
                
                # Tokens del run
                if props.get("gen_ai.usage.input_tokens"):
                    run.input_tokens = self._safe_int(props.get("gen_ai.usage.input_tokens"))
                if props.get("gen_ai.usage.output_tokens"):
                    run.output_tokens = self._safe_int(props.get("gen_ai.usage.output_tokens"))
                if props.get("gen_ai.usage.total_tokens"):
                    run.total_tokens = self._safe_int(props.get("gen_ai.usage.total_tokens"))
                
                # ============================================================
                # EXTRAER TOOLS - Registros con tool.name o nombre tool.*
                # ============================================================
                is_tool = (
                    name.startswith("tool.") or 
                    name.startswith("search.") or
                    "tool.name" in props
                )
                
                if is_tool:
                    tool_name = str(props.get("tool.name", ""))
                    if not tool_name:
                        # Extraer nombre del Name (ej: "tool.gestionar_plantilla" -> "gestionar_plantilla")
                        if name.startswith("tool."):
                            tool_name = name[5:]  # Remover "tool."
                        elif name.startswith("search."):
                            tool_name = name
                        else:
                            tool_name = name
                    
                    tool = ToolInfo(
                        tool_id=str(dep.get("Id", "")),
                        tool_name=tool_name,
                        tool_type=str(props.get("tool.kind", "function")),
                        tool_description=str(props.get("tool.description", "")),
                        start_time=str(dep.get("TimeGenerated", "")),
                        duration_ms=self._safe_float(dep.get("DurationMs")),
                        input_full=str(props.get("tool.input_full", "") or props.get("tool.arguments", "")),
                        output_full=str(props.get("tool.output_full", "")),
                        status="completed" if self._is_success(dep.get("Success")) else "failed",
                        metadata={
                            "call_type": str(props.get("tool.call_type", "")),
                            "tool_function": str(props.get("tool.function", "")),
                            "tool_result": str(props.get("tool.result", "")),
                        }
                    )
                    
                    # Agrupar tools por run_id
                    if run_id not in tools_by_run:
                        tools_by_run[run_id] = []
                    tools_by_run[run_id].append(tool)
                
                # ============================================================
                # EXTRAER RUN STEPS - Operaciones del agente
                # ============================================================
                run_step_operations = [
                    "start_thread_run", "get_thread_run", "submit_tool_outputs",
                    "create_message", "list_messages"
                ]
                
                if operation_name in run_step_operations:
                    step_type = "tool_calls" if operation_name == "submit_tool_outputs" else "message_creation"
                    
                    run_step = RunStep(
                        step_id=str(dep.get("Id", "")),
                        step_type=step_type,
                        start_time=str(dep.get("TimeGenerated", "")),
                        duration_ms=self._safe_float(dep.get("DurationMs")),
                        status=str(props.get("gen_ai.thread.run.status", "completed")),
                        metadata={
                            "operation_name": operation_name,
                            "agent_id": str(props.get("gen_ai.agent.id", "")),
                            "model": str(props.get("gen_ai.response.model", ""))
                        }
                    )
                    
                    if run_id not in run_steps_by_run:
                        run_steps_by_run[run_id] = []
                    run_steps_by_run[run_id].append(run_step)
        
        # ============================================================
        # ASOCIAR TOOLS Y RUN_STEPS A LOS RUNS
        # ============================================================
        for run_id, run in self.runs.items():
            # Agregar run_steps
            if run_id in run_steps_by_run:
                run.run_steps = run_steps_by_run[run_id]
            
            # Agregar tools al primer run_step de tipo tool_calls o crear uno nuevo
            if run_id in tools_by_run:
                tools = tools_by_run[run_id]
                
                # Buscar o crear run_step de tipo tool_calls
                tool_step = None
                for step in run.run_steps:
                    if step.step_type == "tool_calls":
                        tool_step = step
                        break
                
                if not tool_step:
                    # Crear un nuevo run_step para las tools
                    tool_step = RunStep(
                        step_id=f"step_tools_{run_id}",
                        step_type="tool_calls",
                        start_time=tools[0].start_time if tools else "",
                        status="completed"
                    )
                    run.run_steps.insert(0, tool_step)
                
                tool_step.tools = tools
                
                # Calcular duración total del step
                total_duration = sum(t.duration_ms for t in tools)
                tool_step.duration_ms = total_duration
        
        # Estadísticas
        total_tools = sum(len(tools) for tools in tools_by_run.values())
        total_steps = sum(len(steps) for steps in run_steps_by_run.values())
        
        print(f"✓ Runs actualizados: {len(self.runs)}")
        print(f"✓ Run Steps extraídos: {total_steps}")
        print(f"✓ Tools extraídos: {total_tools}")
        print()
    
    def _parse_timestamp(self, ts_str: str) -> Optional[datetime]:
        """Parsea un timestamp ISO a datetime."""
        if not ts_str:
            return None
        try:
            # Remover 'Z' y parsear
            ts_clean = ts_str.replace("Z", "+00:00")
            return datetime.fromisoformat(ts_clean)
        except:
            return None
    
    def _find_run_by_timestamp(self, msg_time: datetime, thread_id: str) -> Optional[Run]:
        """
        Encuentra el run correcto para un mensaje basándose en timestamp.
        El mensaje de usuario debe pertenecer al run cuyo start_time es el más 
        cercano ANTERIOR al timestamp del mensaje.
        """
        if not msg_time:
            return None
        
        # Obtener runs del mismo thread, ordenados por start_time
        thread_runs = []
        for run in self.runs.values():
            if run.thread_id == thread_id:
                run_time = self._parse_timestamp(run.start_time)
                if run_time:
                    thread_runs.append((run_time, run))
        
        if not thread_runs:
            return None
        
        # Ordenar por tiempo
        thread_runs.sort(key=lambda x: x[0])
        
        # Encontrar el run cuyo start_time es el más cercano anterior al mensaje
        target_run = None
        for run_time, run in thread_runs:
            if run_time <= msg_time:
                target_run = run
            else:
                break
        
        return target_run

    def extract_messages(self) -> None:
        """Extrae mensajes de entrada y salida desde app_traces."""
        print("=" * 60)
        print("EXTRAYENDO MENSAJES Y METADATA")
        print("=" * 60)
        
        if self.df_traces is None:
            print("✗ No hay datos de traces")
            return
        
        traces_data = self._parse_properties_column(self.df_traces)
        messages_extracted = 0
        metadata_extracted = 0
        
        for trace in traces_data:
            message = str(trace.get("Message", "") or "")
            props = trace.get("Properties_parsed", {})
            operation_id = str(trace.get("OperationId", "") or "")
            time_generated = str(trace.get("TimeGenerated", "") or "")
            msg_time = self._parse_timestamp(time_generated)
            
            # Buscar el run asociado
            run_id = str(props.get("gen_ai.thread.run.id", "") or "")
            thread_id = str(props.get("gen_ai.thread.id", "") or "")
            
            # ============================================================
            # MENSAJES DE USUARIO (gen_ai.user.message)
            # Los mensajes de usuario NO tienen run_id, hay que asociarlos
            # por timestamp: el run cuyo start_time sea anterior y más cercano
            # ============================================================
            if message == "gen_ai.user.message":
                event_content = self._extract_nested_json(props, "gen_ai.event.content")
                content = str(event_content.get("content", ""))
                
                # Buscar el run correcto por timestamp
                target_run = self._find_run_by_timestamp(msg_time, thread_id)
                
                if target_run and not target_run.input_message:
                    target_run.input_message = content
                    messages_extracted += 1
            
            # ============================================================
            # MENSAJES DEL ASISTENTE (gen_ai.assistant.message)
            # Los mensajes del asistente SÍ tienen run_id, asociar directamente
            # ============================================================
            elif message == "gen_ai.assistant.message":
                event_content = self._extract_nested_json(props, "gen_ai.event.content")
                content_obj = event_content.get("content", {})
                
                # Extraer el texto del mensaje
                if isinstance(content_obj, dict):
                    text_obj = content_obj.get("text", {})
                    if isinstance(text_obj, dict):
                        content = str(text_obj.get("value", ""))
                    else:
                        content = str(text_obj)
                else:
                    content = str(content_obj)
                
                # Asociar con el run correcto
                if run_id and run_id in self.runs:
                    if not self.runs[run_id].output_message:
                        self.runs[run_id].output_message = content
                        messages_extracted += 1
            
            # ============================================================
            # RUN COMPLETED - Tokens y duración final
            # Los logs run_completed NO tienen run_id ni thread_id, hay que
            # asociarlos por timestamp al run más cercano anterior
            # ============================================================
            elif message == "run_completed":
                # Buscar el run por timestamp (probar con todos los threads)
                target_run = None
                for tid in self.threads.keys():
                    candidate = self._find_run_by_timestamp(msg_time, tid)
                    if candidate and not candidate.total_tokens:
                        target_run = candidate
                        break
                
                if target_run:
                    target_run.input_tokens = self._safe_int(props.get("prompt_tokens"))
                    target_run.output_tokens = self._safe_int(props.get("completion_tokens"))
                    target_run.total_tokens = self._safe_int(props.get("total_tokens"))
                    target_run.duration_ms = self._safe_float(props.get("duration_ms"))
                    status = str(props.get("status", ""))
                    target_run.status = status.replace("RunStatus.", "")
                    target_run.end_time = time_generated
                    metadata_extracted += 1
            
            # ============================================================
            # AUDIT.BUSINESS - Log completo con toda la información
            # ============================================================
            elif '"log_type": "audit.business"' in message or "audit.business" in message:
                try:
                    log_data = json.loads(message) if message.startswith("{") else {}
                    if log_data and log_data.get("log_type") == "audit.business":
                        conversation = log_data.get("conversation", {})
                        tid = str(log_data.get("thread_id", ""))
                        extra = log_data.get("extra", {})
                        target_run_id = str(extra.get("run_id", ""))
                        
                        # Buscar el run específico por run_id
                        target_run = None
                        if target_run_id and target_run_id in self.runs:
                            target_run = self.runs[target_run_id]
                        else:
                            # Buscar por thread_id
                            for rid, run in self.runs.items():
                                if run.thread_id == tid:
                                    target_run = run
                                    break
                        
                        if target_run:
                            if not target_run.input_message:
                                target_run.input_message = str(conversation.get("input", ""))
                            if not target_run.output_message:
                                target_run.output_message = str(conversation.get("output", ""))
                            
                            metrics = log_data.get("metrics", {})
                            if metrics and not target_run.total_tokens:
                                target_run.input_tokens = self._safe_int(metrics.get("prompt_tokens"))
                                target_run.output_tokens = self._safe_int(metrics.get("completion_tokens"))
                                target_run.total_tokens = self._safe_int(metrics.get("total_tokens"))
                                target_run.duration_ms = self._safe_float(metrics.get("duration_ms"))
                            
                            # Metadata adicional
                            target_run.status = str(log_data.get("status", "")).replace("RunStatus.", "")
                            if extra.get("agent_id"):
                                target_run.agent_id = str(extra.get("agent_id", ""))
                            target_run.end_time = time_generated
                            
                            # Guardar metadata del audit
                            target_run.metadata = {
                                "trace_id": str(log_data.get("trace_id", "")),
                                "span_id": str(log_data.get("span_id", "")),
                                "user_id": str(log_data.get("user_id", "")),
                                "input_length": conversation.get("input_length", 0),
                                "output_length": conversation.get("output_length", 0),
                            }
                            metadata_extracted += 1
                except json.JSONDecodeError:
                    pass
            
            # ============================================================
            # TOOL MESSAGES - Mensajes de herramientas
            # ============================================================
            elif message == "gen_ai.tool.message":
                event_content = self._extract_nested_json(props, "gen_ai.event.content")
                tool_content = str(event_content.get("content", ""))
                tool_id = str(event_content.get("id", ""))
                
                # Buscar la tool correspondiente en los runs
                if run_id and run_id in self.runs:
                    run = self.runs[run_id]
                    for step in run.run_steps:
                        for tool in step.tools:
                            if tool.tool_id == tool_id or not tool.output_full:
                                if not tool.output_full:
                                    tool.output_full = tool_content
                                break
        
        print(f"✓ Mensajes extraídos: {messages_extracted}")
        print(f"✓ Metadata extraída: {metadata_extracted}")
        print()
    
    def calculate_aggregations(self) -> None:
        """Calcula agregaciones a nivel de Thread y User."""
        print("=" * 60)
        print("CALCULANDO AGREGACIONES")
        print("=" * 60)
        
        # Agregar tokens y tiempos a nivel de Thread
        for thread_id, thread in self.threads.items():
            total_input = 0
            total_output = 0
            total_tokens = 0
            min_time = None
            max_time = None
            
            for run in thread.runs:
                total_input += run.input_tokens
                total_output += run.output_tokens
                total_tokens += run.total_tokens
                
                # Calcular tiempos
                if run.start_time:
                    try:
                        run_time = datetime.fromisoformat(run.start_time.replace("Z", "+00:00"))
                        if min_time is None or run_time < min_time:
                            min_time = run_time
                        if max_time is None or run_time > max_time:
                            max_time = run_time
                    except:
                        pass
            
            thread.total_input_tokens = total_input
            thread.total_output_tokens = total_output
            thread.total_tokens = total_tokens
            
            if min_time:
                thread.start_time = min_time.isoformat()
            if max_time:
                thread.end_time = max_time.isoformat()
            if min_time and max_time:
                thread.duration_ms = (max_time - min_time).total_seconds() * 1000
        
        # Agregar a nivel de User
        for user_id, user in self.users.items():
            total_tokens = 0
            total_interactions = 0
            
            for thread in user.threads:
                total_tokens += thread.total_tokens
                total_interactions += len(thread.runs)
            
            user.total_tokens = total_tokens
            user.total_interactions = total_interactions
        
        print(f"✓ Agregaciones calculadas")
        print()
    
    def analyze(self) -> Dict[str, User]:
        """
        Ejecuta el análisis completo de telemetría.
        
        Returns:
            Diccionario de usuarios con toda la estructura jerárquica
        """
        self.load_csv_files()
        self.extract_users_and_threads()
        self.extract_run_details()
        self.extract_messages()
        self.calculate_aggregations()
        
        return self.users
    
    def print_summary(self) -> None:
        """Imprime un resumen del análisis."""
        print("=" * 60)
        print("RESUMEN DEL ANÁLISIS")
        print("=" * 60)
        
        print(f"\n📊 ESTADÍSTICAS GENERALES")
        print(f"   Usuarios únicos: {len(self.users)}")
        print(f"   Threads totales: {len(self.threads)}")
        print(f"   Runs totales: {len(self.runs)}")
        
        total_tokens = sum(u.total_tokens for u in self.users.values())
        print(f"   Tokens totales: {total_tokens:,}")
        
        print("\n" + "=" * 60)
    
    def print_hierarchical_view(self) -> None:
        """Imprime la vista jerárquica completa al estilo Azure AI Foundry."""
        print("\n" + "=" * 60)
        print("VISTA JERÁRQUICA - ESTILO AZURE AI FOUNDRY")
        print("=" * 60)
        
        for user_id, user in self.users.items():
            print(f"\n👤 USER: {user_id}")
            print(f"   Total Interactions: {user.total_interactions}")
            print(f"   Total Tokens: {user.total_tokens:,}")
            
            for thread in user.threads:
                print(f"\n   📋 THREAD: {thread.thread_id}")
                print(f"      Status: ✅ Completed")
                print(f"      Total Tokens: {thread.total_tokens:,}")
                print(f"      Duration: {thread.duration_ms:.0f}ms")
                
                # Ordenar runs por start_time (cronológicamente)
                sorted_runs = sorted(thread.runs, key=lambda r: r.start_time or "")
                for run in sorted_runs:
                    status_icon = "✅" if run.status.lower() == "completed" else "⏳"
                    print(f"\n      🔄 RUN: {run.run_id}")
                    print(f"         Status: {status_icon} {run.status}")
                    print(f"         Duration: {run.duration_ms:.0f}ms")
                    print(f"         Tokens: {run.total_tokens:,} (in: {run.input_tokens:,}, out: {run.output_tokens:,})")
                    print(f"         Model: {run.model}")
                    
                    # Input/Output messages
                    if run.input_message:
                        preview = run.input_message[:100] + "..." if len(run.input_message) > 100 else run.input_message
                        print(f"         📥 Input: \"{preview}\"")
                    if run.output_message:
                        preview = run.output_message[:100] + "..." if len(run.output_message) > 100 else run.output_message
                        print(f"         📤 Output: \"{preview}\"")
                    
                    # Run Steps y Tools
                    for step in run.run_steps:
                        print(f"\n         📌 RUN STEP: {step.step_type}")
                        print(f"            Duration: {step.duration_ms:.0f}ms")
                        
                        for tool in step.tools:
                            print(f"\n            🔧 TOOL: {tool.tool_name}")
                            print(f"               Type: {tool.tool_type}")
                            print(f"               Duration: {tool.duration_ms:.0f}ms")
                            if tool.input_full:
                                input_preview = tool.input_full[:80] + "..." if len(tool.input_full) > 80 else tool.input_full
                                print(f"               Input: {input_preview}")
                            if tool.output_full:
                                output_preview = tool.output_full[:80] + "..." if len(tool.output_full) > 80 else tool.output_full
                                print(f"               Output: {output_preview}")
        
        print("\n" + "=" * 60)
    
    def to_json(self) -> str:
        """Exporta la estructura completa a JSON."""
        def convert_to_dict(obj):
            if hasattr(obj, '__dataclass_fields__'):
                return asdict(obj)
            elif isinstance(obj, list):
                return [convert_to_dict(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: convert_to_dict(v) for k, v in obj.items()}
            return obj
        
        return json.dumps(convert_to_dict(self.users), indent=2, ensure_ascii=False)
    
    def get_dataframes(self) -> Dict[str, SparkDataFrame]:
        """
        Retorna Spark DataFrames con la información procesada.
        
        Returns:
            Diccionario con DataFrames para users, threads, runs, run_steps, tools
        """
        # Crear listas para cada nivel
        users_data = []
        threads_data = []
        runs_data = []
        run_steps_data = []
        tools_data = []
        
        for user_id, user in self.users.items():
            users_data.append({
                "user_id": user_id,
                "total_interactions": user.total_interactions,
                "total_tokens": user.total_tokens
            })
            
            for thread in user.threads:
                threads_data.append({
                    "user_id": user_id,
                    "thread_id": thread.thread_id,
                    "start_time": thread.start_time,
                    "end_time": thread.end_time,
                    "duration_ms": thread.duration_ms,
                    "total_input_tokens": thread.total_input_tokens,
                    "total_output_tokens": thread.total_output_tokens,
                    "total_tokens": thread.total_tokens
                })
                
                for run in thread.runs:
                    runs_data.append({
                        "thread_id": thread.thread_id,
                        "run_id": run.run_id,
                        "agent_id": run.agent_id,
                        "model": run.model,
                        "start_time": run.start_time,
                        "end_time": run.end_time,
                        "duration_ms": run.duration_ms,
                        "status": run.status,
                        "input_tokens": run.input_tokens,
                        "output_tokens": run.output_tokens,
                        "total_tokens": run.total_tokens,
                        "input_message": run.input_message,
                        "output_message": run.output_message
                    })
                    
                    for step in run.run_steps:
                        run_steps_data.append({
                            "run_id": run.run_id,
                            "step_id": step.step_id,
                            "step_type": step.step_type,
                            "start_time": step.start_time,
                            "end_time": step.end_time,
                            "duration_ms": step.duration_ms,
                            "status": step.status,
                            "input_tokens": step.input_tokens,
                            "output_tokens": step.output_tokens,
                            "total_tokens": step.total_tokens
                        })
                        
                        for tool in step.tools:
                            tools_data.append({
                                "step_id": step.step_id,
                                "tool_id": tool.tool_id,
                                "tool_name": tool.tool_name,
                                "tool_type": tool.tool_type,
                                "tool_description": tool.tool_description,
                                "start_time": tool.start_time,
                                "end_time": tool.end_time,
                                "duration_ms": tool.duration_ms,
                                "input_full": tool.input_full,
                                "output_full": tool.output_full,
                                "status": tool.status
                            })
        
        # Crear Spark DataFrames
        dfs = {}
        
        if users_data:
            dfs["users"] = self.spark.createDataFrame(users_data)
        if threads_data:
            dfs["threads"] = self.spark.createDataFrame(threads_data)
        if runs_data:
            dfs["runs"] = self.spark.createDataFrame(runs_data)
        if run_steps_data:
            dfs["run_steps"] = self.spark.createDataFrame(run_steps_data)
        if tools_data:
            dfs["tools"] = self.spark.createDataFrame(tools_data)
        
        return dfs
    
    def stop(self) -> None:
        """Detiene la sesión de Spark."""
        if self.spark:
            self.spark.stop()

# COMMAND ----------

# =============================================================================
# FUNCIONES AUXILIARES PARA ANÁLISIS ADICIONAL
# =============================================================================

def analyze_token_usage(analyzer: TelemetryAnalyzer) -> None:
    """Analiza el uso de tokens por usuario, thread y run."""
    print("\n" + "=" * 60)
    print("ANÁLISIS DE USO DE TOKENS")
    print("=" * 60)
    
    dfs = analyzer.get_dataframes()
    
    if "runs" in dfs:
        print("\n📊 Tokens por Run:")
        runs_df = dfs["runs"].select("run_id", "input_tokens", "output_tokens", "total_tokens")
        runs_df = runs_df.orderBy(F.col("total_tokens").desc()).limit(10)
        runs_df.show(truncate=False)
    
    if "threads" in dfs:
        print("\n📊 Tokens por Thread:")
        threads_df = dfs["threads"].select("thread_id", "total_input_tokens", "total_output_tokens", "total_tokens")
        threads_df = threads_df.orderBy(F.col("total_tokens").desc()).limit(10)
        threads_df.show(truncate=False)


def analyze_tool_usage(analyzer: TelemetryAnalyzer) -> None:
    """Analiza el uso de herramientas."""
    print("\n" + "=" * 60)
    print("ANÁLISIS DE USO DE HERRAMIENTAS")
    print("=" * 60)
    
    dfs = analyzer.get_dataframes()
    
    if "tools" in dfs and dfs["tools"].count() > 0:
        print("\n🔧 Herramientas más utilizadas:")
        tools_summary = dfs["tools"].groupBy("tool_name").agg(
            F.count("tool_id").alias("invocations"),
            F.avg("duration_ms").alias("avg_duration_ms")
        ).orderBy(F.col("invocations").desc()).limit(10)
        tools_summary.show(truncate=False)
    else:
        print("\n   No se encontraron herramientas en los datos")


def analyze_latency(analyzer: TelemetryAnalyzer) -> None:
    """Analiza la latencia de las operaciones."""
    print("\n" + "=" * 60)
    print("ANÁLISIS DE LATENCIA")
    print("=" * 60)
    
    dfs = analyzer.get_dataframes()
    
    if "runs" in dfs:
        print("\n⏱️ Latencia de Runs:")
        runs_df = dfs["runs"]
        stats = runs_df.agg(
            F.avg("duration_ms").alias("avg_duration_ms"),
            F.min("duration_ms").alias("min_duration_ms"),
            F.max("duration_ms").alias("max_duration_ms")
        ).collect()[0]
        print(f"   avg_duration_ms: {stats['avg_duration_ms']:.2f}")
        print(f"   min_duration_ms: {stats['min_duration_ms']:.2f}")
        print(f"   max_duration_ms: {stats['max_duration_ms']:.2f}")

# COMMAND ----------

# =============================================================================
# PUNTO DE ENTRADA PRINCIPAL
# =============================================================================

def main():
    """Función principal de ejecución."""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║          TELEMETRY ANALYZER - AZURE AI AGENTS                ║
    ║        Análisis de telemetría con PySpark                    ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Crear el analizador con PySpark (lee desde tablas Hive)
    analyzer = TelemetryAnalyzer()
    
    try:
        # Ejecutar análisis completo
        users = analyzer.analyze()
        
        # Mostrar resumen
        analyzer.print_summary()
        
        # Mostrar vista jerárquica
        analyzer.print_hierarchical_view()
        
        # Análisis adicionales
        analyze_token_usage(analyzer)
        analyze_tool_usage(analyzer)
        analyze_latency(analyzer)
        
        # Exportar a JSON (como string)
        json_output = analyzer.to_json()
        print(f"\n✓ Análisis exportado a JSON (disponible en variable json_output)")
        
        # Obtener DataFrames para análisis adicional
        dfs = analyzer.get_dataframes()
        print(f"\n📊 DataFrames disponibles: {list(dfs.keys())}")
        
        # Mostrar esquemas de los DataFrames (Spark)
        print("\n" + "=" * 60)
        print("ESQUEMAS DE DATAFRAMES (SPARK)")
        print("=" * 60)
        for name, df in dfs.items():
            print(f"\n{name.upper()}:")
            print(f"   Columnas: {df.columns}")
            print(f"   Registros: {df.count()}")
            df.printSchema()
        
    finally:
        # Detener Spark (no necesario en Databricks)
        analyzer.stop()
    
    print("\n✅ Análisis completado exitosamente")
    return analyzer


if __name__ == "__main__":
    analyzer = main()