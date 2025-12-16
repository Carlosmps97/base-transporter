-- Databricks notebook source
-- Crear schema si no existe
CREATE SCHEMA IF NOT EXISTS hive_metastore.bronze;

-- COMMAND ----------

-- Crear tabla Bronze AppRequests
CREATE TABLE IF NOT EXISTS hive_metastore.bronze.app_requests (
    TenantId                STRING COMMENT 'The Log Analytics workspace ID',
    TimeGenerated           STRING COMMENT 'Date and time when request processing started (ISO8601)',
    Id                      STRING COMMENT 'Application-generated unique request ID',
    Source                  STRING COMMENT 'Friendly name of the request source',
    Name                    STRING COMMENT 'Human-readable name of the request',
    Url                     STRING COMMENT 'URL of the request',
    Success                 STRING COMMENT 'Whether the request was handled successfully (true/false)',
    ResultCode              STRING COMMENT 'HTTP result code returned by the application',
    DurationMs              STRING COMMENT 'Milliseconds to handle the request',
    PerformanceBucket       STRING COMMENT 'Performance bucket category',
    Properties              STRING COMMENT 'Application-defined properties (JSON)',
    Measurements            STRING COMMENT 'Application-defined measurements (JSON)',
    OperationName           STRING COMMENT 'Application-defined name of the overall operation',
    OperationId             STRING COMMENT 'Application-defined operation ID for correlation',
    OperationLinks          STRING COMMENT 'Operation links (JSON)',
    ParentId                STRING COMMENT 'ID of the parent operation',
    SyntheticSource         STRING COMMENT 'Synthetic source of the operation',
    SessionId               STRING COMMENT 'Application-defined session ID',
    UserId                  STRING COMMENT 'Anonymous ID of user accessing the application',
    UserAuthenticatedId     STRING COMMENT 'Persistent string representing authenticated user',
    UserAccountId           STRING COMMENT 'Application-defined account associated with user',
    AppVersion              STRING COMMENT 'Version of the application',
    AppRoleName             STRING COMMENT 'Role name of the application',
    AppRoleInstance         STRING COMMENT 'Role instance of the application',
    ClientType              STRING COMMENT 'Type of the client device',
    ClientModel             STRING COMMENT 'Model of the client device',
    ClientOS                STRING COMMENT 'Operating system of the client device',
    ClientIP                STRING COMMENT 'IP address of the client device',
    ClientCity              STRING COMMENT 'City where the client device is located',
    ClientStateOrProvince   STRING COMMENT 'State or province of the client device',
    ClientCountryOrRegion   STRING COMMENT 'Country or region of the client device',
    ClientBrowser           STRING COMMENT 'Browser running on the client device',
    ResourceGUID            STRING COMMENT 'Unique persistent identifier of Azure resource',
    IKey                    STRING COMMENT 'Instrumentation key of the Azure resource',
    SDKVersion              STRING COMMENT 'Version of the SDK used to generate telemetry',
    ItemCount               STRING COMMENT 'Number of telemetry items in sample',
    ReferencedItemId        STRING COMMENT 'ID of item with additional details',
    ReferencedType          STRING COMMENT 'Name of table with additional details',
    SourceSystem            STRING COMMENT 'Type of agent that collected the event',
    Type                    STRING COMMENT 'The name of the table (AppRequests)',
    _ResourceId             STRING COMMENT 'Unique identifier for the Azure resource',
    TimeGeneratedUTCMinus5  STRING COMMENT 'TimeGenerated adjusted to UTC-5 timezone',
    date_routine            STRING COMMENT 'Fecha de ejecución del proceso'
)
USING DELTA
PARTITIONED BY (date_routine)
COMMENT 'Bronze layer - Raw Application Insights requests from Log Analytics export'
LOCATION 'abfss://agentsdata@adlsagentslab01.dfs.core.windows.net/data/bronze/app_requests';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS hive_metastore.bronze.app_traces (
    TenantId                STRING COMMENT 'The Log Analytics workspace ID',
    TimeGenerated           STRING COMMENT 'Date and time when trace was recorded (ISO8601)',
    Message                 STRING COMMENT 'Trace message',
    SeverityLevel           STRING COMMENT 'Severity level of the trace (Verbose, Information, Warning, Error, Critical)',
    Properties              STRING COMMENT 'Application-defined properties (JSON)',
    Measurements            STRING COMMENT 'Application-defined measurements (JSON)',
    OperationName           STRING COMMENT 'Application-defined name of the overall operation',
    OperationId             STRING COMMENT 'Application-defined operation ID for correlation',
    ParentId                STRING COMMENT 'ID of the parent operation',
    SyntheticSource         STRING COMMENT 'Synthetic source of the operation',
    SessionId               STRING COMMENT 'Application-defined session ID',
    UserId                  STRING COMMENT 'Anonymous ID of user accessing the application',
    UserAuthenticatedId     STRING COMMENT 'Persistent string representing authenticated user',
    UserAccountId           STRING COMMENT 'Application-defined account associated with user',
    AppVersion              STRING COMMENT 'Version of the application',
    AppRoleName             STRING COMMENT 'Role name of the application',
    AppRoleInstance         STRING COMMENT 'Role instance of the application',
    ClientType              STRING COMMENT 'Type of the client device',
    ClientModel             STRING COMMENT 'Model of the client device',
    ClientOS                STRING COMMENT 'Operating system of the client device',
    ClientIP                STRING COMMENT 'IP address of the client device',
    ClientCity              STRING COMMENT 'City where the client device is located',
    ClientStateOrProvince   STRING COMMENT 'State or province of the client device',
    ClientCountryOrRegion   STRING COMMENT 'Country or region of the client device',
    ClientBrowser           STRING COMMENT 'Browser running on the client device',
    ResourceGUID            STRING COMMENT 'Unique persistent identifier of Azure resource',
    IKey                    STRING COMMENT 'Instrumentation key of the Azure resource',
    SDKVersion              STRING COMMENT 'Version of the SDK used to generate telemetry',
    ItemCount               STRING COMMENT 'Number of telemetry items in sample',
    ReferencedItemId        STRING COMMENT 'ID of item with additional details',
    ReferencedType          STRING COMMENT 'Name of table with additional details',
    SourceSystem            STRING COMMENT 'Type of agent that collected the event',
    Type                    STRING COMMENT 'The name of the table (AppTraces)',
    _ResourceId             STRING COMMENT 'Unique identifier for the Azure resource',
    TimeGeneratedUTCMinus5  STRING COMMENT 'TimeGenerated adjusted to UTC-5 timezone',
    date_routine            STRING COMMENT 'Date of the ingestion routine execution'
)
USING DELTA
PARTITIONED BY (date_routine)
COMMENT 'Bronze layer - Raw Application Insights traces from Log Analytics export'
LOCATION 'abfss://agentsdata@adlsagentslab01.dfs.core.windows.net/data/bronze/app_traces';

-- COMMAND ----------

-- Crear tabla Bronze AppDependencies
CREATE TABLE IF NOT EXISTS hive_metastore.bronze.app_dependencies (
    TenantId                STRING COMMENT 'The Log Analytics workspace ID',
    TimeGenerated           STRING COMMENT 'Date and time when dependency call was recorded (ISO8601)',
    Id                      STRING COMMENT 'Application-generated unique dependency ID',
    Target                  STRING COMMENT 'Target site of a dependency call (server name, host address)',
    DependencyType          STRING COMMENT 'Type of the dependency (SQL, HTTP, Azure blob, etc.)',
    Name                    STRING COMMENT 'Human-readable name of the dependency call',
    Data                    STRING COMMENT 'Dependency call command (SQL statement, HTTP URL, etc.)',
    Success                 STRING COMMENT 'Whether the dependency call was successful (true/false)',
    ResultCode              STRING COMMENT 'Result code returned by the dependency call',
    DurationMs              STRING COMMENT 'Milliseconds taken by the dependency call',
    PerformanceBucket       STRING COMMENT 'Performance bucket category',
    Properties              STRING COMMENT 'Application-defined properties (JSON)',
    Measurements            STRING COMMENT 'Application-defined measurements (JSON)',
    OperationName           STRING COMMENT 'Application-defined name of the overall operation',
    OperationId             STRING COMMENT 'Application-defined operation ID for correlation',
    ParentId                STRING COMMENT 'ID of the parent operation',
    SyntheticSource         STRING COMMENT 'Synthetic source of the operation',
    SessionId               STRING COMMENT 'Application-defined session ID',
    UserId                  STRING COMMENT 'Anonymous ID of user accessing the application',
    UserAuthenticatedId     STRING COMMENT 'Persistent string representing authenticated user',
    UserAccountId           STRING COMMENT 'Application-defined account associated with user',
    AppVersion              STRING COMMENT 'Version of the application',
    AppRoleName             STRING COMMENT 'Role name of the application',
    AppRoleInstance         STRING COMMENT 'Role instance of the application',
    ClientType              STRING COMMENT 'Type of the client device',
    ClientModel             STRING COMMENT 'Model of the client device',
    ClientOS                STRING COMMENT 'Operating system of the client device',
    ClientIP                STRING COMMENT 'IP address of the client device',
    ClientCity              STRING COMMENT 'City where the client device is located',
    ClientStateOrProvince   STRING COMMENT 'State or province of the client device',
    ClientCountryOrRegion   STRING COMMENT 'Country or region of the client device',
    ClientBrowser           STRING COMMENT 'Browser running on the client device',
    ResourceGUID            STRING COMMENT 'Unique persistent identifier of Azure resource',
    IKey                    STRING COMMENT 'Instrumentation key of the Azure resource',
    SDKVersion              STRING COMMENT 'Version of the SDK used to generate telemetry',
    ItemCount               STRING COMMENT 'Number of telemetry items in sample',
    ReferencedItemId        STRING COMMENT 'ID of item with additional details',
    ReferencedType          STRING COMMENT 'Name of table with additional details',
    SourceSystem            STRING COMMENT 'Type of agent that collected the event',
    Type                    STRING COMMENT 'The name of the table (AppDependencies)',
    _ResourceId             STRING COMMENT 'Unique identifier for the Azure resource',
    TimeGeneratedUTCMinus5  STRING COMMENT 'TimeGenerated adjusted to UTC-5 timezone',
    date_routine            STRING COMMENT 'Date of the ingestion routine execution'
)
USING DELTA
PARTITIONED BY (date_routine)
COMMENT 'Bronze layer - Raw Application Insights dependencies from Log Analytics export'
LOCATION 'abfss://agentsdata@adlsagentslab01.dfs.core.windows.net/data/bronze/app_dependencies';

-- COMMAND ----------

select * from hive_metastore.bronze.app_dependencies;