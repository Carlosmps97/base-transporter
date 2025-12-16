# Inicia sesión primero con: Connect-AzAccount
# Instala el módulo si no lo tienes: Install-Module Az.OperationalInsights

$ResourceGroup = "rg-agents-lab01"
$WorkspaceName = "logdratacraft02"

# Las tablas principales de App Insights que seguramente quieres limpiar
$Tables = @("AppTraces", "AppRequests", "AppDependencies", "AppExceptions", "AppMetrics")

# Define el filtro: Borrar todo lo anterior a "ahora mismo"
$TimeFilter = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")

foreach ($Table in $Tables) {
    Write-Host "Purgando tabla: $Table..."
    
    # Envía la orden de purga
    New-AzOperationalInsightsPurgeWorkspace `
        -ResourceGroupName $ResourceGroup `
        -WorkspaceName $WorkspaceName `
        -Table $Table `
        -Column "TimeGenerated" `
        -Operator "<" `
        -Value $TimeFilter
}

Write-Host "Solicitudes de purga enviadas. Nota: Los datos pueden tardar horas en desaparecer."