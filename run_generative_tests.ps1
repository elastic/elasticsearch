# PowerShell script to run GenerativeIT tests and collect failures
# Filters out "Unknown column" errors for unmapped fields used after STATS/KEEP/DROP incorrectly

$unmappedFieldNames = @("foo", "foobar", "bar", "baz")
$outputFile = "generative_test_failures.txt"
$totalRuns = 100
$tempOutputFile = "temp_test_output.txt"

# Clear output file
"GenerativeIT Test Failures Report" | Out-File -FilePath $outputFile -Encoding UTF8
"=" * 80 | Out-File -FilePath $outputFile -Append -Encoding UTF8
"Run started: $(Get-Date)" | Out-File -FilePath $outputFile -Append -Encoding UTF8
"Total planned runs: $totalRuns" | Out-File -FilePath $outputFile -Append -Encoding UTF8
"=" * 80 | Out-File -FilePath $outputFile -Append -Encoding UTF8
"" | Out-File -FilePath $outputFile -Append -Encoding UTF8

$failureCount = 0
$filteredCount = 0
$successCount = 0

function Should-FilterFailure {
    param (
        [string]$query,
        [string]$fullError
    )
    
    # Check if this is an "Unknown column" error
    if ($fullError -notmatch "Unknown column \[([^\]]+)\]") {
        return $false
    }
    
    $columnName = $Matches[1]
    
    # Check if the column is one of our unmapped field names
    if ($columnName -notin $unmappedFieldNames) {
        return $false
    }
    
    # Normalize query to lowercase for easier matching
    $queryLower = $query.ToLower()
    
    # Find schema-fixing commands: STATS (not INLINE STATS), KEEP, DROP
    $hasSchemaFixingCommand = $false
    $schemaFixPosition = -1
    
    # Check for STATS (not inline stats)
    if ($queryLower -match "\|\s*stats\s") {
        $statsMatch = [regex]::Match($queryLower, "\|\s*stats\s")
        if ($statsMatch.Success) {
            $beforeStats = $queryLower.Substring(0, $statsMatch.Index)
            if ($beforeStats -notmatch "\|\s*inline\s*$") {
                $hasSchemaFixingCommand = $true
                $schemaFixPosition = $statsMatch.Index
            }
        }
    }
    
    # Check for KEEP
    if ($queryLower -match "\|\s*keep\s") {
        $keepMatch = [regex]::Match($queryLower, "\|\s*keep\s")
        if ($keepMatch.Success) {
            $hasSchemaFixingCommand = $true
            if ($schemaFixPosition -eq -1 -or $keepMatch.Index -lt $schemaFixPosition) {
                $schemaFixPosition = $keepMatch.Index
            }
        }
    }
    
    # Check for DROP
    if ($queryLower -match "\|\s*drop\s") {
        $dropMatch = [regex]::Match($queryLower, "\|\s*drop\s")
        if ($dropMatch.Success) {
            $hasSchemaFixingCommand = $true
            if ($schemaFixPosition -eq -1 -or $dropMatch.Index -lt $schemaFixPosition) {
                $schemaFixPosition = $dropMatch.Index
            }
        }
    }
    
    if (-not $hasSchemaFixingCommand) {
        return $false
    }
    
    $afterSchemaFix = $queryLower.Substring($schemaFixPosition)
    
    # Extract the schema-fixing command (up to the next pipe)
    $schemaCommand = ""
    if ($afterSchemaFix -match "^(\|[^|]+)") {
        $schemaCommand = $Matches[1]
    }
    
    # If the unmapped field IS used in the schema-fixing command, don't filter
    if ($schemaCommand -match "\b$columnName\b") {
        return $false
    }
    
    # Check if the unmapped field appears anywhere after the schema-fixing command
    if ($afterSchemaFix -match "\b$columnName\b") {
        return $true
    }
    
    return $false
}

function Extract-TestFailure {
    param (
        [string]$rawOutput
    )
    
    $result = @{
        Query = ""
        Error = ""
        StackTrace = @()
    }
    
    # Find the AssertionError line which contains "query:" and "exception:"
    # The format is: java.lang.AssertionError: query: <QUERY>\nexception: <EXCEPTION_MESSAGE>
    
    # First try to extract the query
    if ($rawOutput -match "java\.lang\.AssertionError:\s*query:\s*(.+?)(?:\r?\n\s*exception:|\r?\n\s*error:)") {
        $result.Query = $Matches[1].Trim()
    } elseif ($rawOutput -match "query:\s*(.+?)(?:\r?\nexception:|\r?\nerror:)") {
        $result.Query = $Matches[1].Trim()
    }
    
    # Extract the full exception/error message
    # Look for "exception:" or "error:" followed by content until the test stack trace starts
    if ($rawOutput -match "(?:exception|error):\s*(.+?)(?:\r?\n\s+at __randomizedtesting)") {
        $result.Error = $Matches[1].Trim()
    } elseif ($rawOutput -match "(?:exception|error):\s*(.+?)(?:\r?\n\s+at org\.junit)") {
        $result.Error = $Matches[1].Trim()
    }
    
    # Look for specific error patterns in the full output
    # These are the actual ES error messages
    $errorPatterns = @(
        "Found \d+ problems?\s*\r?\n\s*line \d+:[^\r\n]+",
        "Unknown column \[[^\]]+\]",
        "Cannot invoke[^\r\n]+",
        "verification_exception[^\r\n]+",
        "parsing_exception[^\r\n]+",
        "NullPointerException[^\r\n]*",
        "IllegalArgumentException[^\r\n]+"
    )
    
    foreach ($pattern in $errorPatterns) {
        if ($rawOutput -match $pattern) {
            if ($result.Error -notmatch [regex]::Escape($Matches[0])) {
                $result.Error += "`n" + $Matches[0]
            }
        }
    }
    
    # Extract "Found X problems" section with line numbers
    if ($rawOutput -match "(Found \d+ problems?(?:\s*\r?\n\s*line \d+:[^\r\n]+)+)") {
        $result.Error = $Matches[1] -replace "\r?\n\s+", "`n"
    }
    
    # Extract stack trace - look for lines starting with "at " after the error
    $lines = $rawOutput -split "\r?\n"
    $inStackTrace = $false
    $stackTraceCount = 0
    
    foreach ($line in $lines) {
        # Start capturing after we see the test assertion failure
        if ($line -match "^\s+at __randomizedtesting" -or $line -match "^\s+at org\.junit\.Assert") {
            $inStackTrace = $true
        }
        
        if ($inStackTrace -and $line -match "^\s+at\s+") {
            if ($stackTraceCount -lt 20) {
                $result.StackTrace += $line.Trim()
                $stackTraceCount++
            }
        }
    }
    
    # Also try to extract server-side stack trace for 500 errors
    if ($rawOutput -match "stack_trace.+?(org\.elasticsearch[^\r\n]+(?:\r?\n\s+at [^\r\n]+){0,10})") {
        $result.Error += "`n`nServer Stack Trace:`n" + ($Matches[1] -replace "\s+at ", "`nat ")
    }
    
    return $result
}

for ($i = 1; $i -le $totalRuns; $i++) {
    Write-Host "Running test $i of $totalRuns..." -ForegroundColor Cyan
    
    # Snapshot Java PIDs before the run so we only kill new ones afterwards
    $javapidsBefore = @(Get-Process -Name "java" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)
    
    # Run test and capture full output (--no-daemon to avoid daemon accumulation)
    $rawOutput = & .\gradlew.bat ":x-pack:plugin:esql:qa:server:single-node:javaRestTest" --tests "org.elasticsearch.xpack.esql.qa.single_node.GenerativeIT" --console=plain --no-daemon 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
    
    # Kill only Java processes spawned during this run (preserves IDE, etc.)
    $javapidsAfter = @(Get-Process -Name "java" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)
    $newPids = $javapidsAfter | Where-Object { $_ -notin $javapidsBefore }
    if ($newPids.Count -gt 0) {
        Write-Host "  Cleaning up $($newPids.Count) Java process(es) spawned by this run..." -ForegroundColor DarkGray
        $newPids | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
        Start-Sleep -Seconds 2
    }
    
    if ($exitCode -eq 0) {
        $successCount++
        Write-Host "  Test $i passed" -ForegroundColor Green
        continue
    }
    
    # Extract failure details
    $failure = Extract-TestFailure -rawOutput $rawOutput
    
    # Check if we should filter this failure
    if (Should-FilterFailure -query $failure.Query -fullError $failure.Error) {
        $filteredCount++
        Write-Host "  Test $i failed (FILTERED - unmapped field after schema-fixing command)" -ForegroundColor Yellow
        continue
    }
    
    $failureCount++
    
    # Show brief error summary
    $errorSummary = $failure.Error
    if ($errorSummary.Length -gt 100) {
        $errorSummary = $errorSummary.Substring(0, 100) + "..."
    }
    Write-Host "  Test $i FAILED - $errorSummary" -ForegroundColor Red
    
    # Write to output file
    "" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "=" * 80 | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "FAILURE #$failureCount (Run $i)" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "=" * 80 | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "QUERY:" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    $failure.Query | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "ERROR DETAILS:" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    $failure.Error | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "STACK TRACE (first 20 lines):" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    $failure.StackTrace -join "`n" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "" | Out-File -FilePath $outputFile -Append -Encoding UTF8
}

# Write summary
"" | Out-File -FilePath $outputFile -Append -Encoding UTF8
"=" * 80 | Out-File -FilePath $outputFile -Append -Encoding UTF8
"SUMMARY" | Out-File -FilePath $outputFile -Append -Encoding UTF8
"=" * 80 | Out-File -FilePath $outputFile -Append -Encoding UTF8
"Total runs: $totalRuns" | Out-File -FilePath $outputFile -Append -Encoding UTF8
"Successful: $successCount" | Out-File -FilePath $outputFile -Append -Encoding UTF8
"Failed (recorded): $failureCount" | Out-File -FilePath $outputFile -Append -Encoding UTF8
"Failed (filtered - unmapped after schema-fixing): $filteredCount" | Out-File -FilePath $outputFile -Append -Encoding UTF8
"Run completed: $(Get-Date)" | Out-File -FilePath $outputFile -Append -Encoding UTF8

Write-Host ""
Write-Host "=" * 60 -ForegroundColor White
Write-Host "Test run complete!" -ForegroundColor Green
Write-Host "Total runs: $totalRuns"
Write-Host "Successful: $successCount" -ForegroundColor Green
Write-Host "Failed (recorded): $failureCount" -ForegroundColor Red
Write-Host "Failed (filtered): $filteredCount" -ForegroundColor Yellow
Write-Host "Results written to: $outputFile"
Write-Host "=" * 60 -ForegroundColor White
