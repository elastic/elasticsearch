# See here: https://github.com/buildkite/agent/issues/2202
# Background processes after the buildkite step script finishes causes the job to hang.
# We can't really programmatically find and kill all of the descendent processes, because they are orphaned. There's no tree to follow.
# So, as a hopefully temporary workaround, we send two ctrl+c signals to the buildkite-agent process.
# This causes the agent process to gracefully shut down. It still runs the post-command hook, and displays the correct exit status.

$ProcessID = (Get-Process | Where {$_.ProcessName -Like "buildkite-agent*"} | Select -first 1).id
$encodedCommand = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes("Add-Type -Names 'w' -Name 'k' -M '[DllImport(""kernel32.dll"")]public static extern bool FreeConsole();[DllImport(""kernel32.dll"")]public static extern bool AttachConsole(uint p);[DllImport(""kernel32.dll"")]public static extern bool SetConsoleCtrlHandler(uint h, bool a);[DllImport(""kernel32.dll"")]public static extern bool GenerateConsoleCtrlEvent(uint e, uint p);public static void SendCtrlC(uint p){FreeConsole();AttachConsole(p);GenerateConsoleCtrlEvent(0, 0);}';[w.k]::SendCtrlC($ProcessID)"))
start-process powershell.exe -argument "-nologo -noprofile -executionpolicy bypass -EncodedCommand $encodedCommand"
Start-Sleep -s 5 # If these signals are back-to-back, buildkite-agent doesn't register them sometimes. This could probably be 1s, but lets do 5 to be safe
start-process powershell.exe -argument "-nologo -noprofile -executionpolicy bypass -EncodedCommand $encodedCommand"
