/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.common.CommandLineParser.ShellType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class CommandLineParserTests extends ESTestCase {

    // ========== ShellType.fromString ==========

    public void testShellTypeFromString() {
        assertThat(ShellType.fromString("bash"), equalTo(ShellType.BASH));
        assertThat(ShellType.fromString("sh"), equalTo(ShellType.BASH));
        assertThat(ShellType.fromString("BASH"), equalTo(ShellType.BASH));
        assertThat(ShellType.fromString("Sh"), equalTo(ShellType.BASH));
        assertThat(ShellType.fromString("cmd"), equalTo(ShellType.CMD));
        assertThat(ShellType.fromString("CMD"), equalTo(ShellType.CMD));
        assertThat(ShellType.fromString("powershell"), equalTo(ShellType.POWERSHELL));
        assertThat(ShellType.fromString("PowerShell"), equalTo(ShellType.POWERSHELL));
    }

    public void testShellTypeFromStringInvalid() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ShellType.fromString("zsh"));
        assertThat(e.getMessage(), equalTo("unsupported shell type [zsh]; supported types are [bash], [cmd], and [powershell]"));
    }

    // ========== Common edge cases ==========

    public void testEmptyString() {
        for (ShellType shell : ShellType.values()) {
            assertThat("shell=" + shell, CommandLineParser.parse("", shell), empty());
        }
    }

    public void testWhitespaceOnly() {
        for (ShellType shell : ShellType.values()) {
            assertThat("shell=" + shell, CommandLineParser.parse("   \t  ", shell), empty());
        }
    }

    // ========== Bash tests ==========

    public void testBashSimpleArgs() {
        assertThat(CommandLineParser.parse("ls -la /tmp", ShellType.BASH), contains("ls", "-la", "/tmp"));
    }

    public void testBashMultipleWhitespace() {
        assertThat(CommandLineParser.parse("  ls   -la   /tmp  ", ShellType.BASH), contains("ls", "-la", "/tmp"));
    }

    public void testBashTabSeparated() {
        assertThat(CommandLineParser.parse("ls\t-la\t/tmp", ShellType.BASH), contains("ls", "-la", "/tmp"));
    }

    public void testBashSingleQuotes() {
        assertThat(CommandLineParser.parse("echo 'hello world'", ShellType.BASH), contains("echo", "hello world"));
    }

    public void testBashSingleQuotesPreserveBackslash() {
        assertThat(CommandLineParser.parse("echo 'hello\\nworld'", ShellType.BASH), contains("echo", "hello\\nworld"));
    }

    public void testBashSingleQuotesPreserveDoubleQuotes() {
        assertThat(CommandLineParser.parse("echo 'he said \"hi\"'", ShellType.BASH), contains("echo", "he said \"hi\""));
    }

    public void testBashDoubleQuotes() {
        assertThat(CommandLineParser.parse("echo \"hello world\"", ShellType.BASH), contains("echo", "hello world"));
    }

    public void testBashDoubleQuotesEscapedQuote() {
        assertThat(CommandLineParser.parse("echo \"he said \\\"hi\\\"\"", ShellType.BASH), contains("echo", "he said \"hi\""));
    }

    public void testBashDoubleQuotesEscapedBackslash() {
        assertThat(CommandLineParser.parse("echo \"path\\\\dir\"", ShellType.BASH), contains("echo", "path\\dir"));
    }

    public void testBashDoubleQuotesEscapedDollar() {
        assertThat(CommandLineParser.parse("echo \"\\$HOME\"", ShellType.BASH), contains("echo", "$HOME"));
    }

    public void testBashDoubleQuotesEscapedBacktick() {
        assertThat(CommandLineParser.parse("echo \"\\`date\\`\"", ShellType.BASH), contains("echo", "`date`"));
    }

    public void testBashDoubleQuotesEscapedExclamation() {
        assertThat(CommandLineParser.parse("echo \"\\!important\"", ShellType.BASH), contains("echo", "!important"));
    }

    public void testBashDoubleQuotesNonSpecialBackslash() {
        // backslash before a non-special character is preserved literally inside double quotes
        assertThat(CommandLineParser.parse("echo \"hello\\nworld\"", ShellType.BASH), contains("echo", "hello\\nworld"));
    }

    public void testBashUnquotedBackslashEscape() {
        assertThat(CommandLineParser.parse("echo hello\\ world", ShellType.BASH), contains("echo", "hello world"));
    }

    public void testBashUnquotedBackslashEscapeQuote() {
        assertThat(CommandLineParser.parse("echo \\\"quoted\\\"", ShellType.BASH), contains("echo", "\"quoted\""));
    }

    public void testBashMixedQuoting() {
        // bash allows concatenation of differently-quoted segments within one argument
        assertThat(CommandLineParser.parse("echo 'hello '\"world\"", ShellType.BASH), contains("echo", "hello world"));
    }

    public void testBashEmptyStringArg() {
        assertThat(CommandLineParser.parse("echo '' \"\"", ShellType.BASH), contains("echo", "", ""));
    }

    public void testBashSingleArg() {
        assertThat(CommandLineParser.parse("ls", ShellType.BASH), contains("ls"));
    }

    public void testBashComplexCommandLine() {
        String cmd = "find /var/log -name '*.log' -exec grep -l \"error\" {} \\;";
        assertThat(
            CommandLineParser.parse(cmd, ShellType.BASH),
            contains("find", "/var/log", "-name", "*.log", "-exec", "grep", "-l", "error", "{}", ";")
        );
    }

    public void testBashPathWithSpaces() {
        assertThat(CommandLineParser.parse("cat \"/path/to/my file.txt\"", ShellType.BASH), contains("cat", "/path/to/my file.txt"));
    }

    public void testBashUnclosedSingleQuote() {
        // unclosed quotes just consume to end of string
        assertThat(CommandLineParser.parse("echo 'hello", ShellType.BASH), contains("echo", "hello"));
    }

    public void testBashUnclosedDoubleQuote() {
        assertThat(CommandLineParser.parse("echo \"hello", ShellType.BASH), contains("echo", "hello"));
    }

    public void testBashTrailingBackslash() {
        // trailing backslash with nothing after it: the backslash is dropped
        assertThat(CommandLineParser.parse("echo test\\", ShellType.BASH), contains("echo", "test"));
    }

    // ========== Cmd (CommandLineToArgvW) tests ==========

    public void testCmdSimpleArgs() {
        assertThat(CommandLineParser.parse("program.exe arg1 arg2", ShellType.CMD), contains("program.exe", "arg1", "arg2"));
    }

    public void testCmdDoubleQuotedSpaces() {
        assertThat(CommandLineParser.parse("\"a b c\" d e", ShellType.CMD), contains("a b c", "d", "e"));
    }

    public void testCmdEscapedQuote() {
        // \" -> literal "
        assertThat(CommandLineParser.parse("ab\\\"c d", ShellType.CMD), contains("ab\"c", "d"));
    }

    public void testCmdQuotedEscapedQuote() {
        // inside quotes: \" still produces literal "
        assertThat(CommandLineParser.parse("\"ab\\\"c\" d", ShellType.CMD), contains("ab\"c", "d"));
    }

    public void testCmdDoubleBackslashBeforeQuote() {
        // \\" -> one \ + end/begin quote
        assertThat(CommandLineParser.parse("\"a b\\\\\" c", ShellType.CMD), contains("a b\\", "c"));
    }

    public void testCmdTripleBackslashBeforeQuote() {
        // \\\" -> one \ + literal "
        assertThat(CommandLineParser.parse("\"a b\\\\\\\"c\" d", ShellType.CMD), contains("a b\\\"c", "d"));
    }

    public void testCmdBackslashNotBeforeQuote() {
        // backslashes not before a quote are literal
        assertThat(CommandLineParser.parse("a\\\\\\b d", ShellType.CMD), contains("a\\\\\\b", "d"));
    }

    public void testCmdQuotedBackslashNotBeforeQuote() {
        assertThat(CommandLineParser.parse("\"a\\\\\\b\" d", ShellType.CMD), contains("a\\\\\\b", "d"));
    }

    public void testCmdDoubleDoubleQuote() {
        // "" inside quotes -> literal "
        assertThat(CommandLineParser.parse("\"a b\"\"c d\" e", ShellType.CMD), contains("a b\"c d", "e"));
    }

    public void testCmdEmptyQuotedArg() {
        assertThat(CommandLineParser.parse("\"\" arg2", ShellType.CMD), contains("", "arg2"));
    }

    public void testCmdMicrosoftExample1() {
        // "a b c" d e -> [a b c, d, e]
        assertThat(CommandLineParser.parse("\"a b c\" d e", ShellType.CMD), contains("a b c", "d", "e"));
    }

    public void testCmdMicrosoftExample2() {
        // "ab\"c" "\\" d -> [ab"c, \, d]
        assertThat(CommandLineParser.parse("\"ab\\\"c\" \"\\\\\" d", ShellType.CMD), contains("ab\"c", "\\", "d"));
    }

    public void testCmdMicrosoftExample3() {
        // a\\\b d"e f"g h -> [a\\\b, de fg, h]
        assertThat(CommandLineParser.parse("a\\\\\\b d\"e f\"g h", ShellType.CMD), contains("a\\\\\\b", "de fg", "h"));
    }

    public void testCmdMicrosoftExample4() {
        // a\\\"b c d -> [a\"b, c, d]
        assertThat(CommandLineParser.parse("a\\\\\\\"b c d", ShellType.CMD), contains("a\\\"b", "c", "d"));
    }

    public void testCmdMicrosoftExample5() {
        // a\\\\"b c" d e -> [a\\b c, d, e]
        assertThat(CommandLineParser.parse("a\\\\\\\\\"b c\" d e", ShellType.CMD), contains("a\\\\b c", "d", "e"));
    }

    public void testCmdWindowsPath() {
        assertThat(
            CommandLineParser.parse("\"C:\\Program Files\\app.exe\" --config \"C:\\my config\\file.ini\"", ShellType.CMD),
            contains("C:\\Program Files\\app.exe", "--config", "C:\\my config\\file.ini")
        );
    }

    public void testCmdTrailingBackslash() {
        assertThat(CommandLineParser.parse("\"C:\\TEST A\\\\\" b", ShellType.CMD), contains("C:\\TEST A\\", "b"));
    }

    public void testCmdMultipleWhitespace() {
        assertThat(CommandLineParser.parse("  a   b   c  ", ShellType.CMD), contains("a", "b", "c"));
    }

    // ========== PowerShell tests ==========

    public void testPowerShellSimpleArgs() {
        assertThat(CommandLineParser.parse("Get-Process -Name svchost", ShellType.POWERSHELL), contains("Get-Process", "-Name", "svchost"));
    }

    public void testPowerShellDoubleQuoted() {
        assertThat(CommandLineParser.parse("Write-Host \"hello world\"", ShellType.POWERSHELL), contains("Write-Host", "hello world"));
    }

    public void testPowerShellSingleQuoted() {
        assertThat(CommandLineParser.parse("Write-Host 'hello world'", ShellType.POWERSHELL), contains("Write-Host", "hello world"));
    }

    public void testPowerShellBacktickEscapeInDoubleQuotes() {
        assertThat(CommandLineParser.parse("\"hello`nworld\"", ShellType.POWERSHELL), contains("hello\nworld"));
    }

    public void testPowerShellBacktickEscapeTab() {
        assertThat(CommandLineParser.parse("\"col1`tcol2\"", ShellType.POWERSHELL), contains("col1\tcol2"));
    }

    public void testPowerShellBacktickEscapeOutsideQuotes() {
        assertThat(CommandLineParser.parse("hello` world", ShellType.POWERSHELL), contains("hello world"));
    }

    public void testPowerShellBacktickEscapeQuote() {
        assertThat(CommandLineParser.parse("\"he said `\"hi`\"\"", ShellType.POWERSHELL), contains("he said \"hi\""));
    }

    public void testPowerShellDoubleDoubleQuoteInDoubleQuotes() {
        assertThat(CommandLineParser.parse("\"he said \"\"hi\"\"\"", ShellType.POWERSHELL), contains("he said \"hi\""));
    }

    public void testPowerShellDoubleSingleQuoteInSingleQuotes() {
        assertThat(CommandLineParser.parse("'it''s working'", ShellType.POWERSHELL), contains("it's working"));
    }

    public void testPowerShellSingleQuotesPreserveBacktick() {
        // backtick is not special inside single quotes
        assertThat(CommandLineParser.parse("'hello`nworld'", ShellType.POWERSHELL), contains("hello`nworld"));
    }

    public void testPowerShellEmptyStringArgs() {
        assertThat(CommandLineParser.parse("echo '' \"\"", ShellType.POWERSHELL), contains("echo", "", ""));
    }

    public void testPowerShellBacktickEscapeZero() {
        assertThat(CommandLineParser.parse("\"`0\"", ShellType.POWERSHELL), contains("\0"));
    }

    public void testPowerShellBacktickNonSpecial() {
        // backtick before a non-special character just produces that character
        assertThat(CommandLineParser.parse("`x", ShellType.POWERSHELL), contains("x"));
    }

    public void testPowerShellMixedQuotes() {
        assertThat(CommandLineParser.parse("'hello '\"world\"", ShellType.POWERSHELL), contains("hello world"));
    }

    public void testPowerShellComplexCommand() {
        String cmd = "powershell.exe -NoProfile -Command \"Get-ChildItem 'C:\\Program Files'\"";
        assertThat(
            CommandLineParser.parse(cmd, ShellType.POWERSHELL),
            contains("powershell.exe", "-NoProfile", "-Command", "Get-ChildItem 'C:\\Program Files'")
        );
    }

    public void testPowerShellUnclosedDoubleQuote() {
        assertThat(CommandLineParser.parse("echo \"hello", ShellType.POWERSHELL), contains("echo", "hello"));
    }

    public void testPowerShellUnclosedSingleQuote() {
        assertThat(CommandLineParser.parse("echo 'hello", ShellType.POWERSHELL), contains("echo", "hello"));
    }

    // ========== Realistic security event command lines ==========

    public void testBashSysmonCommandLine() {
        String cmd = "/usr/bin/python3 /opt/agent/monitor.py --config '/etc/agent/config.yml' --log-level debug";
        assertThat(
            CommandLineParser.parse(cmd, ShellType.BASH),
            contains("/usr/bin/python3", "/opt/agent/monitor.py", "--config", "/etc/agent/config.yml", "--log-level", "debug")
        );
    }

    public void testCmdWindowsServiceCommandLine() {
        String cmd = "\"C:\\Program Files\\Defender\\MsMpEng.exe\" /runservice /scheduledscan /scantype=2";
        assertThat(
            CommandLineParser.parse(cmd, ShellType.CMD),
            contains("C:\\Program Files\\Defender\\MsMpEng.exe", "/runservice", "/scheduledscan", "/scantype=2")
        );
    }

    public void testPowerShellEncodedCommandLine() {
        String cmd = "powershell.exe -ExecutionPolicy Bypass -NoProfile -EncodedCommand ZQBjAGgAbwAgACIAaABlAGwAbABvACIA";
        assertThat(
            CommandLineParser.parse(cmd, ShellType.POWERSHELL),
            contains("powershell.exe", "-ExecutionPolicy", "Bypass", "-NoProfile", "-EncodedCommand", "ZQBjAGgAbwAgACIAaABlAGwAbABvACIA")
        );
    }

    public void testCmdNetCommand() {
        String cmd = "net user \"John Doe\" P@ssw0rd /add /domain";
        assertThat(CommandLineParser.parse(cmd, ShellType.CMD), contains("net", "user", "John Doe", "P@ssw0rd", "/add", "/domain"));
    }
}
