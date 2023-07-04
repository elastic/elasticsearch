/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cli.CommandTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public abstract class WindowsServiceCliTestCase extends CommandTestCase {

    Path javaHome;
    Path binDir;
    Path serviceExe;
    Path mgrExe;
    int mockProcessExit = 0;
    ProcessValidator mockProcessValidator = null;

    @ParametersFactory
    public static Iterable<Object[]> spaceInPathProvider() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    protected WindowsServiceCliTestCase(boolean spaceInPath) {
        super(spaceInPath);
    }

    interface ProcessValidator {
        void validate(Map<String, String> env, ProcrunCall procrunCall);
    }

    record ProcrunCall(String exe, String command, String serviceId, Map<String, List<String>> args) {}

    class MockProcess extends Process {

        @Override
        public OutputStream getOutputStream() {
            throw new AssertionError("should not access output stream");
        }

        @Override
        public InputStream getInputStream() {
            throw new AssertionError("should not access input stream");
        }

        @Override
        public InputStream getErrorStream() {
            throw new AssertionError("should not access error stream");
        }

        @Override
        public int waitFor() {
            return mockProcessExit;
        }

        @Override
        public int exitValue() {
            return mockProcessExit;
        }

        @Override
        public void destroy() {
            throw new AssertionError("should not kill procrun process");
        }
    }

    protected Process mockProcess(ProcessBuilder processBuilder) throws IOException {
        assertThat(processBuilder.redirectInput(), equalTo(INHERIT));
        assertThat(processBuilder.redirectOutput(), equalTo(INHERIT));
        assertThat(processBuilder.redirectError(), equalTo(INHERIT));
        if (mockProcessValidator != null) {
            var fullCommand = processBuilder.command();
            assertThat(fullCommand, hasSize(3));
            assertThat(fullCommand.get(0), equalTo("cmd.exe"));
            assertThat(fullCommand.get(1), equalTo("/C"));
            ProcrunCall procrunCall = parseProcrunCall(fullCommand.get(2));
            mockProcessValidator.validate(processBuilder.environment(), procrunCall);
        }
        return new MockProcess();
    }

    // args could have spaces in them, so splitting on string alone is not enough
    // instead we look for the next --Foo and reconstitute the argument following it
    private static final Pattern commandPattern = Pattern.compile("//([A-Z]{2})/([\\w-]+)");

    private static ProcrunCall parseProcrunCall(String unparsedArgs) {
        // command/exe is quoted
        assert unparsedArgs.charAt(0) == '"';
        int idx = unparsedArgs.indexOf('"', 1);
        String exe = unparsedArgs.substring(0, idx + 1);
        // Strip the leading command/exe from the args
        unparsedArgs = unparsedArgs.substring(idx + 1).stripLeading();

        String[] splitArgs = unparsedArgs.split(" ");
        assertThat(unparsedArgs, splitArgs.length, greaterThanOrEqualTo(1));
        Map<String, List<String>> args = new HashMap<>();
        Matcher commandMatcher = commandPattern.matcher(splitArgs[0]);
        assertThat(splitArgs[0], commandMatcher.matches(), is(true));
        String command = commandMatcher.group(1);
        String serviceId = commandMatcher.group(2);

        int i = 1;
        while (i < splitArgs.length) {
            String arg = splitArgs[i];
            assertThat("procrun args begin with -- or ++", arg, anyOf(startsWith("--"), startsWith("++")));
            ++i;
            assertThat("missing value for arg " + arg, i, lessThan(splitArgs.length));

            List<String> argValue = new ArrayList<>();
            while (i < splitArgs.length && splitArgs[i].startsWith("--") == false && splitArgs[i].startsWith("++") == false) {
                argValue.add(splitArgs[i++]);
            }

            String key = arg.substring(2);
            args.compute(key, (k, value) -> {
                if (arg.startsWith("--")) {
                    assertThat("overwriting existing arg: " + key, value, nullValue());
                }
                if (value == null) {
                    // could be ++ implicitly creating new list, or -- above
                    value = new ArrayList<>();
                }
                value.add(String.join(" ", argValue));
                return value;
            });
        }

        return new ProcrunCall(exe, command, serviceId, args);
    }

    @Before
    public void resetMockProcess() throws Exception {
        javaHome = createTempDir();
        Path javaBin = javaHome.resolve("bin");
        sysprops.put("java.home", javaHome.toString());
        binDir = esHomeDir.resolve("bin");
        Files.createDirectories(binDir);
        serviceExe = binDir.resolve("elasticsearch-service-x64.exe");
        Files.createFile(serviceExe);
        mgrExe = binDir.resolve("elasticsearch-service-mgr.exe");
        Files.createFile(mgrExe);
        mockProcessExit = 0;
        mockProcessValidator = null;
    }

    protected abstract String getCommand();

    protected abstract String getDefaultSuccessMessage();

    protected abstract String getDefaultFailureMessage();

    static String quote(String s) {
        return '"' + s + '"';
    }

    protected String getExe() {
        return quote(serviceExe.toString());
    }

    protected boolean includeLogsArgs() {
        return true;
    }

    public void testDefaultCommand() throws Exception {
        mockProcessValidator = (environment, procrunCall) -> {
            assertThat(procrunCall.exe, equalTo(getExe()));
            assertThat(procrunCall.command, equalTo(getCommand()));
            assertThat(procrunCall.serviceId, equalTo("elasticsearch-service-x64"));
            if (includeLogsArgs()) {
                assertThat(procrunCall.args, hasKey("LogPath"));
            } else {
                assertThat(procrunCall.args, not(hasKey("LogPath")));
            }
        };
        assertOkWithOutput(containsString(getDefaultSuccessMessage()), emptyString());
    }

    public void testFailure() throws Exception {
        mockProcessExit = 5;
        assertThat(executeMain(), equalTo(5));
        assertThat(terminal.getErrorOutput(), containsString(getDefaultFailureMessage()));
    }

    // for single value args
    protected void assertServiceArgs(Map<String, String> expectedArgs) throws Exception {
        mockProcessValidator = (environment, procrunCall) -> {
            for (var expected : expectedArgs.entrySet()) {
                List<String> value = procrunCall.args.get(expected.getKey());
                assertThat("missing arg " + expected.getKey(), value, notNullValue());
                assertThat(value.toString(), value, hasSize(1));
                assertThat(value.get(0), equalTo(expected.getValue()));
            }
        };
        assertOkWithOutput(containsString(getDefaultSuccessMessage()), emptyString());
    }
}
