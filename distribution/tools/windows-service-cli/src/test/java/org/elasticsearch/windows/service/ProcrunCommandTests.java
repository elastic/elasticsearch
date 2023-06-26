/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;

public class ProcrunCommandTests extends WindowsServiceCliTestCase {

    public ProcrunCommandTests(boolean spaceInPath) {
        super(spaceInPath);
    }

    PreExecuteHook preExecuteHook;
    boolean includeLogArgs;
    String additionalArgs;
    String serviceId;

    interface PreExecuteHook {
        void preExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws UserException;
    }

    @Before
    public void resetArgs() {
        serviceId = "elasticsearch-service-x64";
        preExecuteHook = null;
        includeLogArgs = false;
        additionalArgs = "";
    }

    class TestProcrunCommand extends ProcrunCommand {

        protected TestProcrunCommand() {
            super("test command", "DC");
        }

        @Override
        protected void preExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws UserException {
            if (preExecuteHook != null) {
                preExecuteHook.preExecute(terminal, pinfo, serviceId);
            }
        }

        protected String getAdditionalArgs(String serviceId, ProcessInfo processInfo) {
            return additionalArgs;
        }

        @Override
        protected boolean includeLogArgs() {
            return includeLogArgs;
        }

        @Override
        protected String getSuccessMessage(String serviceId) {
            return "success message for " + serviceId;
        }

        @Override
        protected String getFailureMessage(String serviceId) {
            return "failure message for " + serviceId;
        }

        @Override
        Process startProcess(ProcessBuilder processBuilder) throws IOException {
            return mockProcess(processBuilder);
        }
    }

    @Override
    protected Command newCommand() {
        return new TestProcrunCommand();
    }

    @Override
    protected boolean includeLogsArgs() {
        return includeLogArgs;
    }

    @Override
    protected String getCommand() {
        return "DC";
    }

    @Override
    protected String getDefaultSuccessMessage() {
        return "success message for " + serviceId;
    }

    @Override
    protected String getDefaultFailureMessage() {
        return "failure message for " + serviceId;
    }

    public void testMissingExe() throws Exception {
        Files.delete(serviceExe);
        var e = expectThrows(IllegalStateException.class, () -> executeMain("install"));
        assertThat(e.getMessage(), containsString("Missing procrun exe"));
    }

    public void testServiceId() throws Exception {
        assertUsage(containsString("too many arguments"), "servicename", "servicename");
        terminal.reset();
        preExecuteHook = (terminal, pinfo, serviceId) -> assertThat(serviceId, equalTo("my-service-id"));
        assertOkWithOutput(containsString("success"), emptyString(), "my-service-id");
        terminal.reset();
        envVars.put("SERVICE_ID", "my-service-id");
        assertOkWithOutput(containsString("success"), emptyString());
    }

    public void testPreExecuteError() throws Exception {
        preExecuteHook = (terminal, pinfo, serviceId) -> { throw new UserException(ExitCodes.USAGE, "validation error"); };
        assertUsage(containsString("validation error"));
    }

    void assertLogArgs(Map<String, String> logArgs) throws Exception {
        terminal.reset();
        includeLogArgs = true;
        assertServiceArgs(logArgs);
    }

    public void testDefaultLogArgs() throws Exception {
        String logsDir = esHomeDir.resolve("logs").toString();
        assertLogArgs(
            Map.of("LogPath", "\"" + logsDir + "\"", "LogPrefix", "\"elasticsearch-service-x64\"", "StdError", "auto", "StdOutput", "auto")
        );
    }

    public void testLogOpts() throws Exception {
        envVars.put("LOG_OPTS", "--LogPath custom");
        assertLogArgs(Map.of("LogPath", "custom"));
    }

    public void testLogDir() throws Exception {
        envVars.put("SERVICE_LOG_DIR", "mylogdir");
        assertLogArgs(Map.of("LogPath", "\"mylogdir\""));
    }

    public void testLogPrefix() throws Exception {
        serviceId = "myservice";
        envVars.put("SERVICE_ID", "myservice");
        assertLogArgs(Map.of("LogPrefix", "\"myservice\""));
    }

    public void testAdditionalArgs() throws Exception {
        additionalArgs = "--Foo bar";
        assertServiceArgs(Map.of("Foo", "bar"));
    }
}
