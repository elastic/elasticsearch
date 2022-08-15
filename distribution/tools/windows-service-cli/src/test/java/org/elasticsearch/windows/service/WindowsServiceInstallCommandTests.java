/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

import org.elasticsearch.Version;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;

public class WindowsServiceInstallCommandTests extends WindowsServiceCliTestCase {

    Path jvmDll;

    public WindowsServiceInstallCommandTests(boolean spaceInPath) {
        super(spaceInPath);
    }

    @Before
    public void setupJvm() throws Exception {
        jvmDll = javaHome.resolve("jre/bin/server/jvm.dll");
        Files.createDirectories(jvmDll.getParent());
        Files.createFile(jvmDll);
        sysprops.put("java.class.path", "javaclasspath");
        envVars.put("COMPUTERNAME", "mycomputer");
    }

    @Override
    protected Command newCommand() {
        return new WindowsServiceInstallCommand() {
            @Override
            Process startProcess(ProcessBuilder processBuilder) throws IOException {
                return mockProcess(processBuilder);
            }
        };
    }

    @Override
    protected String getCommand() {
        return "IS";
    }

    @Override
    protected String getDefaultSuccessMessage() {
        return "The service 'elasticsearch-service-x64' has been installed";
    }

    @Override
    protected String getDefaultFailureMessage() {
        return "Failed installing 'elasticsearch-service-x64' service";
    }

    public void testDllMissing() throws Exception {
        Files.delete(jvmDll);
        assertThat(executeMain(), equalTo(ExitCodes.CONFIG));
        assertThat(terminal.getErrorOutput(), containsString("Invalid java installation (no jvm.dll"));
    }

    public void testAlternateDllLocation() throws Exception {
        Files.delete(jvmDll);
        Path altJvmDll = javaHome.resolve("bin/server/jvm.dll");
        Files.createDirectories(altJvmDll.getParent());
        Files.createFile(altJvmDll);
        assertServiceArgs(Map.of());
    }

    public void testDll() throws Exception {
        assertServiceArgs(Map.of("Jvm", quote(jvmDll.toString())));
    }

    public void testPreExecuteOutput() throws Exception {
        envVars.put("SERVICE_ID", "myservice");
        assertOkWithOutput(
            allOf(containsString("Installing service : myservice"), containsString("Using ES_JAVA_HOME : " + javaHome)),
            emptyString()
        );
    }

    public void testJvmOptions() throws Exception {
        sysprops.put("es.distribution.type", "testdistro");
        List<String> expectedOptions = List.of(
            "" + "-XX:+UseSerialGC",
            "-Des.path.home=" + quote(esHomeDir.toString()),
            "-Des.path.conf=" + quote(esHomeDir.resolve("config").toString()),
            "-Des.distribution.type=" + quote("testdistro")
        );
        mockProcessValidator = (environment, procrunCall) -> {
            List<String> options = procrunCall.args().get("JvmOptions");
            assertThat(
                options,
                containsInAnyOrder(
                    "-Dcli.name=windows-service-daemon",
                    "-Dcli.libs=lib/tools/server-cli,lib/tools/windows-service-cli",
                    String.join(";", expectedOptions)
                )
            );
        };
        assertOkWithOutput(any(String.class), emptyString());
    }

    public void testStartupType() throws Exception {
        assertServiceArgs(Map.of("Startup", "manual"));
        envVars.put("ES_START_TYPE", "auto");
        assertServiceArgs(Map.of("Startup", "auto"));
    }

    public void testStopTimeout() throws Exception {
        assertServiceArgs(Map.of("StopTimeout", "0"));
        envVars.put("ES_STOP_TIMEOUT", "5");
        assertServiceArgs(Map.of("StopTimeout", "5"));
    }

    public void testFixedArgs() throws Exception {
        assertServiceArgs(
            Map.ofEntries(
                entry("StartClass", "org.elasticsearch.launcher.CliToolLauncher"),
                entry("StartMethod", "main"),
                entry("StartMode", "jvm"),
                entry("StopClass", "org.elasticsearch.launcher.CliToolLauncher"),
                entry("StopMethod", "close"),
                entry("StopMode", "jvm"),
                entry("JvmMs", "4m"),
                entry("JvmMx", "64m"),
                entry("StartPath", quote(esHomeDir.toString())),
                entry("Classpath", "javaclasspath") // dummy value for tests
            )
        );
    }

    public void testPidFile() throws Exception {
        assertServiceArgs(Map.of("PidFile", "elasticsearch-service-x64.pid"));
        envVars.put("SERVICE_ID", "myservice");
        assertServiceArgs(Map.of("PidFile", "myservice.pid"));
    }

    public void testDisplayName() throws Exception {
        assertServiceArgs(Map.of("DisplayName", "\"Elasticsearch %s (elasticsearch-service-x64)\"".formatted(Version.CURRENT)));
        envVars.put("SERVICE_DISPLAY_NAME", "my service name");
        assertServiceArgs(Map.of("DisplayName", "\"my service name\""));
    }

    public void testDescription() throws Exception {
        String defaultDescription = "\"Elasticsearch %s Windows Service - https://elastic.co\"".formatted(Version.CURRENT);
        assertServiceArgs(Map.of("Description", defaultDescription));
        envVars.put("SERVICE_DESCRIPTION", "my description");
        assertServiceArgs(Map.of("Description", "\"my description\""));
    }

    public void testUsernamePassword() throws Exception {
        assertServiceArgs(Map.of("ServiceUser", "LocalSystem"));

        terminal.reset();
        envVars.put("SERVICE_USERNAME", "myuser");
        assertThat(executeMain(), equalTo(ExitCodes.CONFIG));
        assertThat(terminal.getErrorOutput(), containsString("Both service username and password must be set"));

        terminal.reset();
        envVars.remove("SERVICE_USERNAME");
        envVars.put("SERVICE_PASSWORD", "mypassword");
        assertThat(executeMain(), equalTo(ExitCodes.CONFIG));
        assertThat(terminal.getErrorOutput(), containsString("Both service username and password must be set"));

        terminal.reset();
        envVars.put("SERVICE_USERNAME", "myuser");
        envVars.put("SERVICE_PASSWORD", "mypassword");
        assertServiceArgs(Map.of("ServiceUser", "myuser", "ServicePassword", "mypassword"));
    }

    public void testExtraServiceParams() throws Exception {
        envVars.put("SERVICE_PARAMS", "--MyExtraArg \"and value\"");
        assertServiceArgs(Map.of("MyExtraArg", "\"and value\""));
    }
}
