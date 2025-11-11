/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static com.carrotsearch.randomizedtesting.RandomizedTest.assumeTrue;
import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.copyDirectory;
import static org.elasticsearch.packaging.util.FileUtils.mv;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class WindowsServiceTests extends PackagingTestCase {

    private static final String DEFAULT_ID = "elasticsearch-service-x64";
    private static final String DEFAULT_DISPLAY_NAME = "Elasticsearch " + FileUtils.getCurrentVersion() + " (elasticsearch-service-x64)";
    private static String serviceScript;

    @BeforeClass
    public static void ensureWindows() {
        assumeTrue(Platforms.WINDOWS);
        assumeTrue(distribution().hasJdk);
    }

    @After
    public void uninstallService() {
        sh.runIgnoreExitCode(deleteCommand(DEFAULT_ID));
    }

    private static String startCommand(String serviceId) {
        return "\"sc.exe start " + serviceId + "\"";
    }

    private static String stopCommand(String serviceId) {
        return "\"sc.exe stop " + serviceId + "\"";
    }

    private static String deleteCommand(String serviceId) {
        return "\"sc.exe delete " + serviceId + "\"";
    }

    private void assertService(String id, String status) {
        Result result = sh.run("Get-Service " + id + " | Format-List -Property Name, Status, DisplayName");
        assertThat(result.stdout(), containsString("Name        : " + id));
        assertThat(result.stdout(), containsString("Status      : " + status));
    }

    private void waitForStop(String id, Duration timeout) {
        var stopped = false;
        var start = System.currentTimeMillis();
        while (stopped == false) {
            Result result = sh.run("(Get-Service  " + id + " ).\"Status\"");
            if (result.exitCode() != 0) {
                logger.warn("Cannot get status for {}: stdout:[{}], stderr:[{}]", id, result.stdout(), result.stderr());
                break;
            }
            stopped = "Stopped".equalsIgnoreCase(result.stdout());
            Duration elapsed = Duration.ofMillis(System.currentTimeMillis() - start);
            if (elapsed.compareTo(timeout) > 0) {
                logger.warn("Timeout waiting for stop {}: stdout:[{}], stderr:[{}]", id, result.stdout(), result.stderr());
                break;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    // runs the service command, dumping all log files on failure
    private Result assertCommand(String script) {
        Result result = sh.runIgnoreExitCode(script);
        assertExit(result, script, 0);
        return result;
    }

    private Result assertFailure(String script, int exitCode) {
        Result result = sh.runIgnoreExitCode(script);
        assertExit(result, script, exitCode);
        return result;
    }

    @Override
    protected void dumpDebug() {
        super.dumpDebug();
        dumpServiceLogs();
    }

    private void dumpServiceLogs() {
        logger.warn("\n");
        try (var logsDir = Files.list(installation.logs)) {
            for (Path logFile : logsDir.toList()) {
                String filename = logFile.getFileName().toString();
                if (filename.startsWith("elasticsearch-service-x64")) {
                    logger.warn(filename + "\n" + FileUtils.slurp(logFile));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void assertExit(Result result, String script, int exitCode) {
        if (result.exitCode() != exitCode) {
            logger.error("---- Unexpected exit code (expected " + exitCode + ", got " + result.exitCode() + ") for script: " + script);
            logger.error(result);
            logger.error("Dumping log files\n");
            dumpDebug();
            fail();
        } else {
            logger.info("\nscript: " + script + "\nstdout: " + result.stdout() + "\nstderr: " + result.stderr());
        }
    }

    public void test10InstallArchive() throws Exception {
        installation = installArchive(sh, distribution());
        verifyArchiveInstallation(installation, distribution());
        setFileSuperuser("test_superuser", "test_superuser_password");
        serviceScript = installation.bin("elasticsearch-service.bat").toString();
    }

    public void test12InstallService() {
        sh.run(serviceScript + " install");
        assertService(DEFAULT_ID, "Stopped");
        sh.run(deleteCommand(DEFAULT_ID));
    }

    public void test15RemoveNotInstalled() {
        Result result = assertFailure(deleteCommand(DEFAULT_ID), 1);
        assertThat(result.stdout(), containsString("The specified service does not exist as an installed service"));
    }

    public void test16InstallSpecialCharactersInJdkPath() throws IOException {
        assumeTrue("Only run this test when we know where the JDK is.", distribution().hasJdk);
        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("a (special) jdk");
        sh.getEnv().put("ES_JAVA_HOME", relocatedJdk.toString());

        try {
            mv(installation.bundledJdk, relocatedJdk);
            Result result = sh.run(serviceScript + " install");
            assertThat(result.stdout(), containsString("The service 'elasticsearch-service-x64' has been installed"));
        } finally {
            sh.runIgnoreExitCode(deleteCommand(DEFAULT_ID));
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test20CustomizeServiceId() {
        String serviceId = "my-es-service";
        try {
            sh.getEnv().put("SERVICE_ID", serviceId);
            sh.run(serviceScript + " install");
            assertService(serviceId, "Stopped");
        } finally {
            sh.run(deleteCommand(serviceId));
        }
    }

    public void test21CustomizeServiceDisplayName() {
        String displayName = "my es service display name";
        sh.getEnv().put("SERVICE_DISPLAY_NAME", displayName);
        sh.run(serviceScript + " install");
        assertService(DEFAULT_ID, "Stopped");
        sh.run(deleteCommand(DEFAULT_ID));
    }

    // NOTE: service description is not attainable through any powershell api, so checking it is not possible...
    public void assertStartedAndStop() throws Exception {
        ServerUtils.waitForElasticsearch(installation);
        runElasticsearchTests();

        assertCommand(stopCommand(DEFAULT_ID));
        waitForStop(DEFAULT_ID, Duration.ofMinutes(1));
        assertService(DEFAULT_ID, "Stopped");
        // the process is stopped async, and can become a zombie process, so we poll for the process actually being gone
        assertCommand(
            "$p = Get-Service -Name \"elasticsearch-service-x64\" -ErrorAction SilentlyContinue;"
                + "$i = 0;"
                + "do {"
                + "  $p = Get-Process -Name \"elasticsearch-service-x64\" -ErrorAction SilentlyContinue;"
                + "  echo \"$p\";"
                + "  if ($p -eq $Null) {"
                + "    Write-Host \"exited after $i seconds\";"
                + "    exit 0;"
                + "  }"
                + "  Start-Sleep -Seconds 1;"
                + "  $i += 1;"
                + "} while ($i -lt 300);"
                + "exit 9;"
        );
    }

    public void test30StartStop() throws Exception {
        sh.run(serviceScript + " install");
        assertCommand(startCommand(DEFAULT_ID));
        assertStartedAndStop();
    }

    public void test31StartNotInstalled() throws IOException {
        Result result = sh.runIgnoreExitCode(startCommand(DEFAULT_ID));
        assertThat(result.stderr(), result.exitCode(), equalTo(1));
        dumpServiceLogs();
        assertThat(result.stdout(), containsString("The specified service does not exist as an installed service"));
    }

    public void test32StopNotStarted() throws IOException {
        sh.run(serviceScript + " install");
        Result result = sh.runIgnoreExitCode(stopCommand(DEFAULT_ID));
        assertThat(result.stdout(), containsString("The service has not been started"));
    }

    public void test33JavaChanged() throws Exception {
        final Path alternateJdk = installation.bundledJdk.getParent().resolve("jdk.copy");

        try {
            copyDirectory(installation.bundledJdk, alternateJdk);
            sh.getEnv().put("ES_JAVA_HOME", alternateJdk.toString());
            assertCommand(serviceScript + " install");
            sh.getEnv().remove("ES_JAVA_HOME");
            assertCommand(startCommand(DEFAULT_ID));
            assertStartedAndStop();
        } finally {
            FileUtils.rm(alternateJdk);
        }
    }

    public void test80JavaOptsInEnvVar() throws Exception {
        sh.getEnv().put("ES_JAVA_OPTS", "-Xmx2g -Xms2g");
        sh.run(serviceScript + " install");
        assertCommand(startCommand(DEFAULT_ID));
        assertStartedAndStop();
        sh.getEnv().remove("ES_JAVA_OPTS");
    }

    public void test81JavaOptsInJvmOptions() throws Exception {
        withCustomConfig(tempConf -> {
            append(tempConf.resolve("jvm.options"), "-Xmx2g" + System.lineSeparator());
            append(tempConf.resolve("jvm.options"), "-Xms2g" + System.lineSeparator());
            sh.run(serviceScript + " install");
            assertCommand(startCommand(DEFAULT_ID));
            assertStartedAndStop();
        });
    }

    // TODO:
    // custom SERVICE_USERNAME/SERVICE_PASSWORD
    // custom SERVICE_LOG_DIR
    // custom LOG_OPTS (looks like it currently conflicts with setting custom log dir)
    // install and run java opts Xmx/s (each data size type)
}
