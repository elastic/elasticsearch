/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import junit.framework.TestCase;

import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static com.carrotsearch.randomizedtesting.RandomizedTest.assumeTrue;
import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileUtils.append;
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
        sh.runIgnoreExitCode(serviceScript + " remove");
    }

    private void assertService(String id, String status, String displayName) {
        Result result = sh.run("Get-Service " + id + " | Format-List -Property Name, Status, DisplayName");
        assertThat(result.stdout(), containsString("Name        : " + id));
        assertThat(result.stdout(), containsString("Status      : " + status));
        assertThat(result.stdout(), containsString("DisplayName : " + displayName));
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

    private void assertExit(Result result, String script, int exitCode) {
        if (result.exitCode() != exitCode) {
            logger.error("---- Unexpected exit code (expected " + exitCode + ", got " + result.exitCode() + ") for script: " + script);
            logger.error(result);
            logger.error("Dumping log files\n");
            Result logs = sh.run(
                "$files = Get-ChildItem \""
                    + installation.logs
                    + "\\elasticsearch.log\"; "
                    + "Write-Output $files; "
                    + "foreach ($file in $files) {"
                    + "    Write-Output \"$file\"; "
                    + "    Get-Content \"$file\" "
                    + "}"
            );
            logger.error(logs.stdout());
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

    public void test11InstallServiceExeMissing() throws IOException {
        Path serviceExe = installation.bin("elasticsearch-service-x64.exe");
        Path tmpServiceExe = serviceExe.getParent().resolve(serviceExe.getFileName() + ".tmp");
        Files.move(serviceExe, tmpServiceExe);
        Result result = sh.runIgnoreExitCode(serviceScript + " install");
        assertThat(result.exitCode(), equalTo(1));
        assertThat(result.stdout(), containsString("elasticsearch-service-x64.exe was not found..."));
        Files.move(tmpServiceExe, serviceExe);
    }

    public void test12InstallService() {
        sh.run(serviceScript + " install");
        assertService(DEFAULT_ID, "Stopped", DEFAULT_DISPLAY_NAME);
        sh.run(serviceScript + " remove");
    }

    public void test13InstallMissingBundledJdk() throws IOException {
        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");

        try {
            mv(installation.bundledJdk, relocatedJdk);
            Result result = sh.runIgnoreExitCode(serviceScript + " install");
            assertThat(result.exitCode(), equalTo(1));
            assertThat(result.stderr(), containsString("could not find java in bundled JDK"));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test14InstallBadJavaHome() throws IOException {
        sh.getEnv().put("ES_JAVA_HOME", "doesnotexist");
        Result result = sh.runIgnoreExitCode(serviceScript + " install");
        assertThat(result.exitCode(), equalTo(1));
        assertThat(result.stderr(), containsString("could not find java in ES_JAVA_HOME"));
    }

    public void test15RemoveNotInstalled() {
        Result result = assertFailure(serviceScript + " remove", 1);
        assertThat(result.stdout(), containsString("Failed removing '" + DEFAULT_ID + "' service"));
    }

    public void test16InstallSpecialCharactersInJdkPath() throws IOException {
        assumeTrue("Only run this test when we know where the JDK is.", distribution().hasJdk);
        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("a (special) jdk");
        sh.getEnv().put("ES_JAVA_HOME", relocatedJdk.toString());

        try {
            mv(installation.bundledJdk, relocatedJdk);
            Result result = sh.run(serviceScript + " install");
            assertThat(result.stdout(), containsString("The service 'elasticsearch-service-x64' has been installed."));
        } finally {
            sh.runIgnoreExitCode(serviceScript + " remove");
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test20CustomizeServiceId() {
        String serviceId = "my-es-service";
        String displayName = DEFAULT_DISPLAY_NAME.replace(DEFAULT_ID, serviceId);
        sh.getEnv().put("SERVICE_ID", serviceId);
        sh.run(serviceScript + " install");
        assertService(serviceId, "Stopped", displayName);
        sh.run(serviceScript + " remove");
    }

    public void test21CustomizeServiceDisplayName() {
        String displayName = "my es service display name";
        sh.getEnv().put("SERVICE_DISPLAY_NAME", displayName);
        sh.run(serviceScript + " install");
        assertService(DEFAULT_ID, "Stopped", displayName);
        sh.run(serviceScript + " remove");
    }

    // NOTE: service description is not attainable through any powershell api, so checking it is not possible...
    public void assertStartedAndStop() throws Exception {
        ServerUtils.waitForElasticsearch(installation);
        runElasticsearchTests();

        assertCommand(serviceScript + " stop");
        assertService(DEFAULT_ID, "Stopped", DEFAULT_DISPLAY_NAME);
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

        assertCommand(serviceScript + " remove");
        assertCommand(
            "$p = Get-Service -Name \"elasticsearch-service-x64\" -ErrorAction SilentlyContinue;"
                + "echo \"$p\";"
                + "if ($p -eq $Null) {"
                + "  exit 0;"
                + "} else {"
                + "  exit 1;"
                + "}"
        );
    }

    public void test30StartStop() throws Exception {
        sh.run(serviceScript + " install");
        assertCommand(serviceScript + " start");
        assertStartedAndStop();
    }

    public void test31StartNotInstalled() throws IOException {
        Result result = sh.runIgnoreExitCode(serviceScript + " start");
        assertThat(result.stdout(), result.exitCode(), equalTo(1));
        assertThat(result.stdout(), containsString("Failed starting '" + DEFAULT_ID + "' service"));
    }

    public void test32StopNotStarted() throws IOException {
        sh.run(serviceScript + " install");
        Result result = sh.run(serviceScript + " stop"); // stop is ok when not started
        assertThat(result.stdout(), containsString("The service '" + DEFAULT_ID + "' has been stopped"));
    }

    public void test33JavaChanged() throws Exception {
        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");

        try {
            mv(installation.bundledJdk, relocatedJdk);
            sh.getEnv().put("ES_JAVA_HOME", relocatedJdk.toString());
            assertCommand(serviceScript + " install");
            sh.getEnv().remove("ES_JAVA_HOME");
            assertCommand(serviceScript + " start");
            assertStartedAndStop();
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test60Manager() throws IOException {
        Path serviceMgr = installation.bin("elasticsearch-service-mgr.exe");
        Path tmpServiceMgr = serviceMgr.getParent().resolve(serviceMgr.getFileName() + ".tmp");
        Files.move(serviceMgr, tmpServiceMgr);
        Path fakeServiceMgr = serviceMgr.getParent().resolve("elasticsearch-service-mgr.bat");
        Files.write(fakeServiceMgr, Arrays.asList("echo \"Fake Service Manager GUI\""));
        Shell sh = new Shell();
        Result result = sh.run(serviceScript + " manager");
        assertThat(result.stdout(), containsString("Fake Service Manager GUI"));

        // check failure too
        Files.write(fakeServiceMgr, Arrays.asList("echo \"Fake Service Manager GUI Failure\"", "exit 1"));
        result = sh.runIgnoreExitCode(serviceScript + " manager");
        TestCase.assertEquals(1, result.exitCode());
        TestCase.assertTrue(result.stdout(), result.stdout().contains("Fake Service Manager GUI Failure"));
        Files.move(tmpServiceMgr, serviceMgr);
    }

    public void test70UnknownCommand() {
        Result result = sh.runIgnoreExitCode(serviceScript + " bogus");
        assertThat(result.exitCode(), equalTo(1));
        assertThat(result.stdout(), containsString("Unknown option \"bogus\""));
    }

    public void test80JavaOptsInEnvVar() throws Exception {
        sh.getEnv().put("ES_JAVA_OPTS", "-Xmx2g -Xms2g");
        sh.run(serviceScript + " install");
        assertCommand(serviceScript + " start");
        assertStartedAndStop();
        sh.getEnv().remove("ES_JAVA_OPTS");
    }

    public void test81JavaOptsInJvmOptions() throws Exception {
        withCustomConfig(tempConf -> {
            append(tempConf.resolve("jvm.options"), "-Xmx2g" + System.lineSeparator());
            append(tempConf.resolve("jvm.options"), "-Xms2g" + System.lineSeparator());
            sh.run(serviceScript + " install");
            assertCommand(serviceScript + " start");
            assertStartedAndStop();
        });
    }

    // TODO:
    // custom SERVICE_USERNAME/SERVICE_PASSWORD
    // custom SERVICE_LOG_DIR
    // custom LOG_OPTS (looks like it currently conflicts with setting custom log dir)
    // install and run java opts Xmx/s (each data size type)
}
