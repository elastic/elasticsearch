/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.packaging.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.Directory;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p644;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileMatcher.p755;
import static org.elasticsearch.packaging.util.FileUtils.getCurrentVersion;
import static org.elasticsearch.packaging.util.FileUtils.getDefaultArchiveInstallPath;
import static org.elasticsearch.packaging.util.FileUtils.getDistributionFile;
import static org.elasticsearch.packaging.util.FileUtils.lsGlob;
import static org.elasticsearch.packaging.util.FileUtils.mv;
import static org.elasticsearch.packaging.util.FileUtils.slurp;
import static org.elasticsearch.packaging.util.Platforms.isDPKG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Installation and verification logic for archive distributions
 */
public class Archives {

    protected static final Logger logger =  LogManager.getLogger(Archives.class);

    // in the future we'll run as a role user on Windows
    public static final String ARCHIVE_OWNER = Platforms.WINDOWS
        ? System.getenv("username")
        : "elasticsearch";

    /** This is an arbitrarily chosen value that gives Elasticsearch time to log Bootstrap
     *  errors to the console if they occur before the logging framework is initialized. */
    public static final String ES_STARTUP_SLEEP_TIME_SECONDS = "10";

    public static Installation installArchive(Shell sh, Distribution distribution) throws Exception {
        return installArchive(sh, distribution, getDefaultArchiveInstallPath(), getCurrentVersion());
    }

    public static Installation installArchive(Shell sh, Distribution distribution, Path fullInstallPath, String version) throws Exception {
        final Path distributionFile = getDistributionFile(distribution);
        final Path baseInstallPath = fullInstallPath.getParent();
        final Path extractedPath = baseInstallPath.resolve("elasticsearch-" + version);

        assertThat("distribution file must exist: " + distributionFile.toString(), Files.exists(distributionFile), is(true));
        assertThat("elasticsearch must not already be installed", lsGlob(baseInstallPath, "elasticsearch*"), empty());

        logger.info("Installing file: " + distributionFile);
        final String installCommand;
        if (distribution.packaging == Distribution.Packaging.TAR) {
            if (Platforms.WINDOWS) {
                throw new IllegalStateException("Distribution " + distribution + " is not supported on windows");
            }
            installCommand = "tar -C " + baseInstallPath + " -xzpf " + distributionFile;

        } else if (distribution.packaging == Distribution.Packaging.ZIP) {
            if (Platforms.WINDOWS == false) {
                throw new IllegalStateException("Distribution " + distribution + " is not supported on linux");
            }
            installCommand =
                "Add-Type -AssemblyName 'System.IO.Compression.Filesystem'; " +
                "[IO.Compression.ZipFile]::ExtractToDirectory('" + distributionFile + "', '" + baseInstallPath + "')";

        } else {
            throw new RuntimeException("Distribution " + distribution + " is not a known archive type");
        }

        sh.run(installCommand);
        assertThat("archive was extracted", Files.exists(extractedPath), is(true));

        mv(extractedPath, fullInstallPath);

        assertThat("extracted archive moved to install location", Files.exists(fullInstallPath));
        final List<Path> installations = lsGlob(baseInstallPath, "elasticsearch*");
        assertThat("only the intended installation exists", installations, hasSize(1));
        assertThat("only the intended installation exists", installations.get(0), is(fullInstallPath));

        Platforms.onLinux(() -> setupArchiveUsersLinux(fullInstallPath));

        sh.chown(fullInstallPath);

        return Installation.ofArchive(sh, distribution, fullInstallPath);
    }

    private static void setupArchiveUsersLinux(Path installPath) {
        final Shell sh = new Shell();

        if (sh.runIgnoreExitCode("getent group elasticsearch").isSuccess() == false) {
            if (isDPKG()) {
                sh.run("addgroup --system elasticsearch");
            } else {
                sh.run("groupadd -r elasticsearch");
            }
        }

        if (sh.runIgnoreExitCode("id elasticsearch").isSuccess() == false) {
            if (isDPKG()) {
                sh.run("adduser " +
                    "--quiet " +
                    "--system " +
                    "--no-create-home " +
                    "--ingroup elasticsearch " +
                    "--disabled-password " +
                    "--shell /bin/false " +
                    "elasticsearch");
            } else {
                sh.run("useradd " +
                    "--system " +
                    "-M " +
                    "--gid elasticsearch " +
                    "--shell /sbin/nologin " +
                    "--comment 'elasticsearch user' " +
                    "elasticsearch");
            }
        }
    }

    public static void verifyArchiveInstallation(Installation installation, Distribution distribution) {
        verifyOssInstallation(installation, distribution, ARCHIVE_OWNER);
        if (distribution.flavor == Distribution.Flavor.DEFAULT) {
            verifyDefaultInstallation(installation, distribution, ARCHIVE_OWNER);
        }
    }

    private static void verifyOssInstallation(Installation es, Distribution distribution, String owner) {
        Stream.of(
            es.home,
            es.config,
            es.plugins,
            es.modules,
            es.logs
        ).forEach(dir -> assertThat(dir, file(Directory, owner, owner, p755)));

        assertThat(Files.exists(es.data), is(false));

        assertThat(es.bin, file(Directory, owner, owner, p755));
        assertThat(es.lib, file(Directory, owner, owner, p755));
        assertThat(Files.exists(es.config("elasticsearch.keystore")), is(false));

        Stream.of(
            "elasticsearch",
            "elasticsearch-env",
            "elasticsearch-keystore",
            "elasticsearch-plugin",
            "elasticsearch-shard",
            "elasticsearch-node"
        ).forEach(executable -> {

            assertThat(es.bin(executable), file(File, owner, owner, p755));

            if (distribution.packaging == Distribution.Packaging.ZIP) {
                assertThat(es.bin(executable + ".bat"), file(File, owner));
            }
        });

        if (distribution.packaging == Distribution.Packaging.ZIP) {
            Stream.of(
                "elasticsearch-service.bat",
                "elasticsearch-service-mgr.exe",
                "elasticsearch-service-x64.exe"
            ).forEach(executable -> assertThat(es.bin(executable), file(File, owner)));
        }

        Stream.of(
            "elasticsearch.yml",
            "jvm.options",
            "log4j2.properties"
        ).forEach(configFile -> assertThat(es.config(configFile), file(File, owner, owner, p660)));

        Stream.of(
            "NOTICE.txt",
            "LICENSE.txt",
            "README.textile"
        ).forEach(doc -> assertThat(es.home.resolve(doc), file(File, owner, owner, p644)));
    }

    private static void verifyDefaultInstallation(Installation es, Distribution distribution, String owner) {

        Stream.of(
            "elasticsearch-certgen",
            "elasticsearch-certutil",
            "elasticsearch-croneval",
            "elasticsearch-saml-metadata",
            "elasticsearch-setup-passwords",
            "elasticsearch-sql-cli",
            "elasticsearch-syskeygen",
            "elasticsearch-users",
            "x-pack-env",
            "x-pack-security-env",
            "x-pack-watcher-env"
        ).forEach(executable -> {

            assertThat(es.bin(executable), file(File, owner, owner, p755));

            if (distribution.packaging == Distribution.Packaging.ZIP) {
                assertThat(es.bin(executable + ".bat"), file(File, owner));
            }
        });

        // at this time we only install the current version of archive distributions, but if that changes we'll need to pass
        // the version through here
        assertThat(es.bin("elasticsearch-sql-cli-" + getCurrentVersion() + ".jar"), file(File, owner, owner, p755));

        Stream.of(
            "users",
            "users_roles",
            "roles.yml",
            "role_mapping.yml",
            "log4j2.properties"
        ).forEach(configFile -> assertThat(es.config(configFile), file(File, owner, owner, p660)));
    }

    public static Shell.Result runElasticsearchStartCommand(Installation installation, Shell sh) {
        final Path pidFile = installation.home.resolve("elasticsearch.pid");

        assertFalse("Pid file doesn't exist when starting Elasticsearch", Files.exists(pidFile));

        final Installation.Executables bin = installation.executables();

        if (Platforms.WINDOWS == false) {
            // If jayatana is installed then we try to use it. Elasticsearch should ignore it even when we try.
            // If it doesn't ignore it then Elasticsearch will fail to start because of security errors.
            // This line is attempting to emulate the on login behavior of /usr/share/upstart/sessions/jayatana.conf
            if (Files.exists(Paths.get("/usr/share/java/jayatanaag.jar"))) {
                sh.getEnv().put("JAVA_TOOL_OPTIONS", "-javaagent:/usr/share/java/jayatanaag.jar");
            }

            // We need to give Elasticsearch enough time to print failures to stderr before exiting
            sh.getEnv().put("ES_STARTUP_SLEEP_TIME", ES_STARTUP_SLEEP_TIME_SECONDS);
            return sh.runIgnoreExitCode("sudo -E -u " + ARCHIVE_OWNER + " " + bin.elasticsearch + " -d -p " + pidFile);
        }
        final Path stdout = getPowershellOutputPath(installation);
        final Path stderr = getPowershellErrorPath(installation);

        String powerShellProcessUserSetup;
        if (System.getenv("username").equals("vagrant")) {
            // the tests will run as Administrator in vagrant.
            // we don't want to run the server as Administrator, so we provide the current user's
            // username and password to the process which has the effect of starting it not as Administrator.
            powerShellProcessUserSetup =
                "$password = ConvertTo-SecureString 'vagrant' -AsPlainText -Force; " +
                "$processInfo.Username = 'vagrant'; " +
                "$processInfo.Password = $password; ";
        } else {
            powerShellProcessUserSetup = "";
        }

        // this starts the server in the background. the -d flag is unsupported on windows
        return sh.run(
            "$processInfo = New-Object System.Diagnostics.ProcessStartInfo; " +
                "$processInfo.FileName = '" + bin.elasticsearch + "'; " +
                "$processInfo.Arguments = '-p " + installation.home.resolve("elasticsearch.pid") + "'; " +
                powerShellProcessUserSetup +
                "$processInfo.RedirectStandardOutput = $true; " +
                "$processInfo.RedirectStandardError = $true; " +
                "$processInfo.RedirectStandardInput = $true; " +
                sh.env.entrySet().stream()
                    .map(entry -> "$processInfo.Environment.Add('" + entry.getKey() + "', '" + entry.getValue() + "'); ")
                    .collect(joining()) +
                "$processInfo.UseShellExecute = $false; " +
                "$process = New-Object System.Diagnostics.Process; " +
                "$process.StartInfo = $processInfo; " +

                // set up some asynchronous output handlers
                "$outScript = { $EventArgs.Data | Out-File -Encoding UTF8 -Append '" + stdout + "' }; " +
                "$errScript = { $EventArgs.Data | Out-File -Encoding UTF8 -Append '" + stderr + "' }; " +
                "$stdOutEvent = Register-ObjectEvent -InputObject $process " +
                "-Action $outScript -EventName 'OutputDataReceived'; " +
                "$stdErrEvent = Register-ObjectEvent -InputObject $process " +
                "-Action $errScript -EventName 'ErrorDataReceived'; " +

                "$process.Start() | Out-Null; " +
                "$process.BeginOutputReadLine(); " +
                "$process.BeginErrorReadLine(); " +
                "Wait-Process -Timeout " + ES_STARTUP_SLEEP_TIME_SECONDS + " -Id $process.Id; " +
                "$process.Id;"
            );
    }

    public static void assertElasticsearchStarted(Installation installation) throws Exception {
        final Path pidFile = installation.home.resolve("elasticsearch.pid");
        ServerUtils.waitForElasticsearch(installation);

        assertTrue("Starting Elasticsearch produced a pid file at " + pidFile, Files.exists(pidFile));
        String pid = slurp(pidFile).trim();
        assertThat(pid, is(not(emptyOrNullString())));
    }

    public static void stopElasticsearch(Installation installation) throws Exception {
        Path pidFile = installation.home.resolve("elasticsearch.pid");
        assertTrue("pid file should exist", Files.exists(pidFile));
        String pid = slurp(pidFile).trim();
        assertThat(pid, is(not(emptyOrNullString())));

        final Shell sh = new Shell();
        Platforms.onLinux(() -> sh.run("kill -SIGTERM " + pid + "; tail --pid=" + pid + " -f /dev/null"));
        Platforms.onWindows(() -> {
            sh.run("Get-Process -Id " + pid + " | Stop-Process -Force; Wait-Process -Id " + pid);

            // Clear the asynchronous event handlers
            sh.runIgnoreExitCode("Get-EventSubscriber | " +
                "where {($_.EventName -eq 'OutputDataReceived' -Or $_.EventName -eq 'ErrorDataReceived' |" +
                "Unregister-EventSubscriber -Force");
        });
        if (Files.exists(pidFile)) {
            Files.delete(pidFile);
        }
    }

    public static Path getPowershellErrorPath(Installation installation) {
        return installation.logs.resolve("output.err");
    }

    private static Path getPowershellOutputPath(Installation installation) {
        return installation.logs.resolve("output.out");
    }

}
