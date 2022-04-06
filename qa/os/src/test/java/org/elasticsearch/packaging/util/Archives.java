/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileDoesNotExist;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * Installation and verification logic for archive distributions
 */
public class Archives {

    protected static final Logger logger = LogManager.getLogger(Archives.class);

    // in the future we'll run as a role user on Windows
    public static final String ARCHIVE_OWNER = Platforms.WINDOWS ? System.getenv("username") : "elasticsearch";

    /** This is an arbitrarily chosen value that gives Elasticsearch time to log Bootstrap
     *  errors to the console if they occur before the logging framework is initialized. */
    public static final String ES_STARTUP_SLEEP_TIME_SECONDS = "25";

    public static Installation installArchive(Shell sh, Distribution distribution) throws Exception {
        return installArchive(sh, distribution, getDefaultArchiveInstallPath(), getCurrentVersion(), false);
    }

    public static Installation installArchive(
        Shell sh,
        Distribution distribution,
        Path fullInstallPath,
        String version,
        boolean allowMultiple
    ) throws Exception {
        final Path distributionFile = getDistributionFile(distribution);
        final Path baseInstallPath = fullInstallPath.getParent();
        final Path extractedPath = baseInstallPath.resolve("elasticsearch-" + version);

        assertThat("distribution file must exist: " + distributionFile.toString(), Files.exists(distributionFile), is(true));
        if (allowMultiple == false) {
            assertThat("elasticsearch must not already be installed", lsGlob(baseInstallPath, "elasticsearch*"), empty());
        }

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
            installCommand = String.format(
                Locale.ROOT,
                "Add-Type -AssemblyName 'System.IO.Compression.Filesystem'; [IO.Compression.ZipFile]::ExtractToDirectory('%s', '%s')",
                distributionFile,
                baseInstallPath
            );

        } else {
            throw new RuntimeException("Distribution " + distribution + " is not a known archive type");
        }

        sh.run(installCommand);
        assertThat("archive was extracted", Files.exists(extractedPath), is(true));

        mv(extractedPath, fullInstallPath);

        assertThat("extracted archive moved to install location", Files.exists(fullInstallPath));
        if (allowMultiple == false) {
            final List<Path> installations = lsGlob(baseInstallPath, "elasticsearch*");
            assertThat("only the intended installation exists", installations, hasSize(1));
            assertThat("only the intended installation exists", installations.get(0), is(fullInstallPath));
        }

        Platforms.onLinux(() -> setupArchiveUsersLinux(fullInstallPath));

        sh.chown(fullInstallPath);

        Installation installation = Installation.ofArchive(sh, distribution, fullInstallPath);
        ServerUtils.disableGeoIpDownloader(installation);

        return installation;
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
                sh.run(
                    "adduser "
                        + "--quiet "
                        + "--system "
                        + "--no-create-home "
                        + "--ingroup elasticsearch "
                        + "--disabled-password "
                        + "--shell /bin/false "
                        + "elasticsearch"
                );
            } else {
                sh.run(
                    "useradd "
                        + "--system "
                        + "-M "
                        + "--gid elasticsearch "
                        + "--shell /sbin/nologin "
                        + "--comment 'elasticsearch user' "
                        + "elasticsearch"
                );
            }
        }
    }

    public static void verifyArchiveInstallation(Installation installation, Distribution distribution) {
        verifyOssInstallation(installation, distribution, ARCHIVE_OWNER);
        verifyDefaultInstallation(installation, distribution, ARCHIVE_OWNER);
    }

    private static void verifyOssInstallation(Installation es, Distribution distribution, String owner) {
        Stream.of(es.home, es.config, es.plugins, es.modules, es.logs).forEach(dir -> assertThat(dir, file(Directory, owner, owner, p755)));

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
            Stream.of("elasticsearch-service.bat", "elasticsearch-service-mgr.exe", "elasticsearch-service-x64.exe")
                .forEach(executable -> assertThat(es.bin(executable), file(File, owner)));
        }

        Stream.of("elasticsearch.yml", "jvm.options", "log4j2.properties")
            .forEach(configFile -> assertThat(es.config(configFile), file(File, owner, owner, p660)));

        Stream.of("NOTICE.txt", "LICENSE.txt", "README.asciidoc")
            .forEach(doc -> assertThat(es.home.resolve(doc), file(File, owner, owner, p644)));
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
            "elasticsearch-service-tokens",
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

        Stream.of("users", "users_roles", "roles.yml", "role_mapping.yml", "log4j2.properties")
            .forEach(configFile -> assertThat(es.config(configFile), file(File, owner, owner, p660)));
    }

    /**
     * Starts an elasticsearch node from an attached terminal, optionally waiting for a specific string to be printed in stdout
     */
    public static Shell.Result startElasticsearchWithTty(
        Installation installation,
        Shell sh,
        String keystorePassword,
        List<String> parameters,
        String outputStringToMatch,
        boolean daemonize
    ) {
        final Path pidFile = installation.home.resolve("elasticsearch.pid");
        final Installation.Executables bin = installation.executables();

        // requires the "expect" utility to be installed
        List<String> command = new ArrayList<>();
        command.add("sudo -E -u %s %s -p %s");
        if (daemonize) {
            command.add("-d");
        }
        command.add("-v"); // verbose auto-configuration
        if (parameters != null && parameters.isEmpty() == false) {
            command.addAll(parameters);
        }
        String keystoreScript = keystorePassword == null ? "" : """
            expect "Elasticsearch keystore password:"
            send "%s\\r"
            """.formatted(keystorePassword);
        String checkStartupScript = daemonize ? "expect eof" : """
            expect {
              "uncaught exception" { send_user "\\nStartup failed due to uncaught exception\\n"; exit 1 }
              timeout { send_user "\\nTimed out waiting for startup to succeed\\n"; exit 1 }
              eof { send_user "\\nFailed to determine if startup succeeded\\n"; exit 1 }
              %s
            }
            """.formatted(null == outputStringToMatch ? "-re \"o\\.e\\.n\\.Node.*] started\"" : "\"" + outputStringToMatch + "\"");
        String expectScript = """
            expect - <<EXPECT
            set timeout 60
            spawn -ignore HUP %s
            %s
            %s
            EXPECT
            """.formatted(
            String.join(" ", command).formatted(ARCHIVE_OWNER, bin.elasticsearch, pidFile),
            keystoreScript,
            checkStartupScript
        );
        sh.getEnv().put("ES_STARTUP_SLEEP_TIME", ES_STARTUP_SLEEP_TIME_SECONDS);
        return sh.runIgnoreExitCode(expectScript);
    }

    public static Shell.Result runElasticsearchStartCommand(
        Installation installation,
        Shell sh,
        String keystorePassword,
        List<String> parameters,
        boolean daemonize
    ) {
        final Path pidFile = installation.home.resolve("elasticsearch.pid");

        assertThat(pidFile, fileDoesNotExist());

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

            List<String> command = new ArrayList<>();
            command.add("sudo -E -u ");
            command.add(ARCHIVE_OWNER);
            command.add(bin.elasticsearch.toString());
            if (daemonize) {
                command.add("-d");
            }
            command.add("-v"); // verbose auto-configuration
            command.add("-p");
            command.add(pidFile.toString());
            if (parameters != null && parameters.isEmpty() == false) {
                command.addAll(parameters);
            }
            if (keystorePassword != null) {
                command.add("<<<'" + keystorePassword + "'");
            }
            return sh.runIgnoreExitCode(String.join(" ", command));
        }

        if (daemonize) {
            final Path stdout = getPowershellOutputPath(installation);
            final Path stderr = getPowershellErrorPath(installation);

            String powerShellProcessUserSetup;
            if (System.getenv("username").equals("vagrant")) {
                // the tests will run as Administrator in vagrant.
                // we don't want to run the server as Administrator, so we provide the current user's
                // username and password to the process which has the effect of starting it not as Administrator.
                powerShellProcessUserSetup = "$password = ConvertTo-SecureString 'vagrant' -AsPlainText -Force; "
                    + "$processInfo.Username = 'vagrant'; "
                    + "$processInfo.Password = $password; ";
            } else {
                powerShellProcessUserSetup = "";
            }
            // this starts the server in the background. the -d flag is unsupported on windows
            final String parameterString = parameters != null && parameters.isEmpty() == false ? String.join(" ", parameters) : "";
            return sh.run(
                "$processInfo = New-Object System.Diagnostics.ProcessStartInfo; "
                    + "$processInfo.FileName = '"
                    + bin.elasticsearch
                    + "'; "
                    + "$processInfo.Arguments = '-v -p "
                    + installation.home.resolve("elasticsearch.pid")
                    + parameterString
                    + "'; "
                    + powerShellProcessUserSetup
                    + "$processInfo.RedirectStandardOutput = $true; "
                    + "$processInfo.RedirectStandardError = $true; "
                    + "$processInfo.RedirectStandardInput = $true; "
                    + sh.env.entrySet()
                        .stream()
                        .map(entry -> "$processInfo.Environment.Add('" + entry.getKey() + "', '" + entry.getValue() + "'); ")
                        .collect(joining())
                    + "$processInfo.UseShellExecute = $false; "
                    + "$process = New-Object System.Diagnostics.Process; "
                    + "$process.StartInfo = $processInfo; "
                    +

                    // set up some asynchronous output handlers
                    "$outScript = { $EventArgs.Data | Out-File -Encoding UTF8 -Append '"
                    + stdout
                    + "' }; "
                    + "$errScript = { $EventArgs.Data | Out-File -Encoding UTF8 -Append '"
                    + stderr
                    + "' }; "
                    + "$stdOutEvent = Register-ObjectEvent -InputObject $process "
                    + "-Action $outScript -EventName 'OutputDataReceived'; "
                    + "$stdErrEvent = Register-ObjectEvent -InputObject $process "
                    + "-Action $errScript -EventName 'ErrorDataReceived'; "
                    +

                    "$process.Start() | Out-Null; "
                    + "$process.BeginOutputReadLine(); "
                    + "$process.BeginErrorReadLine(); "
                    + "$process.StandardInput.WriteLine('"
                    + keystorePassword
                    + "'); "
                    + "Wait-Process -Timeout "
                    + ES_STARTUP_SLEEP_TIME_SECONDS
                    + " -Id $process.Id; "
                    + "$process.Id;"
            );
        } else {
            List<String> command = new ArrayList<>();
            if (keystorePassword != null) {
                command.add("echo '" + keystorePassword + "' |");
            }
            command.add(bin.elasticsearch.toString());
            command.add("-v"); // verbose auto-configuration
            command.add("-p");
            command.add(installation.home.resolve("elasticsearch.pid").toString());
            if (parameters != null && parameters.isEmpty() == false) {
                command.addAll(parameters);
            }
            return sh.runIgnoreExitCode(String.join(" ", command));
        }
    }

    public static void assertElasticsearchStarted(Installation installation) throws Exception {
        final Path pidFile = installation.home.resolve("elasticsearch.pid");
        ServerUtils.waitForElasticsearch(installation);

        assertThat("Starting Elasticsearch produced a pid file at " + pidFile, pidFile, fileExists());
        String pid = slurp(pidFile).trim();
        assertThat(pid, is(not(emptyOrNullString())));
    }

    public static void stopElasticsearch(Installation installation) throws Exception {
        Path pidFile = installation.home.resolve("elasticsearch.pid");
        assertThat(pidFile, fileExists());
        String pid = slurp(pidFile).trim();
        assertThat("No PID found in " + pidFile, pid, is(not(emptyOrNullString())));

        final Shell sh = new Shell();
        Platforms.onLinux(() -> sh.run("kill -SIGTERM " + pid + " && tail --pid=" + pid + " -f /dev/null"));
        Platforms.onWindows(() -> {
            sh.run("Get-Process -Id " + pid + " | Stop-Process -Force; Wait-Process -Id " + pid);

            // Clear the asynchronous event handlers
            sh.runIgnoreExitCode(
                "Get-EventSubscriber | "
                    + "Where-Object {($_.EventName -eq 'OutputDataReceived') -or ($_.EventName -eq 'ErrorDataReceived')} | "
                    + "Unregister-Event -Force"
            );
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
