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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertTrue;

/**
 * Installation and verification logic for archive distributions
 */
public class Archives {

    private static final Log logger = LogFactory.getLog(Archives.class);

    // in the future we'll run as a role user on Windows
    public static final String ARCHIVE_OWNER = Platforms.WINDOWS
        ? "vagrant"
        : "elasticsearch";

    public static Installation installArchive(Distribution distribution) throws Exception {
        return installArchive(distribution, getDefaultArchiveInstallPath(), getCurrentVersion());
    }

    public static Installation installArchive(Distribution distribution, Path fullInstallPath, String version) throws Exception {
        final Shell sh = new Shell();

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
        Platforms.onWindows(() -> setupArchiveUsersWindows(fullInstallPath));

        return Installation.ofArchive(fullInstallPath);
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
        sh.run("chown -R elasticsearch:elasticsearch " + installPath);
    }

    private static void setupArchiveUsersWindows(Path installPath) {
        // we want the installation to be owned as the vagrant user rather than the Administrators group

        final Shell sh = new Shell();
        sh.run(
            "$account = New-Object System.Security.Principal.NTAccount 'vagrant'; " +
            "$install = Get-ChildItem -Path '" + installPath + "' -Recurse; " +
            "$install += Get-Item -Path '" + installPath + "'; " +
            "$install | ForEach-Object { " +
                "$acl = Get-Acl $_.FullName; " +
                "$acl.SetOwner($account); " +
                "Set-Acl $_.FullName $acl " +
            "}"
        );
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

    public static void runElasticsearch(Installation installation, Shell sh) throws Exception {
        final Path pidFile = installation.home.resolve("elasticsearch.pid");

        final Installation.Executables bin = installation.executables();

        Platforms.onLinux(() -> {
            // If jayatana is installed then we try to use it. Elasticsearch should ignore it even when we try.
            // If it doesn't ignore it then Elasticsearch will fail to start because of security errors.
            // This line is attempting to emulate the on login behavior of /usr/share/upstart/sessions/jayatana.conf
            if (Files.exists(Paths.get("/usr/share/java/jayatanaag.jar"))) {
                sh.getEnv().put("JAVA_TOOL_OPTIONS", "-javaagent:/usr/share/java/jayatanaag.jar");
            }
            sh.run("sudo -E -u " + ARCHIVE_OWNER + " " +
                bin.elasticsearch + " -d -p " + installation.home.resolve("elasticsearch.pid"));
        });

        Platforms.onWindows(() -> {
            // this starts the server in the background. the -d flag is unsupported on windows
            // these tests run as Administrator. we don't want to run the server as Administrator, so we provide the current user's
            // username and password to the process which has the effect of starting it not as Administrator.
            sh.run(
                "$password = ConvertTo-SecureString 'vagrant' -AsPlainText -Force; " +
                "$processInfo = New-Object System.Diagnostics.ProcessStartInfo; " +
                "$processInfo.FileName = '" + bin.elasticsearch + "'; " +
                "$processInfo.Arguments = '-p " + installation.home.resolve("elasticsearch.pid") + "'; " +
                "$processInfo.Username = 'vagrant'; " +
                "$processInfo.Password = $password; " +
                "$processInfo.RedirectStandardOutput = $true; " +
                "$processInfo.RedirectStandardError = $true; " +
                sh.env.entrySet().stream()
                    .map(entry -> "$processInfo.Environment.Add('" + entry.getKey() + "', '" + entry.getValue() + "'); ")
                    .collect(joining()) +
                "$processInfo.UseShellExecute = $false; " +
                "$process = New-Object System.Diagnostics.Process; " +
                "$process.StartInfo = $processInfo; " +
                "$process.Start() | Out-Null; " +
                "$process.Id;"
            );
        });

        ServerUtils.waitForElasticsearch();

        assertTrue(Files.exists(pidFile));
        String pid = slurp(pidFile).trim();
        assertThat(pid, not(isEmptyOrNullString()));

        Platforms.onLinux(() -> sh.run("ps " + pid));
        Platforms.onWindows(() -> sh.run("Get-Process -Id " + pid));
    }

    public static void stopElasticsearch(Installation installation) throws Exception {
        Path pidFile = installation.home.resolve("elasticsearch.pid");
        assertTrue(Files.exists(pidFile));
        String pid = slurp(pidFile).trim();
        assertThat(pid, not(isEmptyOrNullString()));

        final Shell sh = new Shell();
        Platforms.onLinux(() -> sh.run("kill -SIGTERM " + pid));
        Platforms.onWindows(() -> sh.run("Get-Process -Id " + pid + " | Stop-Process -Force"));
    }

}
