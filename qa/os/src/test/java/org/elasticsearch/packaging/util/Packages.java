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
import org.elasticsearch.packaging.util.Shell.Result;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.elasticsearch.packaging.util.FileMatcher.Fileness.Directory;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p644;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileMatcher.p750;
import static org.elasticsearch.packaging.util.FileMatcher.p755;
import static org.elasticsearch.packaging.util.FileUtils.getCurrentVersion;
import static org.elasticsearch.packaging.util.Platforms.isSysVInit;
import static org.elasticsearch.packaging.util.Platforms.isSystemd;
import static org.elasticsearch.packaging.util.ServerUtils.waitForElasticsearch;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Packages {

    private static final Logger logger =  LogManager.getLogger(Packages.class);

    public static final Path SYSVINIT_SCRIPT = Paths.get("/etc/init.d/elasticsearch");
    public static final Path SYSTEMD_SERVICE = Paths.get("/usr/lib/systemd/system/elasticsearch.service");

    public static void assertInstalled(Distribution distribution) throws Exception {
        final Result status = packageStatus(distribution);
        assertThat(status.exitCode, is(0));

        Platforms.onDPKG(() -> assertFalse(Pattern.compile("(?m)^Status:.+deinstall ok").matcher(status.stdout).find()));
    }

    public static void assertRemoved(Distribution distribution) throws Exception {
        final Result status = packageStatus(distribution);

        Platforms.onRPM(() -> assertThat(status.exitCode, is(1)));

        Platforms.onDPKG(() -> {
            assertThat(status.exitCode, anyOf(is(0), is(1)));
            if (status.exitCode == 0) {
                assertTrue("an uninstalled status should be indicated: " + status.stdout,
                    Pattern.compile("(?m)^Status:.+deinstall ok").matcher(status.stdout).find() ||
                    Pattern.compile("(?m)^Status:.+ok not-installed").matcher(status.stdout).find()
                );
            }
        });
    }

    public static Result packageStatus(Distribution distribution) {
        final Shell sh = new Shell();
        final Result result;

        logger.info("Package type: " + distribution.packaging);
        if (distribution.packaging == Distribution.Packaging.RPM) {
            result = sh.runIgnoreExitCode("rpm -qe " + distribution.flavor.name);
        } else {
            result = sh.runIgnoreExitCode("dpkg -s " + distribution.flavor.name);
        }

        return result;
    }

    public static Installation installPackage(Shell sh, Distribution distribution) throws IOException {
        String systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
        if (distribution.hasJdk == false) {
            sh.getEnv().put("JAVA_HOME", systemJavaHome);
        }
        final Result result = runInstallCommand(distribution, sh);
        if (result.exitCode != 0) {
            throw new RuntimeException("Installing distribution " + distribution + " failed: " + result);
        }

        Installation installation = Installation.ofPackage(sh, distribution);

        if (distribution.hasJdk == false) {
            Files.write(installation.envFile, ("JAVA_HOME=" + systemJavaHome + "\n").getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.APPEND);
        }
        return installation;
    }

    private static Result runInstallCommand(Distribution distribution, Shell sh) {
        final Path distributionFile = distribution.path;

        if (Platforms.isRPM()) {
            return sh.runIgnoreExitCode("rpm -i " + distributionFile);
        } else {
            Result r = sh.runIgnoreExitCode("dpkg -i " + distributionFile);
            if (r.exitCode != 0) {
                Result lockOF = sh.runIgnoreExitCode("lsof /var/lib/dpkg/lock");
                if (lockOF.exitCode == 0) {
                    throw new RuntimeException(
                            "dpkg failed and the lockfile still exists. "
                            + "Failure:\n" + r + "\nLockfile:\n" + lockOF);
                }
            }
            return r;
        }
    }

    public static void remove(Distribution distribution) throws Exception {
        final Shell sh = new Shell();

        Platforms.onRPM(() -> {
            sh.run("rpm -e " + distribution.flavor.name);
            final Result status = packageStatus(distribution);
            assertThat(status.exitCode, is(1));
        });

        Platforms.onDPKG(() -> {
            sh.run("dpkg -r " + distribution.flavor.name);
            final Result status = packageStatus(distribution);
            assertThat(status.exitCode, is(0));
            assertTrue(Pattern.compile("(?m)^Status:.+deinstall ok").matcher(status.stdout).find());
        });
    }

    public static void verifyPackageInstallation(Installation installation, Distribution distribution, Shell sh) {
        verifyOssInstallation(installation, distribution, sh);
        if (distribution.flavor == Distribution.Flavor.DEFAULT) {
            verifyDefaultInstallation(installation);
        }
    }


    private static void verifyOssInstallation(Installation es, Distribution distribution, Shell sh) {

        sh.run("id elasticsearch");
        sh.run("getent group elasticsearch");

        final Result passwdResult = sh.run("getent passwd elasticsearch");
        final Path homeDir = Paths.get(passwdResult.stdout.trim().split(":")[5]);
        assertFalse("elasticsearch user home directory must not exist", Files.exists(homeDir));

        Stream.of(
            es.home,
            es.plugins,
            es.modules
        ).forEach(dir -> assertThat(dir, file(Directory, "root", "root", p755)));

        Stream.of(
            es.data,
            es.logs
        ).forEach(dir -> assertThat(dir, file(Directory, "elasticsearch", "elasticsearch", p750)));

        // we shell out here because java's posix file permission view doesn't support special modes
        assertThat(es.config, file(Directory, "root", "elasticsearch", p750));
        assertThat(sh.run("find \"" + es.config + "\" -maxdepth 0 -printf \"%m\"").stdout, containsString("2750"));

        Stream.of(
            "elasticsearch.keystore",
            "elasticsearch.yml",
            "jvm.options",
            "log4j2.properties"
        ).forEach(configFile -> assertThat(es.config(configFile), file(File, "root", "elasticsearch", p660)));
        assertThat(es.config(".elasticsearch.keystore.initial_md5sum"), file(File, "root", "elasticsearch", p644));

        assertThat(sh.run("sudo -u elasticsearch " + es.bin("elasticsearch-keystore") + " list").stdout, containsString("keystore.seed"));

        Stream.of(
            es.bin,
            es.lib
        ).forEach(dir -> assertThat(dir, file(Directory, "root", "root", p755)));

        Stream.of(
            "elasticsearch",
            "elasticsearch-plugin",
            "elasticsearch-keystore",
            "elasticsearch-shard",
            "elasticsearch-node"
        ).forEach(executable -> assertThat(es.bin(executable), file(File, "root", "root", p755)));

        Stream.of(
            "NOTICE.txt",
            "README.textile"
        ).forEach(doc -> assertThat(es.home.resolve(doc), file(File, "root", "root", p644)));

        assertThat(es.envFile, file(File, "root", "elasticsearch", p660));

        if (distribution.packaging == Distribution.Packaging.RPM) {
            assertThat(es.home.resolve("LICENSE.txt"), file(File, "root", "root", p644));
        } else {
            Path copyrightDir = Paths.get(sh.run("readlink -f /usr/share/doc/" + distribution.flavor.name).stdout.trim());
            assertThat(copyrightDir, file(Directory, "root", "root", p755));
            assertThat(copyrightDir.resolve("copyright"), file(File, "root", "root", p644));
        }

        if (isSystemd()) {
            Stream.of(
                SYSTEMD_SERVICE,
                Paths.get("/usr/lib/tmpfiles.d/elasticsearch.conf"),
                Paths.get("/usr/lib/sysctl.d/elasticsearch.conf")
            ).forEach(confFile -> assertThat(confFile, file(File, "root", "root", p644)));

            final String sysctlExecutable = (distribution.packaging == Distribution.Packaging.RPM)
                ? "/usr/sbin/sysctl"
                : "/sbin/sysctl";
            assertThat(sh.run(sysctlExecutable + " vm.max_map_count").stdout, containsString("vm.max_map_count = 262144"));
        }

        if (isSysVInit()) {
            assertThat(SYSVINIT_SCRIPT, file(File, "root", "root", p750));
        }
    }

    private static void verifyDefaultInstallation(Installation es) {

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
        ).forEach(executable -> assertThat(es.bin(executable), file(File, "root", "root", p755)));

        // at this time we only install the current version of archive distributions, but if that changes we'll need to pass
        // the version through here
        assertThat(es.bin("elasticsearch-sql-cli-" + getCurrentVersion() + ".jar"), file(File, "root", "root", p755));

        Stream.of(
            "users",
            "users_roles",
            "roles.yml",
            "role_mapping.yml",
            "log4j2.properties"
        ).forEach(configFile -> assertThat(es.config(configFile), file(File, "root", "elasticsearch", p660)));
    }

    /**
     * Starts Elasticsearch, without checking that startup is successful.
     */
    public static Shell.Result runElasticsearchStartCommand(Shell sh) throws IOException {
        if (isSystemd()) {
            sh.run("systemctl daemon-reload");
            sh.run("systemctl enable elasticsearch.service");
            sh.run("systemctl is-enabled elasticsearch.service");
            return sh.runIgnoreExitCode("systemctl start elasticsearch.service");
        }
        return sh.runIgnoreExitCode("service elasticsearch start");
    }

    /**
     * Clears the systemd journal. This is intended to clear the <code>journalctl</code> output
     * before a test that checks the journal output.
     */
    public static void clearJournal(Shell sh) {
        if (isSystemd()) {
            sh.run("rm -rf /run/log/journal/*");
            final Result result = sh.runIgnoreExitCode("systemctl restart systemd-journald");

            // Sometimes the restart fails on Debian 10 with:
            //    Job for systemd-journald.service failed because the control process exited with error code.
            //    See "systemctl status systemd-journald.service" and "journalctl -xe" for details.]
            //
            // ...so run these commands in an attempt to figure out what's going on.
            if (result.isSuccess() == false) {
                logger.error("Failed to restart systemd-journald: " + result);

                logger.error(sh.runIgnoreExitCode("systemctl status systemd-journald.service"));
                logger.error(sh.runIgnoreExitCode("journalctl -xe"));

                fail("Couldn't clear the systemd journal as restarting systemd-journald failed");
            }
        }
    }

    public static void assertElasticsearchStarted(Shell sh, Installation installation) throws IOException {
        waitForElasticsearch(installation);

        if (isSystemd()) {
            sh.run("systemctl is-active elasticsearch.service");
            sh.run("systemctl status elasticsearch.service");
        } else {
            sh.run("service elasticsearch status");
        }
    }

    public static void stopElasticsearch(Shell sh) {
        if (isSystemd()) {
            sh.run("systemctl stop elasticsearch.service");
        } else {
            sh.run("service elasticsearch stop");
        }
    }

    public static void restartElasticsearch(Shell sh, Installation installation) throws IOException {
        if (isSystemd()) {
            sh.run("systemctl restart elasticsearch.service");
        } else {
            sh.run("service elasticsearch restart");
        }
        assertElasticsearchStarted(sh, installation);
    }
}
