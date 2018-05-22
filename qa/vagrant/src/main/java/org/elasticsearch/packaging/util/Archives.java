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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.packaging.util.FileMatcher.Fileness.Directory;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p644;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileMatcher.p755;
import static org.elasticsearch.packaging.util.FileUtils.getCurrentVersion;
import static org.elasticsearch.packaging.util.FileUtils.getDefaultArchiveInstallPath;
import static org.elasticsearch.packaging.util.FileUtils.getPackagingArchivesDir;
import static org.elasticsearch.packaging.util.FileUtils.lsGlob;

import static org.elasticsearch.packaging.util.FileUtils.mv;
import static org.elasticsearch.packaging.util.Platforms.isDPKG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

/**
 * Installation and verification logic for archive distributions
 */
public class Archives {

    public static Installation installArchive(Distribution distribution) {
        return installArchive(distribution, getDefaultArchiveInstallPath(), getCurrentVersion());
    }

    public static Installation installArchive(Distribution distribution, Path fullInstallPath, String version) {
        final Shell sh = new Shell();

        final Path distributionFile = getPackagingArchivesDir().resolve(distribution.filename(version));
        final Path baseInstallPath = fullInstallPath.getParent();
        final Path extractedPath = baseInstallPath.resolve("elasticsearch-" + version);

        assertThat("distribution file must exist", Files.exists(distributionFile), is(true));
        assertThat("elasticsearch must not already be installed", lsGlob(baseInstallPath, "elasticsearch*"), empty());

        if (distribution.packaging == Distribution.Packaging.TAR) {

            if (Platforms.LINUX) {
                sh.run("tar", "-C", baseInstallPath.toString(), "-xzpf", distributionFile.toString());
            } else {
                throw new RuntimeException("Distribution " + distribution + " is not supported on windows");
            }

        } else if (distribution.packaging == Distribution.Packaging.ZIP) {

            if (Platforms.LINUX) {
                sh.run("unzip", distributionFile.toString(), "-d", baseInstallPath.toString());
            } else {
                sh.run("powershell.exe", "-Command",
                    "Add-Type -AssemblyName 'System.IO.Compression.Filesystem'; " +
                    "[IO.Compression.ZipFile]::ExtractToDirectory('" + distributionFile + "', '" + baseInstallPath + "')");
            }

        } else {
            throw new RuntimeException("Distribution " + distribution + " is not a known archive type");
        }

        assertThat("archive was extracted", Files.exists(extractedPath), is(true));

        mv(extractedPath, fullInstallPath);

        assertThat("extracted archive moved to install location", Files.exists(fullInstallPath));
        final List<Path> installations = lsGlob(baseInstallPath, "elasticsearch*");
        assertThat("only the intended installation exists", installations, hasSize(1));
        assertThat("only the intended installation exists", installations.get(0), is(fullInstallPath));

        if (Platforms.LINUX) {
            setupArchiveUsersLinux(fullInstallPath);
        }

        return new Installation(fullInstallPath);
    }

    private static void setupArchiveUsersLinux(Path installPath) {
        final Shell sh = new Shell();

        if (sh.runIgnoreExitCode("getent", "group", "elasticsearch").isSuccess() == false) {
            if (isDPKG()) {
                sh.run("addgroup", "--system", "elasticsearch");
            } else {
                sh.run("groupadd", "-r", "elasticsearch");
            }
        }

        if (sh.runIgnoreExitCode("id", "elasticsearch").isSuccess() == false) {
            if (isDPKG()) {
                sh.run("adduser",
                    "--quiet",
                    "--system",
                    "--no-create-home",
                    "--ingroup", "elasticsearch",
                    "--disabled-password",
                    "--shell", "/bin/false",
                    "elasticsearch");
            } else {
                sh.run("useradd",
                    "--system",
                    "-M",
                    "--gid", "elasticsearch",
                    "--shell", "/sbin/nologin",
                    "--comment", "elasticsearch user",
                    "elasticsearch");
            }
        }
        sh.run("chown", "-R", "elasticsearch:elasticsearch", installPath.toString());
    }

    public static void verifyArchiveInstallation(Installation installation, Distribution distribution) {
        // on Windows for now we leave the installation owned by the vagrant user that the tests run as. Since the vagrant account
        // is a local administrator, the files really end up being owned by the local administrators group. In the future we'll
        // install and run elasticesearch with a role user on Windows
        final String owner = Platforms.WINDOWS
            ? "BUILTIN\\Administrators"
            : "elasticsearch";

        verifyOssInstallation(installation, distribution, owner);
        if (distribution.flavor == Distribution.Flavor.DEFAULT) {
            verifyDefaultInstallation(installation, distribution, owner);
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
        assertThat(Files.exists(es.scripts), is(false));

        assertThat(es.home.resolve("bin"), file(Directory, owner, owner, p755));
        assertThat(es.home.resolve("lib"), file(Directory, owner, owner, p755));
        assertThat(Files.exists(es.config.resolve("elasticsearch.keystore")), is(false));

        Stream.of(
            "bin/elasticsearch",
            "bin/elasticsearch-env",
            "bin/elasticsearch-keystore",
            "bin/elasticsearch-plugin",
            "bin/elasticsearch-translog"
        ).forEach(executable -> {

            assertThat(es.home.resolve(executable), file(File, owner, owner, p755));

            if (distribution.packaging == Distribution.Packaging.ZIP) {
                assertThat(es.home.resolve(executable + ".bat"), file(File, owner));
            }
        });

        if (distribution.packaging == Distribution.Packaging.ZIP) {
            Stream.of(
                "bin/elasticsearch-service.bat",
                "bin/elasticsearch-service-mgr.exe",
                "bin/elasticsearch-service-x64.exe"
            ).forEach(executable -> assertThat(es.home.resolve(executable), file(File, owner)));
        }

        Stream.of(
            "elasticsearch.yml",
            "jvm.options",
            "log4j2.properties"
        ).forEach(config -> assertThat(es.config.resolve(config), file(File, owner, owner, p660)));

        Stream.of(
            "NOTICE.txt",
            "LICENSE.txt",
            "README.textile"
        ).forEach(doc -> assertThat(es.home.resolve(doc), file(File, owner, owner, p644)));
    }

    private static void verifyDefaultInstallation(Installation es, Distribution distribution, String owner) {

        Stream.of(
            "bin/elasticsearch-certgen",
            "bin/elasticsearch-certutil",
            "bin/elasticsearch-croneval",
            "bin/elasticsearch-migrate",
            "bin/elasticsearch-saml-metadata",
            "bin/elasticsearch-setup-passwords",
            "bin/elasticsearch-sql-cli",
            "bin/elasticsearch-syskeygen",
            "bin/elasticsearch-users",
            "bin/x-pack-env",
            "bin/x-pack-security-env",
            "bin/x-pack-watcher-env"
        ).forEach(executable -> {

            assertThat(es.home.resolve(executable), file(File, owner, owner, p755));

            if (distribution.packaging == Distribution.Packaging.ZIP) {
                assertThat(es.home.resolve(executable + ".bat"), file(File, owner));
            }
        });

        // at this time we only install the current version of archive distributions, but if that changes we'll need to pass
        // the version through here
        assertThat(es.home.resolve("bin/elasticsearch-sql-cli-" + getCurrentVersion() + ".jar"), file(File, owner, owner, p755));

        Stream.of(
            "users",
            "users_roles",
            "roles.yml",
            "role_mapping.yml",
            "log4j2.properties"
        ).forEach(config -> assertThat(es.config.resolve(config), file(File, owner, owner, p660)));
    }

}
