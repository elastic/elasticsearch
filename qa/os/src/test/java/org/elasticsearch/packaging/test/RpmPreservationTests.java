/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileDoesNotExist;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsDoNotExist;
import static org.elasticsearch.packaging.util.Packages.SYSTEMD_SERVICE;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.installPackage;
import static org.elasticsearch.packaging.util.Packages.remove;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.Platforms.isSystemd;
import static org.elasticsearch.packaging.util.ServerUtils.enableGeoIpDownloader;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeTrue;

public class RpmPreservationTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only rpm", distribution.packaging == Distribution.Packaging.RPM);
        assumeTrue("only bundled jdk", distribution().hasJdk);
    }

    public void test10Install() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
    }

    public void test20Remove() throws Exception {
        setHeap(null); // remove test heap options, so the config directory can be removed
        enableGeoIpDownloader(installation);
        remove(distribution());

        // defaults file was removed
        assertThat(installation.envFile, fileDoesNotExist());

        // don't perform normal setup/teardown after this since we removed the install
        installation = null;
    }

    public void test30PreserveConfig() throws Exception {
        final Shell sh = new Shell();

        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);

        sh.run("echo foobar | " + installation.executables().keystoreTool + " add --stdin foo.bar");
        Stream.of("elasticsearch.yml", "jvm.options", "log4j2.properties")
            .map(each -> installation.config(each))
            .forEach(path -> append(path, "# foo"));
        append(installation.config(Paths.get("jvm.options.d", "heap.options")), "# foo");
        Stream.of("role_mapping.yml", "roles.yml", "users", "users_roles")
            .map(each -> installation.config(each))
            .forEach(path -> append(path, "# foo"));

        remove(distribution());
        assertRemoved(distribution());

        if (isSystemd()) {
            assertThat(sh.runIgnoreExitCode("systemctl is-enabled elasticsearch.service").exitCode, is(1));
        }

        assertPathsDoNotExist(
            installation.bin,
            installation.lib,
            installation.modules,
            installation.plugins,
            installation.logs,
            installation.pidDir,
            installation.envFile,
            SYSTEMD_SERVICE
        );

        assertThat(installation.config, fileExists());
        assertThat(installation.config("elasticsearch.keystore"), fileExists());

        Stream.of("elasticsearch.yml", "jvm.options", "log4j2.properties").forEach(this::assertConfFilePreserved);
        assertThat(installation.config(Paths.get("jvm.options.d", "heap.options")), fileExists());

        Stream.of("role_mapping.yml", "roles.yml", "users", "users_roles").forEach(this::assertConfFilePreserved);
    }

    private void assertConfFilePreserved(String configFile) {
        final Path original = installation.config(configFile);
        final Path saved = installation.config(configFile + ".rpmsave");
        assertConfFilePreserved(original, saved);
    }

    private void assertConfFilePreserved(final Path original, final Path saved) {
        assertThat(original, fileDoesNotExist());
        assertThat(saved, fileExists());
    }

}
