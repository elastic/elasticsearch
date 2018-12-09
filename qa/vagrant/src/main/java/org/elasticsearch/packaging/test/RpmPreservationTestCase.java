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

package org.elasticsearch.packaging.test;

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsDontExist;
import static org.elasticsearch.packaging.util.Packages.SYSTEMD_SERVICE;
import static org.elasticsearch.packaging.util.Packages.SYSVINIT_SCRIPT;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.install;
import static org.elasticsearch.packaging.util.Packages.remove;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.Platforms.isRPM;
import static org.elasticsearch.packaging.util.Platforms.isSystemd;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class RpmPreservationTestCase extends PackagingTestCase {

    private static Installation installation;

    protected abstract Distribution distribution();

    @BeforeClass
    public static void cleanup() {
        installation = null;
        cleanEverything();
    }

    @Before
    public void onlyCompatibleDistributions() {
        assumeTrue("only rpm platforms", isRPM());
        assumeTrue("only compatible distributions", distribution().packaging.compatible);
    }

    public void test10Install() {
        assertRemoved(distribution());
        installation = install(distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution());
    }

    public void test20Remove() {
        assumeThat(installation, is(notNullValue()));

        remove(distribution());

        // config was removed
        assertFalse(Files.exists(installation.config));

        // sysvinit service file was removed
        assertFalse(Files.exists(SYSVINIT_SCRIPT));

        // defaults file was removed
        assertFalse(Files.exists(installation.envFile));
    }

    public void test30PreserveConfig() {
        final Shell sh = new Shell();

        installation = install(distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution());

        sh.run("echo foobar | " + installation.executables().elasticsearchKeystore + " add --stdin foo.bar");
        Stream.of(
            "elasticsearch.yml",
            "jvm.options",
            "log4j2.properties"
        )
            .map(each -> installation.config(each))
            .forEach(path -> append(path, "# foo"));
        if (distribution().isDefault()) {
            Stream.of(
                "role_mapping.yml",
                "roles.yml",
                "users",
                "users_roles"
            )
                .map(each -> installation.config(each))
                .forEach(path -> append(path, "# foo"));
        }

        remove(distribution());
        assertRemoved(distribution());

        if (isSystemd()) {
            assertThat(sh.runIgnoreExitCode("systemctl is-enabled elasticsearch.service").exitCode, is(1));
        }

        assertPathsDontExist(
            installation.bin,
            installation.lib,
            installation.modules,
            installation.plugins,
            installation.logs,
            installation.pidDir,
            installation.envFile,
            SYSVINIT_SCRIPT,
            SYSTEMD_SERVICE
        );

        assertTrue(Files.exists(installation.config));
        assertTrue(Files.exists(installation.config("elasticsearch.keystore")));

        Stream.of(
            "elasticsearch.yml",
            "jvm.options",
            "log4j2.properties"
        ).forEach(this::assertConfFilePreserved);

        if (distribution().isDefault()) {
            Stream.of(
                "role_mapping.yml",
                "roles.yml",
                "users",
                "users_roles"
            ).forEach(this::assertConfFilePreserved);
        }
    }

    private void assertConfFilePreserved(String configFile) {
        final Path original = installation.config(configFile);
        final Path saved = installation.config(configFile + ".rpmsave");
        assertFalse(original + " should not exist", Files.exists(original));
        assertTrue(saved + " should exist", Files.exists(saved));
    }
}
