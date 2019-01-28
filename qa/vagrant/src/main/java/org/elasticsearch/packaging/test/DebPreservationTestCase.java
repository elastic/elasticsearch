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
import java.nio.file.Paths;

import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsDontExist;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsExist;
import static org.elasticsearch.packaging.util.Packages.SYSVINIT_SCRIPT;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.install;
import static org.elasticsearch.packaging.util.Packages.remove;
import static org.elasticsearch.packaging.util.Packages.packageStatus;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.Platforms.isDPKG;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class DebPreservationTestCase extends PackagingTestCase {

    private static Installation installation;

    protected abstract Distribution distribution();

    @BeforeClass
    public static void cleanup() {
        installation = null;
        cleanEverything();
    }

    @Before
    public void onlyCompatibleDistributions() {
        assumeTrue("only dpkg platforms", isDPKG());
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

        // some config files were not removed

        assertPathsExist(
            installation.config,
            installation.config("elasticsearch.yml"),
            installation.config("jvm.options"),
            installation.config("log4j2.properties")
        );

        if (distribution().isDefault()) {
            assertPathsExist(
                installation.config,
                installation.config("role_mapping.yml"),
                installation.config("roles.yml"),
                installation.config("users"),
                installation.config("users_roles")
            );
        }

        // keystore was removed

        assertPathsDontExist(
            installation.config("elasticsearch.keystore"),
            installation.config(".elasticsearch.keystore.initial_md5sum")
        );

        // doc files were removed

        assertPathsDontExist(
            Paths.get("/usr/share/doc/" + distribution().flavor.name),
            Paths.get("/usr/share/doc/" + distribution().flavor.name + "/copyright")
        );

        // sysvinit service file was not removed
        assertTrue(Files.exists(SYSVINIT_SCRIPT));

        // defaults file was not removed
        assertTrue(Files.exists(installation.envFile));
    }

    public void test30Purge() {
        assumeThat(installation, is(notNullValue()));

        final Shell sh = new Shell();
        sh.run("dpkg --purge " + distribution().flavor.name);

        assertRemoved(distribution());

        assertPathsDontExist(
            installation.config,
            installation.envFile,
            SYSVINIT_SCRIPT
        );

        assertThat(packageStatus(distribution()).exitCode, is(1));
    }
}
