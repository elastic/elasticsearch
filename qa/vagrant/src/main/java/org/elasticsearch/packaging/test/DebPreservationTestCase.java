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

import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class DebPreservationTestCase {

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

    @Test
    public void test10Install() {
        assertRemoved(distribution());
        installation = install(distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution());
    }

    @Test
    public void test20Remove() {
        assumeThat(installation, is(notNullValue()));

        remove(distribution());

        // some config files were not removed
        Stream.of(
            installation.config,
            installation.config("elasticsearch.yml"),
            installation.config("jvm.options"),
            installation.config("log4j2.properties")
        ).forEach(path -> assertTrue(path + " should exist", Files.exists(path)));

        // keystore was removed
        Stream.of(
            installation.config("elasticsearch.keystore"),
            installation.config(".elasticsearch.keystore.initial_md5sum")
        ).forEach(path -> assertFalse(path + " should not exist", Files.exists(path)));

        // doc files were removed
        Stream.of(
            "/usr/share/doc/" + distribution().flavor.name,
            "/usr/share/doc/" + distribution().flavor.name + "/copyright"
        ).map(Paths::get).forEach(path -> assertFalse(path + " does not exist", Files.exists(path)));

        // sysvinit service file was not removed
        assertTrue(Files.exists(SYSVINIT_SCRIPT));

        // defaults file was not removed
        assertTrue(Files.exists(installation.envFile));
    }

    @Test
    public void test30Purge() {
        assumeThat(installation, is(notNullValue()));

        final Shell sh = new Shell();
        sh.run("dpkg --purge " + distribution().flavor.name);

        assertRemoved(distribution());

        Stream.of(
            installation.config,
            installation.envFile,
            SYSVINIT_SCRIPT
        ).forEach(path -> assertFalse(path + " should not exist", Files.exists(path)));

        assertThat(packageStatus(distribution()).exitCode, is(1));
    }
}
