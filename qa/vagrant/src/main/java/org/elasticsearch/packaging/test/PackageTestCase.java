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

import java.io.IOException;
import java.nio.file.Files;

import static junit.framework.TestCase.assertTrue;
import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsDontExist;
import static org.elasticsearch.packaging.util.Packages.SYSTEMD_SERVICE;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.install;
import static org.elasticsearch.packaging.util.Packages.remove;
import static org.elasticsearch.packaging.util.Packages.startElasticsearch;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.Platforms.isSystemd;
import static org.elasticsearch.packaging.util.ServerUtils.runElasticsearchTests;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isEmptyString;

import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class PackageTestCase extends PackagingTestCase {

    private static Installation installation;

    protected abstract Distribution distribution();

    @BeforeClass
    public static void cleanup() {
        installation = null;
        cleanEverything();
    }

    @Before
    public void onlyCompatibleDistributions() {
        assumeTrue("only compatible distributions", distribution().packaging.compatible);
    }

    public void test10InstallPackage() {
        assertRemoved(distribution());
        installation = install(distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution());
    }

    public void test20PluginsCommandWhenNoPlugins() {
        assumeThat(installation, is(notNullValue()));

        final Shell sh = new Shell();
        assertThat(sh.run(installation.bin("elasticsearch-plugin") + " list").stdout, isEmptyString());
    }

    public void test30InstallDoesNotStartServer() {
        assumeThat(installation, is(notNullValue()));

        final Shell sh = new Shell();
        assertThat(sh.run("ps aux").stdout, not(containsString("org.elasticsearch.bootstrap.Elasticsearch")));
    }

    public void test40StartServer() throws IOException {
        assumeThat(installation, is(notNullValue()));

        startElasticsearch();
        runElasticsearchTests();
        verifyPackageInstallation(installation, distribution()); // check startup script didn't change permissions
    }

    public void test50Remove() {
        assumeThat(installation, is(notNullValue()));

        remove(distribution());

        // removing must stop the service
        final Shell sh = new Shell();
        assertThat(sh.run("ps aux").stdout, not(containsString("org.elasticsearch.bootstrap.Elasticsearch")));

        if (isSystemd()) {
            assertThat(sh.runIgnoreExitCode("systemctl status elasticsearch.service").exitCode, is(3));
            assertThat(sh.runIgnoreExitCode("systemctl is-enabled elasticsearch.service").exitCode, is(1));
        }

        assertPathsDontExist(
            installation.bin,
            installation.lib,
            installation.modules,
            installation.plugins,
            installation.logs,
            installation.pidDir
        );

        assertTrue(Files.exists(SYSTEMD_SERVICE));
    }

    public void test60Reinstall() {
        assumeThat(installation, is(notNullValue()));

        installation = install(distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution());

        remove(distribution());
        assertRemoved(distribution());
    }
}
