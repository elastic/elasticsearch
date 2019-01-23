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
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.stopElasticsearch;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.FileUtils.slurp;
import static org.elasticsearch.packaging.util.Packages.assertStatuses;
import static org.elasticsearch.packaging.util.Packages.maskSysctl;
import static org.elasticsearch.packaging.util.Packages.recreateTempFiles;
import static org.elasticsearch.packaging.util.Packages.restartElasticsearch;
import static org.elasticsearch.packaging.util.Packages.startElasticsearch;
import static org.elasticsearch.packaging.util.Packages.unmaskSysctl;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.Platforms.isSystemd;
import static org.elasticsearch.packaging.util.ServerUtils.runElasticsearchTests;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class ServiceManagerTestCase extends PackagingTestCase {


    @Before
    public void init() {
        //unless 70_sysv_initd.bats is migrated this should only run for systemd
        assumeTrue(isSystemd());
    }

    public void test10Install() {
        installation = installArchive(distribution());
        verifyArchiveInstallation(installation, distribution());
    }

    public void test20StartServer() throws IOException {
        assumeThat(installation, is(notNullValue()));

        startElasticsearch();
        runElasticsearchTests();
        verifyPackageInstallation(installation, distribution()); // check startup script didn't change permissions
    }

    public void test30RestartServer() throws IOException {
        restartElasticsearch();
        runElasticsearchTests();
    }

    public void test40StopServer() {
        stopElasticsearch(installation);
        assertStatuses(); // non deterministic
    }

    public void test45StopServerAgain() {
        stopElasticsearch(installation);
        assertStatuses(); // non deterministic
    }

    public void test50ManualStartup() throws IOException {
        rm(installation.pidDir);

        recreateTempFiles();

        startElasticsearch();

        final Path pidFile = installation.home.resolve("elasticsearch.pid");

        assertTrue(Files.exists(pidFile));
    }

    public void test60SystemdMask() {
        cleanup();

        maskSysctl();

        installation = installArchive(distribution());

        unmaskSysctl();
    }

    public void test70serviceFileSetsLimits() throws IOException {
        cleanup();

        installation = installArchive(distribution());

        startElasticsearch();

        final Path pidFile = installation.home.resolve("elasticsearch.pid");
        assertTrue(Files.exists(pidFile));
        String pid = slurp(pidFile).trim();


    }

}
