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
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.Shell.Result;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.packaging.util.FileUtils.assertPathsDontExist;
import static org.elasticsearch.packaging.util.FileUtils.mv;
import static org.elasticsearch.packaging.util.Packages.SYSTEMD_SERVICE;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.install;
import static org.elasticsearch.packaging.util.Packages.remove;
import static org.elasticsearch.packaging.util.Packages.startElasticsearch;
import static org.elasticsearch.packaging.util.Packages.stopElasticsearch;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.Platforms.getOsRelease;
import static org.elasticsearch.packaging.util.Platforms.isSystemd;
import static org.elasticsearch.packaging.util.ServerUtils.runElasticsearchTests;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class PackageTestCase extends PackagingTestCase {

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

    public void assertRunsWithJavaHome() throws IOException {
        Shell sh = new Shell();

        String systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
        byte[] originalEnvFile = Files.readAllBytes(installation.envFile);
        try {
            Files.write(installation.envFile, ("JAVA_HOME=" + systemJavaHome + "\n").getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.APPEND);
            startElasticsearch();
            runElasticsearchTests();
            stopElasticsearch();
        } finally {
            Files.write(installation.envFile, originalEnvFile);
        }

        Path log = installation.logs.resolve("elasticsearch.log");
        assertThat(new String(Files.readAllBytes(log), StandardCharsets.UTF_8), containsString(systemJavaHome));
    }

    public void test31JavaHomeOverride() throws IOException {
        assumeThat(installation, is(notNullValue()));

        assertRunsWithJavaHome();
    }

    public void test42BundledJdkRemoved() throws IOException {
        assumeThat(installation, is(notNullValue()));

        Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");
        try {
            mv(installation.bundledJdk, relocatedJdk);
            assertRunsWithJavaHome();
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
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

            final int statusExitCode;

            // Before version 231 systemctl returned exit code 3 for both services that were stopped, and nonexistent
            // services [1]. In version 231 and later it returns exit code 4 for non-existent services.
            //
            // The exception is Centos 7 and oel 7 where it returns exit code 4 for non-existent services from a systemd reporting a version
            // earlier than 231. Centos 6 does not have an /etc/os-release, but that's fine because it also doesn't use systemd.
            //
            // [1] https://github.com/systemd/systemd/pull/3385
            if (getOsRelease().contains("ID=\"centos\"") || getOsRelease().contains("ID=\"ol\"")) {
                statusExitCode = 4;
            } else {

                final Result versionResult = sh.run("systemctl --version");
                final Matcher matcher = Pattern.compile("^systemd (\\d+)\n").matcher(versionResult.stdout);
                matcher.find();
                final int version = Integer.parseInt(matcher.group(1));

                statusExitCode = version < 231
                    ? 3
                    : 4;
            }

            assertThat(sh.runIgnoreExitCode("systemctl status elasticsearch.service").exitCode, is(statusExitCode));
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

        assertFalse(Files.exists(SYSTEMD_SERVICE));
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
