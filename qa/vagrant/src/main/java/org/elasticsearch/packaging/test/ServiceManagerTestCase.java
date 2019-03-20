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
import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.Shell;
import org.hamcrest.CoreMatchers;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.cp;
import static org.elasticsearch.packaging.util.FileUtils.getTempDir;
import static org.elasticsearch.packaging.util.FileUtils.mkdir;
import static org.elasticsearch.packaging.util.FileUtils.mv;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.FileUtils.slurp;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.install;
import static org.elasticsearch.packaging.util.Packages.startElasticsearch;
import static org.elasticsearch.packaging.util.Packages.stopElasticsearch;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.Platforms.isSystemd;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.elasticsearch.packaging.util.ServerUtils.runElasticsearchTests;
import static org.elasticsearch.packaging.util.ServerUtils.waitForElasticsearch;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public class ServiceManagerTestCase extends PackagingTestCase {
    @Override
    protected Distribution distribution() {
        return Distribution.DEFAULT_DEB;
    }

    @Before
    public void init() {
        assumeTrue("only compatible distributions", distribution().packaging.compatible);

        //unless 70_sysv_initd.bats is migrated this should only run for systemd
        assumeTrue(isSystemd());
    }

    public void test10Install() throws IOException {
        assertRemoved(distribution());
        installation = install(distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), newShell());
    }

    public void test20StartServer() throws IOException {
        assumeThat(installation, is(notNullValue()));

        /**
         * systemctl daemon-reload
         * enable
         * is enabled
         * start
         * is-active
         * status
         * run-tests
         */
        startElasticsearch(newShell());

        runElasticsearchTests();
        verifyPackageInstallation(installation, distribution(), newShell()); // check startup script didn't change permissions
    }

    public void test30RestartServer() throws IOException {
        restartElasticsearch();
        runElasticsearchTests();
        stopElasticsearch(newShell());
    }

//    public void test40StopServer() throws IOException {
//        stopElasticsearch();
//        assertStatuses(); // non deterministic
//    }
//
//    public void test45StopServerAgain() {
//        stopElasticsearch(installation);
//        assertStatuses(); // non deterministic
//    }

    /**
     * # Simulates the behavior of a system restart:
     * # the PID directory is deleted by the operating system
     * # but it should not block ES from starting
     * # see https://github.com/elastic/elasticsearch/issues/11594
     */
    public void test50DeletePID_DIRandRestart() throws IOException {
        rm(installation.pidDir);

        recreateTempFiles();

        startElasticsearch(newShell());

        final Path pidFile = installation.pidDir.resolve("elasticsearch.pid");

        assertTrue(Files.exists(pidFile));
    }

//

    public void test70CustomPathConfAndJvmOptions() throws IOException {
        assumeThat(installation, CoreMatchers.is(notNullValue()));
        FileUtils.assertPathsExist(installation.envFile);

        /**
         //         * # The custom config directory is not under /tmp or /var/tmp because
         //         * # systemd's private temp directory functionally means different
         //         * # processes can have different views of what's in these directories
         //         */
        Shell sh = newShell();
        String temp = sh.runIgnoreExitCode("mktemp -p /etc -d").stdout.trim();
        final Path tempConf = Paths.get(temp);
        System.out.println(tempConf);
        System.out.println(installation.envFile);

        try {
            mkdir(tempConf);
            cp(installation.config("elasticsearch.yml"), tempConf.resolve("elasticsearch.yml"));
            cp(installation.config("log4j2.properties"), tempConf.resolve("log4j2.properties"));

            // we have to disable Log4j from using JMX lest it will hit a security
            // manager exception before we have configured logging; this will fail
            // startup since we detect usages of logging before it is configured
            final String jvmOptions =
                "-Xms512m\n" +
                    "-Xmx512m\n" +
                    "-Dlog4j2.disable.jmx=true\n";
            append(tempConf.resolve("jvm.options"), jvmOptions);

            sh.runIgnoreExitCode("chown -R elasticsearch:elasticsearch " + tempConf);

            final Shell serverShell = newShell();
//            serverShell.getEnv().put("ES_PATH_CONF", tempConf.toString());
//            serverShell.getEnv().put("ES_JAVA_OPTS", "-XX:-UseCompressedOops");
            cp(installation.envFile,tempConf.resolve("elasticsearch"));//backup
            append(installation.envFile,"ES_PATH_CONF="+tempConf+"\n");
            append(installation.envFile,"ES_JAVA_OPTS=-XX:-UseCompressedOops");

            startElasticsearch(serverShell);

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));

            stopElasticsearch(serverShell);

        } finally {
//            rm(installation.envFile);
//            cp(tempConf.resolve("elasticsearch"), installation.envFile);
           // rm(tempConf);
        }
    }

//    public void test60SystemdMask() throws IOException {
//        cleanup();
//
//        maskSysctl();
//
//        installation = install(distribution());
//
//        unmaskSysctl();
//    }
//
//    public void test70serviceFileSetsLimits() throws IOException {
//        final Shell sh = newShell();
//
//        cleanup();
//
//        installation = install(distribution());
//
//        startElasticsearch(newShell());
//
//        final Path pidFile = installation.pidDir.resolve("elasticsearch.pid");
//        assertTrue(Files.exists(pidFile));
//        String pid = slurp(pidFile).trim();
//        String maxFileSize = run(sh, "cat /proc/%s/limits | grep \"Max file size\" | awk '{ print $4 }'", pid);
//        assertThat(maxFileSize, equalTo("unlimited"));
//
//        String maxProcesses = run(sh, "cat /proc/%s/limits | grep \"Max processes\" | awk '{ print $3 }'", pid);
//        assertThat(maxProcesses, equalTo("4096"));
//
//        String maxOpenFiles = run(sh, "cat /proc/%s/limits | grep \"Max open files\" | awk '{ print $4 }'", pid);
//        assertThat(maxOpenFiles, equalTo("65535"));
//
//        String maxAddressSpace = run(sh, "cat /proc/%s/limits | grep \"Max address space\" | awk '{ print $4 }'", pid);
//        assertThat(maxAddressSpace, equalTo("unlimited"));
//
//        stopElasticsearch(newShell());
//    }
//
//    public void test80TestRuntimeDirectory() throws IOException {
//        cleanup();
//        installation = install(distribution());
//        FileUtils.rm(Paths.get("/var/run/elasticsearch"));
//        startElasticsearch(newShell());
//        FileUtils.assertPathsExist(Paths.get("/var/run/elasticsearch"));
//        stopElasticsearch(newShell());
//    }
//
//    public void test90gcLogsExist() throws IOException {
//        cleanup();
//        installation = install(distribution());
//        startElasticsearch(newShell());
//        //somehow it is not .0.current when running test?
//        FileUtils.assertPathsExist(Paths.get("/var/log/elasticsearch/gc.log"));
//        stopElasticsearch(newShell());
//    }

    private String run(Shell sh, String command, Object... args) {
        String formattedCommand = String.format(Locale.ROOT, command, args);
        return sh.run(formattedCommand).stdout.trim();
    }

    public void restartElasticsearch() throws IOException {
        final Shell sh = newShell();
        if (isSystemd()) {
            sh.run("systemctl restart elasticsearch.service");
        }
//        } else {
//            sh.run("service elasticsearch start");
//        }

        waitForElasticsearch();

        if (isSystemd()) {
            sh.run("systemctl is-active elasticsearch.service");
            sh.run("systemctl status elasticsearch.service");
        }
//        else {
//            sh.run("service elasticsearch status");
//        }
    }


    //this will be very non deterministic
    public void assertStatuses() {
        final Shell sh = newShell();
        if (isSystemd()) {
            sh.run("systemctl status elasticsearch.service");
        }
    }

    public void recreateTempFiles() {
        final Shell sh = newShell();
        //only systemd
        if (isSystemd()) {
            sh.run("systemd-tmpfiles --create");
        }
    }

    public void maskSysctl() {
        final Shell sh = newShell();
        if (isSystemd()) {
            sh.run("systemctl mask systemd-sysctl.service");
        }
    }


    public void unmaskSysctl() {
        final Shell sh = newShell();
        if (isSystemd()) {
            sh.run("systemctl mask systemd-sysctl.service");
        }
    }

}
