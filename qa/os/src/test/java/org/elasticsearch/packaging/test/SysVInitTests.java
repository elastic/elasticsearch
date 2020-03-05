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

import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsExist;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class SysVInitTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("rpm or deb", distribution.isPackage());
        assumeTrue(Platforms.isSysVInit());
        assumeFalse(Platforms.isSystemd());
    }

    @Override
    public void startElasticsearch() throws Exception {
        sh.run("service elasticsearch start");
        ServerUtils.waitForElasticsearch(installation);
        sh.run("service elasticsearch status");
    }

    @Override
    public void stopElasticsearch() {
        sh.run("service elasticsearch stop");
    }

    public void test10Install() throws Exception {
        install();
    }

    public void test20Start() throws Exception {
        startElasticsearch();
        assertThat(installation.logs.resolve("gc.log"), fileExists());
        ServerUtils.runElasticsearchTests();
        sh.run("service elasticsearch status"); // returns 0 exit status when ok
    }

    public void test21Restart() throws Exception {
        sh.run("service elasticsearch restart");
        sh.run("service elasticsearch status"); // returns 0 exit status when ok
    }

    public void test22Stop() throws Exception {
        stopElasticsearch();
        Shell.Result status = sh.runIgnoreExitCode("service elasticsearch status");
        assertThat(status.exitCode, anyOf(equalTo(3), equalTo(4)));
    }

    public void test30PidDirCreation() throws Exception {
        // Simulates the behavior of a system restart:
        // the PID directory is deleted by the operating system
        // but it should not block ES from starting
        // see https://github.com/elastic/elasticsearch/issues/11594

        sh.run("rm -rf " + installation.pidDir);
        startElasticsearch();
        assertPathsExist(installation.pidDir.resolve("elasticsearch.pid"));
        stopElasticsearch();
    }

    public void test31MaxMapTooSmall() throws Exception {
        sh.run("sysctl -q -w vm.max_map_count=262140");
        startElasticsearch();
        Shell.Result result = sh.run("sysctl -n vm.max_map_count");
        String maxMapCount = result.stdout.strip();
        sh.run("service elasticsearch stop");
        assertThat(maxMapCount, equalTo("262144"));
    }

    public void test32MaxMapBigEnough() throws Exception {
        // Ensures that if $MAX_MAP_COUNT is greater than the set
        // value on the OS we do not attempt to update it.
        sh.run("sysctl -q -w vm.max_map_count=262145");
        startElasticsearch();
        Shell.Result result = sh.run("sysctl -n vm.max_map_count");
        String maxMapCount = result.stdout.strip();
        sh.run("service elasticsearch stop");
        assertThat(maxMapCount, equalTo("262145"));
    }

}
