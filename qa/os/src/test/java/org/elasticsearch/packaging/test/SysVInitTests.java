/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import static org.elasticsearch.packaging.util.FileUtils.assertPathsExist;
import static org.elasticsearch.packaging.util.FileUtils.fileWithGlobExist;
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
        assertThat(installation.logs, fileWithGlobExist("gc.log*"));
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
        String maxMapCount = result.stdout.trim();
        sh.run("service elasticsearch stop");
        assertThat(maxMapCount, equalTo("262144"));
    }

    public void test32MaxMapBigEnough() throws Exception {
        // Ensures that if $MAX_MAP_COUNT is greater than the set
        // value on the OS we do not attempt to update it.
        sh.run("sysctl -q -w vm.max_map_count=262145");
        startElasticsearch();
        Shell.Result result = sh.run("sysctl -n vm.max_map_count");
        String maxMapCount = result.stdout.trim();
        sh.run("service elasticsearch stop");
        assertThat(maxMapCount, equalTo("262145"));
    }

}
