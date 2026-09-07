/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ProcessInfo;
import org.junit.BeforeClass;

import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.assumeTrue;
import static org.hamcrest.Matchers.equalTo;

// tests for how the linux distro interacts with the OS
public class LinuxSystemTests extends PackagingTestCase {

    @BeforeClass
    public static void ensureLinux() {
        assumeTrue(Platforms.LINUX);
    }

    public void test10Install() throws Exception {
        install();
    }

    public void test20CoredumpFilter() throws Exception {
        startElasticsearch();

        // find the Elasticsearch process
        int esPid = -1;
        List<ProcessInfo> procs = ProcessInfo.getProcessInfo(sh, "java");
        for (ProcessInfo proc : procs) {
            if (proc.commandLine().contains("org.elasticsearch.bootstrap.Elasticsearch")) {
                esPid = proc.pid();
            }
        }
        if (esPid == -1) {
            fail("Could not find Elasticsearch process, existing processes:\n" + procs);
        }

        // check the coredump filter was set correctly
        String coredumpFilter = sh.run("cat /proc/" + esPid + "/coredump_filter").stdout().trim();
        assertThat(coredumpFilter, equalTo("00000023"));
    }

}
