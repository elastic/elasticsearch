/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assume.assumeFalse;

public class CronEvalCliTests extends PackagingTestCase {

    @Before
    public void filterDistros() {
        assumeFalse("no docker", distribution.isDocker());
    }

    public void test10Install() throws Exception {
        install();
    }

    public void test20Help() throws Exception {
        Shell.Result result = installation.executables().cronevalTool.run("--help");
        assertThat(result.stdout, containsString("Validates and evaluates a cron expression"));
    }

    public void test30Run() throws Exception {
        Shell.Result result = installation.executables().cronevalTool.run("'0 0 20 ? * MON-THU' -c 2");
        assertThat(result.stdout, containsString("Valid!"));
    }
}
