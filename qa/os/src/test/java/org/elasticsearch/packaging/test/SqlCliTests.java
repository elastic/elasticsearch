/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assume.assumeTrue;

public class SqlCliTests extends PackagingTestCase {
    @Before
    public void filterDistros() {
        assumeTrue("only default distro", distribution.flavor == Distribution.Flavor.DEFAULT);
    }

    public void test010Install() throws Exception {
        install();
    }

    public void test020Help() throws Exception {
        Shell.Result result = installation.executables().sqlCli.run("--help");
        assertThat(result.stdout, containsString("Elasticsearch SQL CLI"));
    }
}
