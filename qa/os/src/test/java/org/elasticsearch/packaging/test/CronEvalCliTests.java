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
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assume.assumeTrue;

public class CronEvalCliTests extends PackagingTestCase {

    @Before
    public void filterDistros() {
        assumeTrue("only default distro", distribution.flavor == Distribution.Flavor.DEFAULT);
        assumeTrue("no docker", distribution.packaging != Distribution.Packaging.DOCKER);
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
