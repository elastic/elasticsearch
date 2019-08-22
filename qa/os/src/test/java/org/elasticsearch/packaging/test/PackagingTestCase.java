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

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;

import java.nio.file.Paths;

import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Class that all packaging test cases should inherit from
 */
@RunWith(RandomizedRunner.class)
@TestMethodProviders({
    JUnit3MethodProvider.class
})
@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class PackagingTestCase extends Assert {

    protected final Log logger = LogFactory.getLog(getClass());

    // the distribution being tested
    protected static final Distribution distribution;
    static {
        distribution = new Distribution(Paths.get(System.getProperty("tests.distribution")));
    }

    // the java installation already installed on the system
    protected static final String systemJavaHome;
    static {
        Shell sh = new Shell();
        if (Platforms.LINUX) {
            systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
        } else {
            assert Platforms.WINDOWS;
            systemJavaHome = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
        }
    }

    // the current installation of the distribution being tested
    protected static Installation installation;

    private static boolean failed;

    @ClassRule
    public static final TestWatcher testFailureRule = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            failed = true;
        }
    };

    // a shell to run system commands with
    protected Shell sh;

    @Rule
    public final TestName testNameRule = new TestName();

    @BeforeClass
    public static void filterCompatible() {
        assumeTrue("only compatible distributions", distribution.packaging.compatible);
    }

    @BeforeClass
    public static void cleanup() throws Exception {
        installation = null;
        cleanEverything();
    }

    @Before
    public void setup() throws Exception {
        assumeFalse(failed); // skip rest of tests once one fails

        sh = newShell();
    }

    /** The {@link Distribution} that should be tested in this case */
    protected static Distribution distribution() {
        return distribution;
    }

    protected Shell newShell() throws Exception {
        Shell sh = new Shell();
        if (distribution().hasJdk == false) {
            Platforms.onLinux(() -> {
                sh.getEnv().put("JAVA_HOME", systemJavaHome);
            });
            Platforms.onWindows(() -> {
                sh.getEnv().put("JAVA_HOME", systemJavaHome);
            });
        }
        return sh;
    }

}
