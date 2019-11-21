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
import com.carrotsearch.randomizedtesting.annotations.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Docker;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Packages;
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

import java.nio.file.Files;
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
@Timeout(millis = 20 * 60 * 1000) // 20 min
@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class PackagingTestCase extends Assert {

    protected final Logger logger =  LogManager.getLogger(getClass());

    // the distribution being tested
    protected static final Distribution distribution;
    static {
        distribution = new Distribution(Paths.get(System.getProperty("tests.distribution")));
    }

    // the java installation already installed on the system
    protected static final String systemJavaHome;
    static {
        Shell sh = new Shell();
        if (Platforms.WINDOWS) {
            systemJavaHome = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
        } else {
            assert Platforms.LINUX || Platforms.DARWIN;
            systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
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

    protected static void install() throws Exception {
        switch (distribution.packaging) {
            case TAR:
            case ZIP:
                installation = Archives.installArchive(distribution);
                Archives.verifyArchiveInstallation(installation, distribution);
                break;
            case DEB:
            case RPM:
                installation = Packages.installPackage(distribution);
                Packages.verifyPackageInstallation(installation, distribution, newShell());
                break;
            case DOCKER:
                installation = Docker.runContainer(distribution);
                Docker.verifyContainerInstallation(installation, distribution);
        }
    }

    protected void assertWhileRunning(Platforms.PlatformAction assertions) throws Exception {
        try {
            switch (distribution.packaging) {
                case TAR:
                case ZIP:
                    Archives.runElasticsearch(installation, sh);
                    break;
                case DEB:
                case RPM:
                    Packages.startElasticsearch(sh, installation);
                    break;
                case DOCKER:
                    // nothing, "installing" docker image is running it
            }

        } catch (Exception e ){
            if (Files.exists(installation.home.resolve("elasticsearch.pid"))) {
                String pid = FileUtils.slurp(installation.home.resolve("elasticsearch.pid")).trim();
                logger.info("Dumping jstack of elasticsearch processb ({}) that failed to start", pid);
                sh.runIgnoreExitCode("jstack " + pid);
            }
            if (Files.exists(installation.logs.resolve("elasticsearch.log"))) {
                logger.warn("Elasticsearch log:\n" +
                    FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"));
            }
            throw e;
        }

        try {
            assertions.run();
        } catch (Exception e) {
            logger.warn("Elasticsearch log:\n" +
                FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"));
            throw e;
        }

        switch (distribution.packaging) {
            case TAR:
            case ZIP:
                Archives.stopElasticsearch(installation);
                break;
            case DEB:
            case RPM:
                Packages.stopElasticsearch(sh);
                break;
            case DOCKER:
                // nothing, removing container is handled externally
        }
    }

    protected static Shell newShell() throws Exception {
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
