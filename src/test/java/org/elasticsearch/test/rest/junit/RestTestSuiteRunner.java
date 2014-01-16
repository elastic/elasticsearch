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
package org.elasticsearch.test.rest.junit;

import com.carrotsearch.hppc.hash.MurmurHash3;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.Randomness;
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.google.common.collect.Lists;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.rest.RestTestExecutionContext;
import org.elasticsearch.test.rest.client.RestException;
import org.elasticsearch.test.rest.client.RestResponse;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.elasticsearch.test.rest.parser.RestTestSuiteParser;
import org.elasticsearch.test.rest.section.DoSection;
import org.elasticsearch.test.rest.section.ExecutableSection;
import org.elasticsearch.test.rest.section.RestTestSuite;
import org.elasticsearch.test.rest.section.TestSection;
import org.elasticsearch.test.rest.spec.RestSpec;
import org.elasticsearch.test.rest.support.FileUtils;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.carrotsearch.randomizedtesting.SeedUtils.parseSeedChain;
import static com.carrotsearch.randomizedtesting.StandaloneRandomizedContext.*;
import static com.carrotsearch.randomizedtesting.SysGlobals.*;
import static org.elasticsearch.test.TestCluster.SHARED_CLUSTER_SEED;
import static org.elasticsearch.test.TestCluster.clusterName;
import static org.elasticsearch.test.rest.junit.DescriptionHelper.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * JUnit runner for elasticsearch REST tests
 *
 * Supports the following options provided as system properties:
 * - tests.rest[true|false|host:port]: determines whether the REST tests need to be run and if so
 *                                     whether to rely on an external cluster (providing host and port) or fire a test cluster (default)
 * - tests.rest.suite: comma separated paths of the test suites to be run (by default loaded from /rest-api-spec/test)
 *                     it is possible to run only a subset of the tests providing a directory or a single yaml file
 *                     (the default /rest-api-spec/test prefix is optional when files are loaded from classpath)
 * - tests.rest.spec: REST spec path (default /rest-api-spec/api)
 * - tests.iters: runs multiple iterations
 * - tests.seed: seed to base the random behaviours on
 * - tests.appendseed[true|false]: enables adding the seed to each test section's description (default false)
 * - tests.cluster_seed: seed used to create the test cluster (if enabled)
 *
 */
public class RestTestSuiteRunner extends ParentRunner<RestTestCandidate> {

    private static final ESLogger logger = Loggers.getLogger(RestTestSuiteRunner.class);

    public static final String REST_TESTS_MODE = "tests.rest";
    public static final String REST_TESTS_SUITE = "tests.rest.suite";
    public static final String REST_TESTS_SPEC = "tests.rest.spec";

    private static final String DEFAULT_TESTS_PATH = "/rest-api-spec/test";
    private static final String DEFAULT_SPEC_PATH = "/rest-api-spec/api";
    private static final int DEFAULT_ITERATIONS = 1;

    private static final String PATHS_SEPARATOR = ",";

    private final RestTestExecutionContext restTestExecutionContext;
    private final List<RestTestCandidate> restTestCandidates;
    private final Description rootDescription;

    private final RunMode runMode;

    private final TestCluster testCluster;

    private static final AtomicInteger sequencer = new AtomicInteger();

    /** The runner's seed (master). */
    private final Randomness runnerRandomness;

    /**
     * If {@link com.carrotsearch.randomizedtesting.SysGlobals#SYSPROP_RANDOM_SEED} property is used with two arguments
     * (master:test_section) then this field contains test section level override.
     */
    private final Randomness testSectionRandomnessOverride;

    enum RunMode {
        NO, TEST_CLUSTER, EXTERNAL_CLUSTER
    }

    static RunMode runMode() {
        String mode = System.getProperty(REST_TESTS_MODE);
        if (!Strings.hasLength(mode)) {
            //default true: we run the tests starting our own test cluster
            mode = Boolean.TRUE.toString();
        }

        if (Boolean.FALSE.toString().equalsIgnoreCase(mode)) {
            return RunMode.NO;
        }
        if (Boolean.TRUE.toString().equalsIgnoreCase(mode)) {
            return RunMode.TEST_CLUSTER;
        }
        return RunMode.EXTERNAL_CLUSTER;
    }

    public RestTestSuiteRunner(Class<?> testClass) throws InitializationError {
        super(testClass);

        this.runMode = runMode();

        if (runMode == RunMode.NO) {
            //the tests won't be run. the run method will be called anyway but we'll just mark the whole suite as ignored
            //no need to go ahead and parse the test suites then
            this.runnerRandomness = null;
            this.testSectionRandomnessOverride = null;
            this.restTestExecutionContext = null;
            this.restTestCandidates = null;
            this.rootDescription = createRootDescription(getRootSuiteTitle());
            this.rootDescription.addChild(createApiDescription("empty suite"));
            this.testCluster = null;
            return;
        }

        //the REST test suite is supposed to be run only once per jvm against either an external es node or a self started one
        if (sequencer.getAndIncrement() > 0) {
            throw new InitializationError("only one instance of RestTestSuiteRunner can be created per jvm");
        }

        //either read the seed from system properties (first one in the chain) or generate a new one
        final String globalSeed = System.getProperty(SYSPROP_RANDOM_SEED());
        final long initialSeed;
        Randomness randomnessOverride = null;
        if (Strings.hasLength(globalSeed)) {
            final long[] seedChain = parseSeedChain(globalSeed);
            if (seedChain.length == 0 || seedChain.length > 2) {
                throw new IllegalArgumentException("Invalid system property "
                        + SYSPROP_RANDOM_SEED() + " specification: " + globalSeed);
            }
            if (seedChain.length > 1) {
                //read the test section level seed if present
                randomnessOverride = new Randomness(seedChain[1]);
            }
            initialSeed = seedChain[0];
        } else {
            initialSeed = MurmurHash3.hash(System.nanoTime());
        }
        this.runnerRandomness = new Randomness(initialSeed);
        this.testSectionRandomnessOverride = randomnessOverride;
        logger.info("Master seed: {}", SeedUtils.formatSeed(initialSeed));

        String host;
        int port;
        if (runMode == RunMode.TEST_CLUSTER) {
            this.testCluster = new TestCluster(SHARED_CLUSTER_SEED, 1, clusterName("REST-tests", ElasticsearchTestCase.CHILD_VM_ID, SHARED_CLUSTER_SEED));
            this.testCluster.beforeTest(runnerRandomness.getRandom(), 0.0f);
            HttpServerTransport httpServerTransport = testCluster.getInstance(HttpServerTransport.class);
            InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) httpServerTransport.boundAddress().publishAddress();
            host = inetSocketTransportAddress.address().getHostName();
            port = inetSocketTransportAddress.address().getPort();
        } else {
            this.testCluster = null;
            String testsMode = System.getProperty(REST_TESTS_MODE);
            String[] split = testsMode.split(":");
            if (split.length < 2) {
                throw new InitializationError("address [" + testsMode + "] not valid");
            }
            host = split[0];
            try {
                port = Integer.valueOf(split[1]);
            } catch(NumberFormatException e) {
                throw new InitializationError("port is not valid, expected number but was [" + split[1] + "]");
            }
        }

        try {
            String[] specPaths = resolvePathsProperty(REST_TESTS_SPEC, DEFAULT_SPEC_PATH);
            RestSpec restSpec = RestSpec.parseFrom(DEFAULT_SPEC_PATH, specPaths);

            this.restTestExecutionContext = new RestTestExecutionContext(host, port, restSpec);
            this.rootDescription = createRootDescription(getRootSuiteTitle());
            this.restTestCandidates = collectTestCandidates(rootDescription);
        } catch (InitializationError e) {
          stopTestCluster();
            throw e;
        } catch (Throwable e) {
            stopTestCluster();
            throw new InitializationError(e);
        }
    }

    /**
     * Parse the test suites and creates the test candidates to be run, together with their junit descriptions.
     * The descriptions will be part of a tree containing api/yaml file/test section/eventual multiple iterations.
     * The test candidates will be instead flattened out to the leaves level (iterations), the part that needs to be run.
     */
    protected List<RestTestCandidate> collectTestCandidates(Description rootDescription) throws InitializationError, IOException {

        String[] paths = resolvePathsProperty(REST_TESTS_SUITE, DEFAULT_TESTS_PATH);
        Map<String, Set<File>> yamlSuites = FileUtils.findYamlSuites(DEFAULT_TESTS_PATH, paths);

        int iterations = determineTestSectionIterationCount();
        boolean appendSeedParameter = RandomizedTest.systemPropertyAsBoolean(SYSPROP_APPEND_SEED(), false);

        //we iterate over the files and we shuffle them (grouped by api, and by yaml file)
        //meanwhile we create the junit descriptions and test candidates (one per iteration)

        //yaml suites are grouped by directory (effectively by api)
        List<String> apis = Lists.newArrayList(yamlSuites.keySet());
        Collections.shuffle(apis, runnerRandomness.getRandom());

        final boolean fixedSeed = testSectionRandomnessOverride != null;
        final boolean hasRepetitions = iterations > 1;

        List<Throwable> parseExceptions = Lists.newArrayList();
        List<RestTestCandidate> testCandidates = Lists.newArrayList();
        RestTestSuiteParser restTestSuiteParser = new RestTestSuiteParser();
        for (String api : apis) {

            Description apiDescription = createApiDescription(api);
            rootDescription.addChild(apiDescription);

            List<File> yamlFiles = Lists.newArrayList(yamlSuites.get(api));
            Collections.shuffle(yamlFiles, runnerRandomness.getRandom());

            for (File yamlFile : yamlFiles) {
                RestTestSuite restTestSuite;
                try {
                    restTestSuite = restTestSuiteParser.parse(restTestExecutionContext.esVersion(), api, yamlFile);
                } catch (RestTestParseException e) {
                    parseExceptions.add(e);
                    //we continue so that we collect all parse errors and show them all at once
                    continue;
                }

                Description testSuiteDescription = createTestSuiteDescription(restTestSuite);
                apiDescription.addChild(testSuiteDescription);

                if (restTestSuite.getTestSections().size() == 0) {
                    assert restTestSuite.getSetupSection().getSkipSection().skipVersion(restTestExecutionContext.esVersion());
                    testCandidates.add(RestTestCandidate.empty(restTestSuite, testSuiteDescription));
                    continue;
                }

                Collections.shuffle(restTestSuite.getTestSections(), runnerRandomness.getRandom());

                for (TestSection testSection : restTestSuite.getTestSections()) {

                    //no need to generate seed if we are going to skip the test section
                    if (testSection.getSkipSection().skipVersion(restTestExecutionContext.esVersion())) {
                        Description testSectionDescription = createTestSectionIterationDescription(restTestSuite, testSection, null);
                        testSuiteDescription.addChild(testSectionDescription);
                        testCandidates.add(new RestTestCandidate(restTestSuite, testSuiteDescription, testSection, testSectionDescription, -1));
                        continue;
                    }

                    Description parentDescription;
                    if (hasRepetitions) {
                        //additional level to group multiple iterations under the same test section's node
                        parentDescription = createTestSectionWithRepetitionsDescription(restTestSuite, testSection);
                        testSuiteDescription.addChild(parentDescription);
                    } else {
                        parentDescription = testSuiteDescription;
                    }

                    final long testSectionSeed = determineTestSectionSeed(restTestSuite.getDescription() + "/" + testSection.getName());
                    for (int i = 0; i < iterations; i++) {
                        //test section name argument needs to be unique here
                        long thisSeed = (fixedSeed ? testSectionSeed : testSectionSeed ^ MurmurHash3.hash((long) i));

                        final LinkedHashMap<String, Object> args = new LinkedHashMap<String, Object>();
                        if (hasRepetitions) {
                            args.put("#", i);
                        }
                        if (hasRepetitions || appendSeedParameter) {
                            args.put("seed=", SeedUtils.formatSeedChain(runnerRandomness, new Randomness(thisSeed)));
                        }

                        Description testSectionDescription = createTestSectionIterationDescription(restTestSuite, testSection, args);
                        parentDescription.addChild(testSectionDescription);
                        testCandidates.add(new RestTestCandidate(restTestSuite, testSuiteDescription, testSection, testSectionDescription, thisSeed));
                    }
                }
            }
        }

        if (!parseExceptions.isEmpty()) {
            throw new InitializationError(parseExceptions);
        }

        return testCandidates;
    }

    protected String getRootSuiteTitle() {
        if (runMode == RunMode.NO) {
            return "elasticsearch REST Tests - not run";
        }
        if (runMode == RunMode.TEST_CLUSTER) {
            return String.format(Locale.ROOT, "elasticsearch REST Tests - test cluster %s", SeedUtils.formatSeed(SHARED_CLUSTER_SEED));
        }
        if (runMode == RunMode.EXTERNAL_CLUSTER) {
            return String.format(Locale.ROOT, "elasticsearch REST Tests - external cluster %s", System.getProperty(REST_TESTS_MODE));
        }
        throw new UnsupportedOperationException("runMode [" + runMode + "] not supported");
    }

    private int determineTestSectionIterationCount() {
        int iterations = RandomizedTest.systemPropertyAsInt(SYSPROP_ITERATIONS(), DEFAULT_ITERATIONS);
        if (iterations < 1) {
            throw new IllegalArgumentException("System property " + SYSPROP_ITERATIONS() + " must be >= 1 but was [" + iterations + "]");
        }
        return iterations;
    }

    protected static String[] resolvePathsProperty(String propertyName, String defaultValue) {
        String property = System.getProperty(propertyName);
        if (!Strings.hasLength(property)) {
            return new String[]{defaultValue};
        } else {
            return property.split(PATHS_SEPARATOR);
        }
    }

    /**
     * Determine a given test section's initial random seed
     */
    private long determineTestSectionSeed(String testSectionName) {
        if (testSectionRandomnessOverride != null) {
            return getSeed(testSectionRandomnessOverride);
        }

        // We assign each test section a different starting hash based on the global seed
        // and a hash of their name (so that the order of sections does not matter, only their names)
        return getSeed(runnerRandomness) ^ MurmurHash3.hash((long) testSectionName.hashCode());
    }

    @Override
    protected List<RestTestCandidate> getChildren() {
        return restTestCandidates;
    }

    @Override
    public Description getDescription() {
        return rootDescription;
    }

    @Override
    protected Description describeChild(RestTestCandidate child) {
        return child.describeTest();
    }

    @Override
    protected Statement classBlock(RunNotifier notifier) {
        //we remove support for @BeforeClass & @AfterClass and JUnit Rules (as we don't call super)
        Statement statement = childrenInvoker(notifier);
        statement = withExecutionContextClose(statement);
        if (testCluster != null) {
            return withTestClusterClose(statement);
        }
        return statement;
    }

    protected Statement withExecutionContextClose(Statement statement) {
        return new RunAfter(statement, new Statement() {
            @Override
            public void evaluate() throws Throwable {
                restTestExecutionContext.close();
            }
        });
    }

    protected Statement withTestClusterClose(Statement statement) {
        return new RunAfter(statement, new Statement() {
            @Override
            public void evaluate() throws Throwable {
                stopTestCluster();
            }
        });
    }

    @Override
    public void run(final RunNotifier notifier) {

        if (runMode == RunMode.NO) {
            notifier.fireTestIgnored(rootDescription.getChildren().get(0));
            return;
        }

        notifier.addListener(new RestReproduceInfoPrinter());

        //the test suite gets run on a separate thread as the randomized context is per thread
        //once the randomized context is disposed it's not possible to create it again on the same thread
        final Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    createRandomizedContext(getTestClass().getJavaClass(), runnerRandomness);
                    RestTestSuiteRunner.super.run(notifier);
                } finally {
                    disposeRandomizedContext();
                }
            }
        };

        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            notifier.fireTestFailure(new Failure(getDescription(),
                    new RuntimeException("Interrupted while waiting for the suite runner? Weird.", e)));
        }
    }

    @Override
    protected void runChild(RestTestCandidate testCandidate, RunNotifier notifier) {

        //if the while suite needs to be skipped, no test sections were loaded, only an empty one that we need to mark as ignored
        if (testCandidate.getSetupSection().getSkipSection().skipVersion(restTestExecutionContext.esVersion())) {
            logger.info("skipped test suite [{}]\nreason: {}\nskip versions: {} (current version: {})",
                    testCandidate.getSuiteDescription(), testCandidate.getSetupSection().getSkipSection().getReason(),
                    testCandidate.getSetupSection().getSkipSection().getVersion(), restTestExecutionContext.esVersion());

            notifier.fireTestIgnored(testCandidate.describeSuite());
            return;
        }

        //from now on no more empty test candidates are expected
        assert testCandidate.getTestSection() != null;

        if (testCandidate.getTestSection().getSkipSection().skipVersion(restTestExecutionContext.esVersion())) {
            logger.info("skipped test [{}/{}]\nreason: {}\nskip versions: {} (current version: {})",
                    testCandidate.getSuiteDescription(), testCandidate.getTestSection().getName(),
                    testCandidate.getTestSection().getSkipSection().getReason(),
                    testCandidate.getTestSection().getSkipSection().getVersion(), restTestExecutionContext.esVersion());

            notifier.fireTestIgnored(testCandidate.describeTest());
            return;
        }

        runLeaf(methodBlock(testCandidate), testCandidate.describeTest(), notifier);
    }

    protected Statement methodBlock(final RestTestCandidate testCandidate) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                final String testThreadName = "TEST-" + testCandidate.getSuiteDescription() +
                        "." + testCandidate.getTestSection().getName() + "-seed#" + SeedUtils.formatSeedChain(runnerRandomness);
                // This has a side effect of setting up a nested context for the test thread.
                final String restoreName = Thread.currentThread().getName();
                try {
                    Thread.currentThread().setName(testThreadName);
                    pushRandomness(new Randomness(testCandidate.getSeed()));
                    runTestSection(testCandidate);
                } finally {
                    Thread.currentThread().setName(restoreName);
                    popAndDestroy();
                }
            }
        };
    }

    protected void runTestSection(RestTestCandidate testCandidate)
            throws IOException, RestException {

        //let's check that there is something to run, otherwise there might be a problem with the test section
        if (testCandidate.getTestSection().getExecutableSections().size() == 0) {
            throw new IllegalArgumentException("No executable sections loaded for ["
                    + testCandidate.getSuiteDescription() + "/" + testCandidate.getTestSection().getName() + "]");
        }

        logger.info("cleaning up before test [{}: {}]", testCandidate.getSuiteDescription(), testCandidate.getTestSection().getName());
        tearDown();

        logger.info("start test [{}: {}]", testCandidate.getSuiteDescription(), testCandidate.getTestSection().getName());

        if (!testCandidate.getSetupSection().isEmpty()) {
            logger.info("start setup test [{}: {}]", testCandidate.getSuiteDescription(), testCandidate.getTestSection().getName());
            for (DoSection doSection : testCandidate.getSetupSection().getDoSections()) {
                doSection.execute(restTestExecutionContext);
            }
            logger.info("end setup test [{}: {}]", testCandidate.getSuiteDescription(), testCandidate.getTestSection().getName());
        }

        restTestExecutionContext.clear();

        for (ExecutableSection executableSection : testCandidate.getTestSection().getExecutableSections()) {
            executableSection.execute(restTestExecutionContext);
        }

        logger.info("end test [{}: {}]", testCandidate.getSuiteDescription(), testCandidate.getTestSection().getName());

        logger.info("cleaning up after test [{}: {}]", testCandidate.getSuiteDescription(), testCandidate.getTestSection().getName());
        tearDown();
    }

    private void tearDown() throws IOException, RestException {
        wipeIndices();
        wipeTemplates();
        restTestExecutionContext.clear();
    }

    private void wipeIndices() throws IOException, RestException {
        logger.debug("deleting all indices");
        RestResponse restResponse = restTestExecutionContext.callApiInternal("indices.delete", "index", "_all");
        assertThat(restResponse.getStatusCode(), equalTo(200));
    }

    @SuppressWarnings("unchecked")
    public void wipeTemplates() throws IOException, RestException {
        logger.debug("deleting all templates");
        //delete templates by wildcard was only added in 0.90.6
        //httpResponse = restTestExecutionContext.callApi("indices.delete_template", "name", "*");
        RestResponse restResponse = restTestExecutionContext.callApiInternal("cluster.state", "metric", "metadata");
        assertThat(restResponse.getStatusCode(), equalTo(200));
        Object object = restResponse.evaluate("metadata.templates");
        assertThat(object, instanceOf(Map.class));
        Set<String> templates = ((Map<String, Object>) object).keySet();
        for (String template : templates) {
            restResponse = restTestExecutionContext.callApiInternal("indices.delete_template", "name", template);
            assertThat(restResponse.getStatusCode(), equalTo(200));
        }
    }

    private void stopTestCluster() {
        if (runMode == RunMode.TEST_CLUSTER) {
            assert testCluster != null;
            testCluster.afterTest();
            testCluster.close();
        }
    }
}
