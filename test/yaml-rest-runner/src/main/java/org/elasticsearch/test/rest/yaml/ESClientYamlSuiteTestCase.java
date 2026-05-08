/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ClasspathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.rules.ErrorCollector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Runs a suite of yaml tests shared with all the official Elasticsearch
 * clients against an elasticsearch cluster.
 * <p>
 * The suite timeout is extended to account for projects with a large number of tests.
 * </p>
 */
@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
public abstract class ESClientYamlSuiteTestCase extends ESRestTestCase {

    /**
     * Property that allows to control which REST tests get run. Supports comma separated list of tests
     * or directories that contain tests e.g. -Dtests.rest.suite=index,get,create/10_with_id
     * <p>
     * A per-task variant {@code tests.rest.suite.<task path>} is also recognised and takes precedence
     * over the unscoped property when set; this lets a single Gradle invocation supply different suite
     * lists to different {@code yamlRestTest} tasks via the {@code tests.task} system property each task
     * already exposes (see {@code GradleTestPolicySetupPlugin}). For example, running
     * {@code :modules:reindex:yamlRestTest :rest-api-spec:yamlRestTest} in one invocation can use:
     * {@code -Dtests.rest.suite.:modules:reindex:yamlRestTest=reindex/51_routing}
     * {@code -Dtests.rest.suite.:rest-api-spec:yamlRestTest=get/41_routing_doc_values}.
     */
    public static final String REST_TESTS_SUITE = "tests.rest.suite";
    /**
     * Property that allows to blacklist some of the REST tests based on a comma separated list of globs
     * e.g. "-Dtests.rest.blacklist=get/10_basic/*"
     */
    public static final String REST_TESTS_BLACKLIST = "tests.rest.blacklist";
    /**
     * We use tests.rest.blacklist in build files to blacklist tests; this property enables a user to add additional blacklisted tests on
     * top of the tests blacklisted in the build.
     */
    public static final String REST_TESTS_BLACKLIST_ADDITIONS = "tests.rest.blacklist_additions";
    /**
     * Property that allows to control whether spec validation is enabled or not (default true).
     */
    private static final String REST_TESTS_VALIDATE_SPEC = "tests.rest.validate_spec";
    /**
     * Property controlling YAML test ordering. When {@code true} (the default), tests run
     * grouped by suite (yaml file) — every test in a given file runs consecutively, in
     * declared order. Set to {@code false} to instead shuffle tests across all files
     * (preserved-seed randomization). Suite grouping pairs with the lazy-cleanup framework
     * to maximize setup-state reuse within a file.
     *
     * <p>Note: this only takes effect for test classes that opt out of the framework-level
     * shuffle by declaring {@code @ParametersFactory(shuffle = false)}. Otherwise the runner
     * re-shuffles the parameters after this method returns and the grouping is lost.</p>
     */
    public static final String REST_TESTS_SUITE_GROUPING = "tests.rest.suite.grouping";

    private static final String TESTS_PATH = "rest-api-spec/test";
    private static final String SPEC_PATH = "rest-api-spec/api";

    /**
     * This separator pattern matches ',' except it is preceded by a '\'.
     * This allows us to support ',' within paths when it is escaped with a slash.
     * <p>
     * For example, the path string "/a/b/c\,d/e/f,/foo/bar,/baz" is separated to "/a/b/c\,d/e/f", "/foo/bar" and "/baz".
     * </p><p>
     * For reference, this regular expression feature is known as zero-width negative look-behind.
     * </p>
     */
    private static final String PATHS_SEPARATOR = "(?<!\\\\),";

    private static List<BlacklistedPathPatternMatcher> blacklistPathMatchers;
    private static ClientYamlTestExecutionContext restTestExecutionContext;
    private static ClientYamlTestExecutionContext adminExecutionContext;
    private static ClientYamlTestClient clientYamlTestClient;

    private final ClientYamlTestCandidate testCandidate;

    private static ClientYamlSuiteRestSpec restSpecification;

    private ESClientYamlSuiteErrorCollector errorCollector;

    protected ESClientYamlSuiteTestCase(ClientYamlTestCandidate testCandidate) {
        this.testCandidate = testCandidate;
    }

    @Before
    public void initAndResetContext() throws Exception {
        if (restTestExecutionContext == null) {
            assert adminExecutionContext == null;
            assert blacklistPathMatchers == null;
            final ClientYamlSuiteRestSpec restSpec = ClientYamlSuiteRestSpec.load(SPEC_PATH);
            validateSpec(restSpec);
            restSpecification = restSpec;
            final List<HttpHost> hosts = getClusterHosts();
            final Set<String> nodesVersions = getCachedNodesVersions();
            final String os = readOsFromNodesInfo(adminClient());

            logger.info("initializing client, node versions [{}], hosts {}, os [{}]", nodesVersions, hosts, os);

            final TestFeatureService testFeatureService = createTestFeatureService(
                getClusterStateFeatures(adminClient()),
                fromSemanticVersions(nodesVersions)
            );

            logger.info("initializing client, node versions [{}], hosts {}, os [{}]", nodesVersions, hosts, os);

            clientYamlTestClient = initClientYamlTestClient(restSpec, client(), hosts);
            restTestExecutionContext = createRestTestExecutionContext(
                testCandidate,
                clientYamlTestClient,
                nodesVersions,
                testFeatureService,
                Set.of(os)
            );
            adminExecutionContext = new ClientYamlTestExecutionContext(
                testCandidate,
                clientYamlTestClient,
                false,
                nodesVersions,
                testFeatureService,
                Set.of(os)
            );
            final String[] blacklist = resolvePathsProperty(REST_TESTS_BLACKLIST, null);
            blacklistPathMatchers = new ArrayList<>();
            for (final String entry : blacklist) {
                blacklistPathMatchers.add(new BlacklistedPathPatternMatcher(entry));
            }
            final String[] blacklistAdditions = resolvePathsProperty(REST_TESTS_BLACKLIST_ADDITIONS, null);
            for (final String entry : blacklistAdditions) {
                blacklistPathMatchers.add(new BlacklistedPathPatternMatcher(entry));
            }
        }
        assert restTestExecutionContext != null;
        assert adminExecutionContext != null;
        assert blacklistPathMatchers != null;

        // admin context must be available for @After always, regardless of whether the test was blacklisted
        adminExecutionContext.clear();

        restTestExecutionContext.clear();

        errorCollector = new ESClientYamlSuiteErrorCollector();
    }

    /**
     * Create the test execution context. Can be overwritten in sub-implementations of the test if the context needs to be modified.
     */
    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        final Set<String> nodesVersions,
        final TestFeatureService testFeatureService,
        final Set<String> osSet
    ) {
        return new ClientYamlTestExecutionContext(
            clientYamlTestCandidate,
            clientYamlTestClient,
            randomizeContentType(),
            nodesVersions,
            testFeatureService,
            osSet
        );
    }

    protected ClientYamlTestClient initClientYamlTestClient(
        final ClientYamlSuiteRestSpec restSpec,
        final RestClient restClient,
        final List<HttpHost> hosts
    ) {
        return new ClientYamlTestClient(restSpec, restClient, hosts, this::getClientBuilderWithSniffedHosts);
    }

    @AfterClass
    public static void closeClient() throws IOException {
        try {
            IOUtils.close(clientYamlTestClient);
        } finally {
            blacklistPathMatchers = null;
            restTestExecutionContext = null;
            adminExecutionContext = null;
            clientYamlTestClient = null;
            deferredCleanupForFile = null;
        }
    }

    /**
     * Create parameters for this parameterized test. Uses the
     * {@link ExecutableSection#XCONTENT_REGISTRY list} of executable sections
     * defined in {@link ExecutableSection}.
     */
    public static Iterable<Object[]> createParameters() throws Exception {
        return createParameters(ExecutableSection.XCONTENT_REGISTRY);
    }

    /**
     * Create parameters for this parameterized test.
     */
    public static Iterable<Object[]> createParameters(NamedXContentRegistry executeableSectionRegistry) throws Exception {
        return createParameters(executeableSectionRegistry, Map.of(), resolveRestTestsSuitePaths());
    }

    /**
     * Create parameters for this parameterized test.
     * @param yamlParameters map or parameters used within the yaml specs to be replaced at parsing time.
     */
    public static Iterable<Object[]> createParameters(Map<String, Object> yamlParameters) throws Exception {
        return createParameters(ExecutableSection.XCONTENT_REGISTRY, yamlParameters, resolveRestTestsSuitePaths());
    }

    /**
     * Create parameters for this parameterized test.
     * @param yamlParameters map or parameters used within the yaml specs to be replaced at parsing time.
     * @param testPaths      list of paths to explicitly search for tests.
     */
    public static Iterable<Object[]> createParameters(Map<String, Object> yamlParameters, String... testPaths) throws Exception {
        if (resolveRestTestsSuiteProperty() != null) {
            throw new IllegalArgumentException("The '" + REST_TESTS_SUITE + "' system property is not supported with explicit test paths.");
        }
        return createParameters(ExecutableSection.XCONTENT_REGISTRY, yamlParameters, testPaths);
    }

    /**
     * Create parameters for this parameterized test.
     * @param testPaths list of paths to explicitly search for tests.
     */
    public static Iterable<Object[]> createParameters(String... testPaths) throws Exception {
        if (resolveRestTestsSuiteProperty() != null) {
            throw new IllegalArgumentException("The '" + REST_TESTS_SUITE + "' system property is not supported with explicit test paths.");
        }
        return createParameters(ExecutableSection.XCONTENT_REGISTRY, Map.of(), testPaths);
    }

    /**
     * Create parameters for this parameterized test.
     *
     * @param executeableSectionRegistry registry of executable sections
     * @param testPaths list of paths to explicitly search for tests.
     * @return list of test candidates.
     */
    public static Iterable<Object[]> createParameters(NamedXContentRegistry executeableSectionRegistry, String... testPaths)
        throws Exception {
        if (resolveRestTestsSuiteProperty() != null) {
            throw new IllegalArgumentException("The '" + REST_TESTS_SUITE + "' system property is not supported with explicit test paths.");
        }
        return createParameters(executeableSectionRegistry, Map.of(), testPaths);
    }

    /**
     * Create parameters for this parameterized test.
     *
     * @param executeableSectionRegistry registry of executable sections
     * @param yamlParameters             map or parameters used within the yaml specs to be replaced at parsing time.
     * @param testPaths                  list of paths to explicitly search for tests. If {@code null} then include all tests in root path.
     * @return list of test candidates.
     */
    public static Iterable<Object[]> createParameters(
        NamedXContentRegistry executeableSectionRegistry,
        Map<String, ?> yamlParameters,
        String... testPaths
    ) throws Exception {

        if (testPaths == null) {
            throw new IllegalArgumentException("testPaths cannot be null");
        }

        Map<String, Set<Path>> yamlSuites = loadSuites(testPaths);
        List<ClientYamlTestSuite> suites = new ArrayList<>();
        IllegalArgumentException validationException = null;
        // yaml suites are grouped by directory (effectively by api)
        for (String api : yamlSuites.keySet()) {
            List<Path> yamlFiles = new ArrayList<>(yamlSuites.get(api));
            for (Path yamlFile : yamlFiles) {
                ClientYamlTestSuite suite = ClientYamlTestSuite.parse(executeableSectionRegistry, api, yamlFile, yamlParameters);
                suites.add(suite);
                try {
                    suite.validate();
                } catch (IllegalArgumentException e) {
                    if (validationException == null) {
                        validationException = new IllegalArgumentException(
                            "Validation errors for the following test suites:\n- " + e.getMessage()
                        );
                    } else {
                        String previousMessage = validationException.getMessage();
                        Throwable[] suppressed = validationException.getSuppressed();
                        validationException = new IllegalArgumentException(previousMessage + "\n- " + e.getMessage());
                        for (Throwable t : suppressed) {
                            validationException.addSuppressed(t);
                        }
                    }
                    validationException.addSuppressed(e);
                }
            }
        }

        if (validationException != null) {
            throw validationException;
        }

        List<Object[]> tests = new ArrayList<>();
        for (ClientYamlTestSuite yamlTestSuite : suites) {
            for (ClientYamlTestSection testSection : yamlTestSuite.getTestSections()) {
                tests.add(new Object[] { new ClientYamlTestCandidate(yamlTestSuite, testSection) });
            }
        }
        // Sort by test path first for a deterministic baseline, then shuffle with the
        // runner's seeded random. When suite-grouping is enabled (the default), re-cluster
        // shuffled tests by their yaml file: each file's tests run consecutively, and the
        // position of each file's cluster in the final order is determined by where the
        // first test from that file lands in the shuffle. This preserves the seeded
        // randomization across files while keeping each file's tests together so the
        // lazy-cleanup framework can reuse setup state within a file. Test classes opt
        // into this ordering by declaring @ParametersFactory(shuffle = false), so the
        // runner does not re-shuffle and undo what we set up here.
        tests.sort(Comparator.comparing(o -> ((ClientYamlTestCandidate) o[0]).getTestPath()));
        // Use a Random seeded directly from the tests.seed system property: this method runs
        // during parameter collection, before any RandomizedRunner per-test context is set up,
        // so RandomizedTest.getRandom() throws here.
        Collections.shuffle(tests, parameterFactoryRandom());
        if (RandomizedTest.systemPropertyAsBoolean(REST_TESTS_SUITE_GROUPING, true)) {
            Map<String, List<Object[]>> byFile = new LinkedHashMap<>();
            for (Object[] test : tests) {
                String suitePath = ((ClientYamlTestCandidate) test[0]).getSuitePath();
                byFile.computeIfAbsent(suitePath, k -> new ArrayList<>()).add(test);
            }
            tests = new ArrayList<>(tests.size());
            for (List<Object[]> group : byFile.values()) {
                tests.addAll(group);
            }
        }
        return tests;
    }

    /**
     * Returns a {@link java.util.Random} seeded from the {@code tests.seed} system property
     * for use when collecting test parameters. {@link RandomizedTest#getRandom()} requires
     * an active per-test {@code RandomizedContext}, which is not set up while the runner
     * is invoking {@code @ParametersFactory} methods, so we re-derive a Random from the
     * raw seed here. Falls back to a fresh non-deterministic {@code Random} if the property
     * is unset (e.g. running ad hoc without a configured seed).
     */
    private static java.util.Random parameterFactoryRandom() {
        String seedProp = System.getProperty("tests.seed");
        if (seedProp == null || seedProp.isEmpty()) {
            return new java.util.Random();
        }
        // tests.seed can be "<master>" or "<master>:<method>"; the first part is the suite seed.
        String masterPart = seedProp.split(":", 2)[0];
        return new java.util.Random(Long.parseUnsignedLong(masterPart, 16));
    }

    /** Find all yaml suites that match the given list of paths from the root test path. */
    // pkg private for tests
    static Map<String, Set<Path>> loadSuites(String... paths) throws Exception {
        Map<String, Set<Path>> files = new HashMap<>();
        Path[] roots = ClasspathUtils.findFilePaths(ESClientYamlSuiteTestCase.class.getClassLoader(), TESTS_PATH);
        for (Path root : roots) {
            for (String strPath : paths) {
                Path path = root.resolve(strPath);
                if (Files.isDirectory(path)) {
                    try (var filesStream = Files.walk(path)) {
                        filesStream.forEach(file -> {
                            if (file.toString().endsWith(".yml")) {
                                addSuite(root, file, files);
                            } else if (file.toString().endsWith(".yaml")) {
                                throw new IllegalArgumentException("yaml files are no longer supported: " + file);
                            }
                        });
                    }
                } else {
                    path = root.resolve(strPath + ".yml");
                    assert Files.exists(path) : "Path " + path + " does not exist in YAML test root";
                    addSuite(root, path, files);
                }
            }
        }
        return files;
    }

    /** Add a single suite file to the set of suites. */
    private static void addSuite(Path root, Path file, Map<String, Set<Path>> files) {
        String groupName = root.relativize(file.getParent()).toString();
        Set<Path> filesSet = files.get(groupName);
        if (filesSet == null) {
            filesSet = new HashSet<>();
            files.put(groupName, filesSet);
        }

        filesSet.add(file);
        List<String> fileNames = filesSet.stream().map(p -> p.getFileName().toString()).toList();
        if (Collections.frequency(fileNames, file.getFileName().toString()) > 1) {
            Logger logger = LogManager.getLogger(ESClientYamlSuiteTestCase.class);
            logger.warn(
                "Found duplicate test name ["
                    + groupName
                    + "/"
                    + file.getFileName()
                    + "] on the class path. "
                    + "This can result in class loader dependent execution commands and reproduction commands "
                    + "(will add #2 to one of the test names dependent on the classloading order)"
            );
        }
    }

    private static String[] resolvePathsProperty(String propertyName, String defaultValue) {
        String property = System.getProperty(propertyName);
        if (Strings.hasLength(property) == false) {
            return defaultValue == null ? Strings.EMPTY_ARRAY : new String[] { defaultValue };
        } else {
            return property.split(PATHS_SEPARATOR);
        }
    }

    /**
     * Resolves the value of {@link #REST_TESTS_SUITE}, preferring the per-task scoped form
     * {@code tests.rest.suite.<task path>} when {@code tests.task} is set and a matching property
     * exists. Returns {@code null} if neither form is set.
     */
    private static String resolveRestTestsSuiteProperty() {
        String task = System.getProperty("tests.task");
        if (task != null) {
            String scoped = System.getProperty(REST_TESTS_SUITE + "." + task);
            if (scoped != null) {
                return scoped;
            }
        }
        return System.getProperty(REST_TESTS_SUITE);
    }

    private static String[] resolveRestTestsSuitePaths() {
        String value = resolveRestTestsSuiteProperty();
        if (Strings.hasLength(value) == false) {
            return new String[] { "" };
        }
        return value.split(PATHS_SEPARATOR);
    }

    protected ClientYamlTestExecutionContext getAdminExecutionContext() {
        return adminExecutionContext;
    }

    static ClientYamlSuiteRestSpec getRestSpec() {
        return restSpecification;
    }

    private static void validateSpec(ClientYamlSuiteRestSpec restSpec) {
        boolean validateSpec = RandomizedTest.systemPropertyAsBoolean(REST_TESTS_VALIDATE_SPEC, true);
        if (validateSpec) {
            StringBuilder errorMessage = new StringBuilder();
            for (ClientYamlSuiteRestApi restApi : restSpec.getApis()) {
                if (restApi.isBodySupported()) {
                    for (ClientYamlSuiteRestApi.Path path : restApi.getPaths()) {
                        List<String> methodsList = Arrays.asList(path.methods());
                        if (methodsList.contains("GET") && restApi.isBodySupported()) {
                            if (methodsList.contains("POST") == false) {
                                errorMessage.append("\n- ")
                                    .append(restApi.getName())
                                    .append(" supports GET with a body but doesn't support POST");
                            }
                        }
                    }
                }
            }
            if (errorMessage.length() > 0) {
                throw new IllegalArgumentException(errorMessage.toString());
            }
        }
    }

    public static String readOsFromNodesInfo(RestClient restClient) throws IOException {
        final Request request = new Request("GET", "/_nodes/os");
        Response response = restClient.performRequest(request);
        ClientYamlTestResponse restTestResponse = new ClientYamlTestResponse(response);
        SortedSet<String> osPrettyNames = new TreeSet<>();

        final Map<String, Object> nodes = restTestResponse.evaluate("nodes");

        for (Entry<String, Object> node : nodes.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> nodeInfo = (Map<String, Object>) node.getValue();

            osPrettyNames.add((String) XContentMapValues.extractValue("os.pretty_name", nodeInfo));
        }

        assert osPrettyNames.isEmpty() == false : "no os found";

        // Although in theory there should only be one element as all nodes are running on the same machine,
        // in reality there can be two in mixed version clusters if different Java versions report the OS
        // name differently. This has been observed to happen on Windows, where Java needs to be updated to
        // recognize new Windows versions, and until this update has been done the newest version of Windows
        // is reported as the previous one. In this case taking the last alphabetically is likely to be most
        // accurate, for example if "Windows Server 2016" and "Windows Server 2019" are reported by different
        // Java versions then Windows Server 2019 is likely to be correct.
        return osPrettyNames.last();
    }

    public void test() throws IOException {
        // skip test if it matches one of the blacklist globs
        for (BlacklistedPathPatternMatcher blacklistedPathMatcher : blacklistPathMatchers) {
            String testPath = testCandidate.getSuitePath() + "/" + testCandidate.getTestSection().getName();
            assumeFalse(
                "[" + testCandidate.getTestPath() + "] skipped, reason: blacklisted",
                blacklistedPathMatcher.isSuffixMatch(testPath)
            );
        }

        // skip test if the whole suite (yaml file) is disabled
        testCandidate.getSetupSection().getPrerequisiteSection().evaluate(restTestExecutionContext, testCandidate.getSuitePath());
        testCandidate.getTeardownSection().getPrerequisiteSection().evaluate(restTestExecutionContext, testCandidate.getSuitePath());
        // skip test if test section is disabled
        testCandidate.getTestSection().getPrerequisiteSection().evaluate(restTestExecutionContext, testCandidate.getTestPath());

        // let's check that there is something to run, otherwise there might be a problem with the test section
        if (testCandidate.getTestSection().getExecutableSections().isEmpty()) {
            throw new IllegalArgumentException("No executable sections loaded for [" + testCandidate.getTestPath() + "]");
        }

        assumeFalse(
            "[" + testCandidate.getTestPath() + "] skipped, reason: in fips 140 mode",
            inFipsJvm() && testCandidate.getTestSection().getPrerequisiteSection().hasYamlRunnerFeature("fips_140")
        );

        // Reset the persistent-resource flag so it tracks only this test's setup+body.
        // Unlike the write-occurred flag (reset between setup and body), this one must span
        // setup AND body so that a setup-phase _start/_open still forces teardown to run.
        org.elasticsearch.client.LazyRefreshRestClient.resetPersistentResourceCreated();

        if (skipSetupSections() == false && testCandidate.getSetupSection().isEmpty() == false) {
            logger.debug("start setup test [{}]", testCandidate.getTestPath());
            for (ExecutableSection executableSection : testCandidate.getSetupSection().getExecutableSections()) {
                executeSection(executableSection);
            }
            logger.debug("end setup test [{}]", testCandidate.getTestPath());
        }

        restTestExecutionContext.clear();
        // Prepare the stash so that ${_project_id_prefix_} is expanded as needed in some assertions:
        restTestExecutionContext.stash().stashValue("_project_id_prefix_", activeProjectPrefix());

        // Reset the write-occurred flag so it is scoped to the test body only — writes done by
        // setup (or by @Before hooks) are not what determines whether end-of-test cleanup must run.
        org.elasticsearch.client.LazyRefreshRestClient.resetWriteOccurred();

        // Mark that we've reached the body. If a test was skipped (assumeFalse) or setup
        // threw, this stays false and preserveClusterUponCompletion won't mark this file's
        // setup as cached on the cluster.
        testBodyRan = true;

        try {
            for (ExecutableSection executableSection : testCandidate.getTestSection().getExecutableSections()) {
                executeSection(executableSection);
            }
            errorCollector.verify();
        } catch (AssertionError e) {
            // Dump the original yaml file, if available, for reference.
            Optional<Path> file = testCandidate.getRestTestSuite().getFile();
            if (file.isPresent()) {
                try {
                    logger.info("Dump test yaml [{}] on failure:\n{}", file.get(), Files.readString(file.get()));
                } catch (IOException ex) {
                    logger.info("Did not dump test yaml [{}] on failure due to an exception [{}]", file.get(), ex);
                }
            }
            // Dump the stash on failure. Instead of dumping it in true json we escape `\n`s so stack traces are easier to read
            logger.info(
                "Stash dump on test failure [{}]",
                Strings.toString(restTestExecutionContext.stash(), true, true)
                    .replace("\\n", "\n")
                    .replace("\\r", "\r")
                    .replace("\\t", "\t")
            );
            throw e;
        } finally {
            if (skipTeardownSections() == false) {
                logger.debug("start teardown test [{}]", testCandidate.getTestPath());
                for (ExecutableSection doSection : testCandidate.getTeardownSection().getDoSections()) {
                    executeSection(doSection);
                }
                logger.debug("end teardown test [{}]", testCandidate.getTestPath());
            }
        }
    }

    private void executeSection(ExecutableSection executableSection) {
        errorCollector.checkSucceeds(() -> {
            try {
                executableSection.execute(restTestExecutionContext);
                return null;
            } catch (Throwable t) {
                if (t instanceof AssertionError) {
                    throw t;
                } else {
                    throw new AssertionError(
                        "Error executing section at ["
                            + testCandidate.getSuitePath()
                            + ":"
                            + executableSection.getLocation().lineNumber()
                            + "]: "
                            + t.getMessage(),
                        t
                    );
                }
            }
        });
    }

    /**
     * Yaml file path whose setup state is currently sitting in the cluster, awaiting a deferred
     * cleanup. {@code null} when no deferred state is pending. Drives the default
     * {@link #skipSetupSections()} / {@link #skipTeardownSections()} /
     * {@link #preserveClusterUponCompletion()} behavior, which lets read-only tests reuse the
     * previous test's setup state and write tests pay the normal setup+cleanup cost.
     */
    private static String deferredCleanupForFile = null;

    /** Cached per-test result for {@link #preserveClusterUponCompletion()} so its decision and
     *  side effects run exactly once even though the framework calls it twice
     *  ({@code cleanUpCluster} and {@code assertEmptyProjects}). */
    private Boolean cachedPreserve = null;

    /** Set to {@code true} once execution reaches the body section of {@link #test()}. Stays
     *  {@code false} when a test is skipped early (blacklist, fips, prerequisite features) or
     *  when setup throws before the body. Used to avoid claiming "this file's setup is on the
     *  cluster" when nothing actually got loaded. */
    private boolean testBodyRan = false;

    /**
     * Yaml runner feature name a test (or its setup section) can declare to opt out of the
     * lazy-cleanup optimization. Tests that assert on cumulative cluster state — request cache
     * hit/miss counters, search/indexing stats, anything else that is incremented by reads and
     * not reset by {@code _cache/clear} — should declare this so the framework runs setup and
     * teardown around them and wipes any deferred state from earlier tests in the same file.
     *
     * <p>Usage in YAML:</p>
     * <pre>
     * "Some test that asserts on cache stats":
     *   - requires:
     *       test_runner_features: ["clean_setup"]
     *   - do: ...
     * </pre>
     */
    public static final String CLEAN_SETUP_FEATURE = "clean_setup";

    /**
     * Set by the {@code yamlRestCompatTest} task. When true, the lazy-cleanup optimization is
     * suppressed for the entire JVM run because the compat task replays yaml tests checked
     * out from a prior branch, which won't carry any {@link #CLEAN_SETUP_FEATURE} markers we
     * add going forward.
     */
    private static final boolean REST_COMPAT_MODE = Boolean.parseBoolean(System.getProperty("tests.restCompat", "false"));

    /**
     * Whether the current test (or its setup section) requires a clean cluster setup. Tests
     * marked with the {@link #CLEAN_SETUP_FEATURE} yaml runner feature opt out of the
     * lazy-cleanup optimization for the current invocation. Also returns {@code true}
     * unconditionally when running under {@code yamlRestCompatTest}.
     */
    private boolean requiresCleanSetup() {
        if (REST_COMPAT_MODE) {
            return true;
        }
        return testCandidate.getTestSection().getPrerequisiteSection().hasYamlRunnerFeature(CLEAN_SETUP_FEATURE)
            || testCandidate.getSetupSection().getPrerequisiteSection().hasYamlRunnerFeature(CLEAN_SETUP_FEATURE);
    }

    /**
     * Default lazy-cleanup behavior: skip the YAML setup section when the previous test
     * deferred cleanup AND it was from the same yaml file (so the current test's setup
     * state is already in the cluster). When the previous test deferred cleanup but the
     * current test is from a different yaml file, this method wipes the stale cluster state
     * before returning {@code false}, so setup runs against a clean cluster. Subclasses may
     * override to disable this.
     */
    protected boolean skipSetupSections() {
        String currentFile = testCandidate.getSuitePath();
        // Tests marked clean_setup must always run their setup against a wiped cluster, even
        // if the previous same-file test deferred cleanup.
        if (requiresCleanSetup()) {
            if (deferredCleanupForFile != null) {
                try {
                    wipeCluster();
                } catch (Exception e) {
                    throw new AssertionError("failed to wipe stale cluster state for clean_setup test", e);
                }
                deferredCleanupForFile = null;
            }
            return false;
        }
        if (deferredCleanupForFile == null) {
            return false;
        }
        if (deferredCleanupForFile.equals(currentFile)) {
            return true;
        }
        // File changed across a deferred cleanup. Wipe the stale state so this file's setup
        // runs against a clean cluster.
        try {
            wipeCluster();
        } catch (Exception e) {
            throw new AssertionError("failed to wipe stale cluster state on yaml file change", e);
        }
        deferredCleanupForFile = null;
        return false;
    }

    /**
     * Default lazy-cleanup behavior: skip the YAML teardown section when the body issued no
     * non-read HTTP requests AND no persistent cluster-side resource (started transform, opened
     * ML job, point-in-time, async search, etc.) was created during setup or body. Cleanup is
     * deferred to the next test or until a write/persistent-resource event happens. Subclasses
     * may override.
     */
    protected boolean skipTeardownSections() {
        if (requiresCleanSetup()) {
            return false;
        }
        return org.elasticsearch.client.LazyRefreshRestClient.writeOccurred() == false
            && org.elasticsearch.client.LazyRefreshRestClient.persistentResourceCreated() == false;
    }

    /**
     * Default lazy-cleanup behavior: preserve cluster state (skip framework wipe and
     * {@code assertEmptyProjects}) when the body issued no non-read HTTP requests AND no
     * persistent cluster-side resource was created during setup or body. The deferred-cleanup
     * marker is updated to the current file on the way out, so the next same-file test can skip
     * its setup. Subclasses may override.
     */
    @Override
    protected boolean preserveClusterUponCompletion() {
        if (cachedPreserve != null) {
            return cachedPreserve;
        }
        if (requiresCleanSetup()) {
            cachedPreserve = false;
            deferredCleanupForFile = null;
            return false;
        }
        boolean writeHappened = org.elasticsearch.client.LazyRefreshRestClient.writeOccurred();
        boolean persistentResourceCreated = org.elasticsearch.client.LazyRefreshRestClient.persistentResourceCreated();
        cachedPreserve = (writeHappened == false && persistentResourceCreated == false);
        if (cachedPreserve) {
            // Only mark this file's setup as cached on the cluster when the body actually
            // executed. If the test was skipped (assumeFalse for blacklist/fips/prerequisites)
            // setup never ran, so leaving deferredCleanupForFile untouched prevents the next
            // same-file test from skipping its own setup against an empty cluster.
            if (testBodyRan) {
                deferredCleanupForFile = testCandidate.getSuitePath();
            }
        } else {
            deferredCleanupForFile = null;
        }
        return cachedPreserve;
    }

    protected boolean randomizeContentType() {
        return true;
    }

    /**
     * Sniff the cluster for host metadata and return a
     * {@link RestClientBuilder} for a client with that metadata.
     */
    protected final RestClientBuilder getClientBuilderWithSniffedHosts() throws IOException {
        ElasticsearchNodesSniffer.Scheme scheme = ElasticsearchNodesSniffer.Scheme.valueOf(getProtocol().toUpperCase(Locale.ROOT));
        ElasticsearchNodesSniffer sniffer = new ElasticsearchNodesSniffer(
            adminClient(),
            ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
            scheme
        );
        RestClientBuilder builder = RestClient.builder(sniffer.sniff().toArray(new Node[0]));
        configureClient(builder, restClientSettings());
        return builder;
    }

    public ClientYamlTestCandidate getTestCandidate() {
        return testCandidate;
    }

    private static class ESClientYamlSuiteErrorCollector extends ErrorCollector {

        public void verify() throws AssertionError {
            try {
                super.verify();
            } catch (Throwable e) {
                throw new AssertionError(e);
            }
        }
    }
}
