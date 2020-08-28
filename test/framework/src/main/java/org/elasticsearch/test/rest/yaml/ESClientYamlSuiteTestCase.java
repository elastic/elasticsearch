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

package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.http.HttpHost;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Runs a suite of yaml tests shared with all the official Elasticsearch
 * clients against against an elasticsearch cluster.
 *
 * The suite timeout is extended to account for projects with a large number of tests.
 */
@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
public abstract class ESClientYamlSuiteTestCase extends ESRestTestCase {

    /**
     * Property that allows to control which REST tests get run. Supports comma separated list of tests
     * or directories that contain tests e.g. -Dtests.rest.suite=index,get,create/10_with_id
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

    private static final String TESTS_PATH = "/rest-api-spec/test";
    private static final String SPEC_PATH = "/rest-api-spec/api";

    /**
     * This separator pattern matches ',' except it is preceded by a '\'.
     * This allows us to support ',' within paths when it is escaped with a slash.
     *
     * For example, the path string "/a/b/c\,d/e/f,/foo/bar,/baz" is separated to "/a/b/c\,d/e/f", "/foo/bar" and "/baz".
     *
     * For reference, this regular expression feature is known as zero-width negative look-behind.
     *
     */
    private static final String PATHS_SEPARATOR = "(?<!\\\\),";

    private static List<BlacklistedPathPatternMatcher> blacklistPathMatchers;
    private static ClientYamlTestExecutionContext restTestExecutionContext;
    private static ClientYamlTestExecutionContext adminExecutionContext;
    private static ClientYamlTestClient clientYamlTestClient;

    private final ClientYamlTestCandidate testCandidate;

    protected ESClientYamlSuiteTestCase(ClientYamlTestCandidate testCandidate) {
        this.testCandidate = testCandidate;
    }

    private static boolean useDefaultNumberOfShards;

    @BeforeClass
    public static void initializeUseDefaultNumberOfShards() {
        useDefaultNumberOfShards = usually();
    }

    @Before
    public void initAndResetContext() throws Exception {
        if (restTestExecutionContext == null) {
            assert adminExecutionContext == null;
            assert blacklistPathMatchers == null;
            final ClientYamlSuiteRestSpec restSpec = ClientYamlSuiteRestSpec.load(SPEC_PATH);
            validateSpec(restSpec);
            final List<HttpHost> hosts = getClusterHosts();
            Tuple<Version, Version> versionVersionTuple = readVersionsFromCatNodes(adminClient());
            final Version esVersion = versionVersionTuple.v1();
            final Version masterVersion = versionVersionTuple.v2();
            logger.info("initializing client, minimum es version [{}], master version, [{}], hosts {}", esVersion, masterVersion, hosts);
            clientYamlTestClient = initClientYamlTestClient(restSpec, client(), hosts, esVersion, masterVersion);
            restTestExecutionContext = new ClientYamlTestExecutionContext(clientYamlTestClient, randomizeContentType());
            adminExecutionContext = new ClientYamlTestExecutionContext(clientYamlTestClient, false);
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
    }

    protected ClientYamlTestClient initClientYamlTestClient(
            final ClientYamlSuiteRestSpec restSpec,
            final RestClient restClient,
            final List<HttpHost> hosts,
            final Version esVersion,
            final Version masterVersion) {
        return new ClientYamlTestClient(restSpec, restClient, hosts, esVersion, masterVersion, this::getClientBuilderWithSniffedHosts);
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
        String[] paths = resolvePathsProperty(REST_TESTS_SUITE, ""); // default to all tests under the test root
        Map<String, Set<Path>> yamlSuites = loadSuites(paths);
        List<ClientYamlTestSuite> suites = new ArrayList<>();
        IllegalArgumentException validationException = null;
        // yaml suites are grouped by directory (effectively by api)
        for (String api : yamlSuites.keySet()) {
            List<Path> yamlFiles = new ArrayList<>(yamlSuites.get(api));
            for (Path yamlFile : yamlFiles) {
                ClientYamlTestSuite suite = ClientYamlTestSuite.parse(executeableSectionRegistry, api, yamlFile);
                suites.add(suite);
                try {
                    suite.validate();
                } catch(IllegalArgumentException e) {
                    if (validationException == null) {
                        validationException = new IllegalArgumentException("Validation errors for the following test suites:\n- "
                            + e.getMessage());
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
                tests.add(new Object[]{ new ClientYamlTestCandidate(yamlTestSuite, testSection) });
            }
        }
        //sort the candidates so they will always be in the same order before being shuffled, for repeatability
        tests.sort(Comparator.comparing(o -> ((ClientYamlTestCandidate) o[0]).getTestPath()));
        return tests;
    }

    /** Find all yaml suites that match the given list of paths from the root test path. */
    // pkg private for tests
    static Map<String, Set<Path>> loadSuites(String... paths) throws Exception {
        Map<String, Set<Path>> files = new HashMap<>();
        Path root = PathUtils.get(ESClientYamlSuiteTestCase.class.getResource(TESTS_PATH).toURI());
        for (String strPath : paths) {
            Path path = root.resolve(strPath);
            if (Files.isDirectory(path)) {
                Files.walk(path).forEach(file -> {
                    if (file.toString().endsWith(".yml")) {
                        addSuite(root, file, files);
                    } else if (file.toString().endsWith(".yaml")) {
                        throw new IllegalArgumentException("yaml files are no longer supported: " + file);
                    }
                });
            } else {
                path = root.resolve(strPath + ".yml");
                assert Files.exists(path);
                addSuite(root, path, files);
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
    }

    private static String[] resolvePathsProperty(String propertyName, String defaultValue) {
        String property = System.getProperty(propertyName);
        if (!Strings.hasLength(property)) {
            return defaultValue == null ? Strings.EMPTY_ARRAY : new String[]{defaultValue};
        } else {
            return property.split(PATHS_SEPARATOR);
        }
    }

    protected ClientYamlTestExecutionContext getAdminExecutionContext() {
        return adminExecutionContext;
    }

    private static void validateSpec(ClientYamlSuiteRestSpec restSpec) {
        boolean validateSpec = RandomizedTest.systemPropertyAsBoolean(REST_TESTS_VALIDATE_SPEC, true);
        if (validateSpec) {
            StringBuilder errorMessage = new StringBuilder();
            for (ClientYamlSuiteRestApi restApi : restSpec.getApis()) {
                if (restApi.isBodySupported()) {
                    for (ClientYamlSuiteRestApi.Path path : restApi.getPaths()) {
                        List<String> methodsList = Arrays.asList(path.getMethods());
                        if (methodsList.contains("GET") && restApi.isBodySupported()) {
                            if (!methodsList.contains("POST")) {
                                errorMessage.append("\n- ").append(restApi.getName())
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

    private Tuple<Version, Version> readVersionsFromCatNodes(RestClient restClient) throws IOException {
        // we simply go to the _cat/nodes API and parse all versions in the cluster
        final Request request = new Request("GET", "/_cat/nodes");
        request.addParameter("h", "version,master");
        request.setOptions(getCatNodesVersionMasterRequestOptions());
        Response response = restClient.performRequest(request);
        ClientYamlTestResponse restTestResponse = new ClientYamlTestResponse(response);
        String nodesCatResponse = restTestResponse.getBodyAsString();
        String[] split = nodesCatResponse.split("\n");
        Version version = null;
        Version masterVersion = null;
        for (String perNode : split) {
            final String[] versionAndMaster = perNode.split("\\s+");
            assert versionAndMaster.length == 2 : "invalid line: " + perNode + " length: " + versionAndMaster.length;
            final Version currentVersion = Version.fromString(versionAndMaster[0]);
            final boolean master = versionAndMaster[1].trim().equals("*");
            if (master) {
                assert masterVersion == null;
                masterVersion = currentVersion;
            }
            if (version == null) {
                version = currentVersion;
            } else if (version.onOrAfter(currentVersion)) {
                version = currentVersion;
            }
        }
        return new Tuple<>(version, masterVersion);
    }

    protected RequestOptions getCatNodesVersionMasterRequestOptions() {
        return RequestOptions.DEFAULT;
    }

    public void test() throws IOException {
        //skip test if it matches one of the blacklist globs
        for (BlacklistedPathPatternMatcher blacklistedPathMatcher : blacklistPathMatchers) {
            String testPath = testCandidate.getSuitePath() + "/" + testCandidate.getTestSection().getName();
            assumeFalse("[" + testCandidate.getTestPath() + "] skipped, reason: blacklisted", blacklistedPathMatcher
                .isSuffixMatch(testPath));
        }

        //skip test if the whole suite (yaml file) is disabled
        assumeFalse(testCandidate.getSetupSection().getSkipSection().getSkipMessage(testCandidate.getSuitePath()),
            testCandidate.getSetupSection().getSkipSection().skip(restTestExecutionContext.esVersion()));
        //skip test if the whole suite (yaml file) is disabled
        assumeFalse(testCandidate.getTeardownSection().getSkipSection().getSkipMessage(testCandidate.getSuitePath()),
            testCandidate.getTeardownSection().getSkipSection().skip(restTestExecutionContext.esVersion()));
        //skip test if test section is disabled
        assumeFalse(testCandidate.getTestSection().getSkipSection().getSkipMessage(testCandidate.getTestPath()),
            testCandidate.getTestSection().getSkipSection().skip(restTestExecutionContext.esVersion()));

        //let's check that there is something to run, otherwise there might be a problem with the test section
        if (testCandidate.getTestSection().getExecutableSections().size() == 0) {
            throw new IllegalArgumentException("No executable sections loaded for [" + testCandidate.getTestPath() + "]");
        }

        if (useDefaultNumberOfShards == false
                && testCandidate.getTestSection().getSkipSection().getFeatures().contains("default_shards") == false) {
            final Request request = new Request("PUT", "/_template/global");
            request.setJsonEntity("{\"index_patterns\":[\"*\"],\"settings\":{\"index.number_of_shards\":2}}");
            // Because this has not yet transitioned to a composable template, it's possible that
            // this can overlap an installed composable template since this is a global (*)
            // template. In order to avoid this failing the test, we override the warnings handler
            // to be permissive in this case. This can be removed once all tests use composable
            // templates instead of legacy templates
            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.setWarningsHandler(WarningsHandler.PERMISSIVE);
            request.setOptions(builder.build());
            adminClient().performRequest(request);
        }

        if (!testCandidate.getSetupSection().isEmpty()) {
            logger.debug("start setup test [{}]", testCandidate.getTestPath());
            for (ExecutableSection executableSection : testCandidate.getSetupSection().getExecutableSections()) {
                executeSection(executableSection);
            }
            logger.debug("end setup test [{}]", testCandidate.getTestPath());
        }

        restTestExecutionContext.clear();

        try {
            for (ExecutableSection executableSection : testCandidate.getTestSection().getExecutableSections()) {
                executeSection(executableSection);
            }
        } finally {
            logger.debug("start teardown test [{}]", testCandidate.getTestPath());
            for (ExecutableSection doSection : testCandidate.getTeardownSection().getDoSections()) {
                executeSection(doSection);
            }
            logger.debug("end teardown test [{}]", testCandidate.getTestPath());
        }
    }

    /**
     * Execute an {@link ExecutableSection}, careful to log its place of origin on failure.
     */
    private void executeSection(ExecutableSection executableSection) {
        try {
            executableSection.execute(restTestExecutionContext);
        } catch (AssertionError | Exception e) {
            // Dump the stash on failure. Instead of dumping it in true json we escape `\n`s so stack traces are easier to read
            logger.info("Stash dump on test failure [{}]",
                    Strings.toString(restTestExecutionContext.stash(), true, true)
                            .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t"));
            if (e instanceof AssertionError) {
                throw new AssertionError(errorMessage(executableSection, e), e);
            } else {
                throw new RuntimeException(errorMessage(executableSection, e), e);
            }
        }
    }

    private String errorMessage(ExecutableSection executableSection, Throwable t) {
        return "Failure at [" + testCandidate.getSuitePath() + ":" + executableSection.getLocation().lineNumber + "]: " + t.getMessage();
    }

    protected boolean randomizeContentType() {
        return true;
    }

    /**
     * Sniff the cluster for host metadata and return a
     * {@link RestClientBuilder} for a client with that metadata.
     */
    protected final RestClientBuilder getClientBuilderWithSniffedHosts() throws IOException {
        ElasticsearchNodesSniffer.Scheme scheme =
            ElasticsearchNodesSniffer.Scheme.valueOf(getProtocol().toUpperCase(Locale.ROOT));
        ElasticsearchNodesSniffer sniffer = new ElasticsearchNodesSniffer(
                adminClient(), ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT, scheme);
        RestClientBuilder builder = RestClient.builder(sniffer.sniff().toArray(new Node[0]));
        configureClient(builder, restClientSettings());
        return builder;
    }

    protected final boolean preserveDataStreamsUponCompletion() {
        // TODO: enable automatic deleting of data streams
        // For now don't automatically try to remove all data streams after each yaml test.
        // The client runners need to be adjust to remove data streams after each test too,
        // otherwise rest yaml tests using data streams succeed in Elasticsearch, but may fail when clients run
        // the yaml test suite. In the mean time we should delete data streams manually after each test.
        return true;
    }
}
