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
import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Runs a suite of yaml tests shared with all the official Elasticsearch clients against against an elasticsearch cluster.
 */
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

    private final ClientYamlTestCandidate testCandidate;

    protected ESClientYamlSuiteTestCase(ClientYamlTestCandidate testCandidate) {
        this.testCandidate = testCandidate;
    }

    @Before
    public void initAndResetContext() throws Exception {
        if (restTestExecutionContext == null) {
            assert adminExecutionContext == null;
            assert blacklistPathMatchers == null;
            ClientYamlSuiteRestSpec restSpec = ClientYamlSuiteRestSpec.load(SPEC_PATH);
            validateSpec(restSpec);
            List<HttpHost> hosts = getClusterHosts();
            RestClient restClient = client();
            Version infoVersion = readVersionsFromInfo(restClient, hosts.size());
            Version esVersion;
            try {
                Tuple<Version, Version> versionVersionTuple = readVersionsFromCatNodes(restClient);
                esVersion = versionVersionTuple.v1();
                Version masterVersion = versionVersionTuple.v2();
                logger.info("initializing yaml client, minimum es version: [{}] master version: [{}] hosts: {}",
                        esVersion, masterVersion, hosts);
            } catch (ResponseException ex) {
                if (ex.getResponse().getStatusLine().getStatusCode() == 403) {
                    logger.warn("Fallback to simple info '/' request, _cat/nodes is not authorized");
                    esVersion = infoVersion;
                    logger.info("initializing yaml client, minimum es version: [{}] hosts: {}", esVersion, hosts);
                } else {
                    throw ex;
                }
            }
            ClientYamlTestClient clientYamlTestClient = initClientYamlTestClient(restSpec, restClient, hosts, esVersion);
            restTestExecutionContext = new ClientYamlTestExecutionContext(clientYamlTestClient, randomizeContentType());
            adminExecutionContext = new ClientYamlTestExecutionContext(clientYamlTestClient, false);
            String[] blacklist = resolvePathsProperty(REST_TESTS_BLACKLIST, null);
            blacklistPathMatchers = new ArrayList<>();
            for (String entry : blacklist) {
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

    protected ClientYamlTestClient initClientYamlTestClient(ClientYamlSuiteRestSpec restSpec, RestClient restClient,
                                                            List<HttpHost> hosts, Version esVersion) throws IOException {
        return new ClientYamlTestClient(restSpec, restClient, hosts, esVersion);
    }

    @Override
    protected void afterIfFailed(List<Throwable> errors) {
        // Dump the stash on failure. Instead of dumping it in true json we escape `\n`s so stack traces are easier to read
        logger.info("Stash dump on failure [{}]",
                Strings.toString(restTestExecutionContext.stash(), true, true)
                        .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t"));
        super.afterIfFailed(errors);
    }

    public static Iterable<Object[]> createParameters() throws Exception {
        String[] paths = resolvePathsProperty(REST_TESTS_SUITE, ""); // default to all tests under the test root
        List<Object[]> tests = new ArrayList<>();
        Map<String, Set<Path>> yamlSuites = loadSuites(paths);
        // yaml suites are grouped by directory (effectively by api)
        for (String api : yamlSuites.keySet()) {
            List<Path> yamlFiles = new ArrayList<>(yamlSuites.get(api));
            for (Path yamlFile : yamlFiles) {
                ClientYamlTestSuite restTestSuite = ClientYamlTestSuite.parse(api, yamlFile);
                for (ClientYamlTestSection testSection : restTestSuite.getTestSections()) {
                    tests.add(new Object[]{ new ClientYamlTestCandidate(restTestSuite, testSection) });
                }
            }
        }

        //sort the candidates so they will always be in the same order before being shuffled, for repeatability
        Collections.sort(tests,
            (o1, o2) -> ((ClientYamlTestCandidate)o1[0]).getTestPath().compareTo(((ClientYamlTestCandidate)o2[0]).getTestPath()));
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
                if (restApi.getMethods().contains("GET") && restApi.isBodySupported()) {
                    if (!restApi.getMethods().contains("POST")) {
                        errorMessage.append("\n- ").append(restApi.getName()).append(" supports GET with a body but doesn't support POST");
                    }
                }
            }
            if (errorMessage.length() > 0) {
                throw new IllegalArgumentException(errorMessage.toString());
            }
        }
    }

    @AfterClass
    public static void clearStatic() {
        blacklistPathMatchers = null;
        restTestExecutionContext = null;
        adminExecutionContext = null;
    }

    private static Tuple<Version, Version> readVersionsFromCatNodes(RestClient restClient) throws IOException {
        // we simply go to the _cat/nodes API and parse all versions in the cluster
        Response response = restClient.performRequest("GET", "/_cat/nodes", Collections.singletonMap("h", "version,master"));
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

    private static Version readVersionsFromInfo(RestClient restClient, int numHosts) throws IOException {
        Version version = null;
        for (int i = 0; i < numHosts; i++) {
            //we don't really use the urls here, we rely on the client doing round-robin to touch all the nodes in the cluster
            Response response = restClient.performRequest("GET", "/");
            ClientYamlTestResponse restTestResponse = new ClientYamlTestResponse(response);
            Object latestVersion = restTestResponse.evaluate("version.number");
            if (latestVersion == null) {
                throw new RuntimeException("elasticsearch version not found in the response");
            }
            final Version currentVersion = Version.fromString(latestVersion.toString());
            if (version == null) {
                version = currentVersion;
            } else if (version.onOrAfter(currentVersion)) {
                version = currentVersion;
            }
        }
        return version;
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

        if (!testCandidate.getSetupSection().isEmpty()) {
            logger.debug("start setup test [{}]", testCandidate.getTestPath());
            for (DoSection doSection : testCandidate.getSetupSection().getDoSections()) {
                executeSection(doSection);
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
            for (DoSection doSection : testCandidate.getTeardownSection().getDoSections()) {
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
        } catch (Exception e) {
            throw new RuntimeException(errorMessage(executableSection, e), e);
        } catch (AssertionError e) {
            throw new AssertionError(errorMessage(executableSection, e), e);
        }
    }

    private String errorMessage(ExecutableSection executableSection, Throwable t) {
        return "Failure at [" + testCandidate.getSuitePath() + ":" + executableSection.getLocation().lineNumber + "]: " + t.getMessage();
    }

    protected boolean randomizeContentType() {
        return true;
    }
}
