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

package org.elasticsearch.test.rest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.client.RestException;
import org.elasticsearch.test.rest.client.RestResponse;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.elasticsearch.test.rest.parser.RestTestSuiteParser;
import org.elasticsearch.test.rest.section.DoSection;
import org.elasticsearch.test.rest.section.ExecutableSection;
import org.elasticsearch.test.rest.section.RestTestSuite;
import org.elasticsearch.test.rest.section.SkipSection;
import org.elasticsearch.test.rest.section.TestSection;
import org.elasticsearch.test.rest.spec.RestApi;
import org.elasticsearch.test.rest.spec.RestSpec;
import org.elasticsearch.test.rest.support.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.sort;

/**
 * Runs the clients test suite against an elasticsearch cluster.
 */
public abstract class ESRestTestCase extends ESTestCase {

    /**
     * Property that allows to control which REST tests get run. Supports comma separated list of tests
     * or directories that contain tests e.g. -Dtests.rest.suite=index,get,create/10_with_id
     */
    public static final String REST_TESTS_SUITE = "tests.rest.suite";
    /**
     * Property that allows to blacklist some of the REST tests based on a comma separated list of globs
     * e.g. -Dtests.rest.blacklist=get/10_basic/*
     */
    public static final String REST_TESTS_BLACKLIST = "tests.rest.blacklist";
    /**
     * Property that allows to control whether spec validation is enabled or not (default true).
     */
    public static final String REST_TESTS_VALIDATE_SPEC = "tests.rest.validate_spec";
    /**
     * Property that allows to control where the REST spec files need to be loaded from
     */
    public static final String REST_TESTS_SPEC = "tests.rest.spec";

    public static final String REST_LOAD_PACKAGED_TESTS = "tests.rest.load_packaged";

    private static final String DEFAULT_TESTS_PATH = "/rest-api-spec/test";
    private static final String DEFAULT_SPEC_PATH = "/rest-api-spec/api";

    /**
     * This separator pattern matches ',' except it is preceded by a '\'. This allows us to support ',' within paths when it is escaped with
     * a slash.
     *
     * For example, the path string "/a/b/c\,d/e/f,/foo/bar,/baz" is separated to "/a/b/c\,d/e/f", "/foo/bar" and "/baz".
     *
     * For reference, this regular expression feature is known as zero-width negative look-behind.
     *
     */
    private static final String PATHS_SEPARATOR = "(?<!\\\\),";

    private final List<BlacklistedPathPatternMatcher> blacklistPathMatchers = new ArrayList<>();
    private final URL[] clusterUrls;
    private static RestTestExecutionContext restTestExecutionContext;
    private static RestTestExecutionContext adminExecutionContext;

    private final RestTestCandidate testCandidate;

    public ESRestTestCase(RestTestCandidate testCandidate) {
        this.testCandidate = testCandidate;
        String[] blacklist = resolvePathsProperty(REST_TESTS_BLACKLIST, null);
        for (String entry : blacklist) {
            this.blacklistPathMatchers.add(new BlacklistedPathPatternMatcher(entry));
        }
        String cluster = System.getProperty("tests.rest.cluster");
        if (cluster == null) {
            throw new RuntimeException("Must specify tests.rest.cluster for rest tests");
        }
        String[] stringUrls = cluster.split(",");
        clusterUrls = new URL[stringUrls.length];
        int i = 0;
        try {
            for (String stringUrl : stringUrls) {
                clusterUrls[i++] = new URL("http://" + stringUrl);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse cluster addresses for rest test", e);
        }
    }

    @Override
    protected void afterIfFailed(List<Throwable> errors) {
        logger.info("Stash dump on failure [{}]", XContentHelper.toString(restTestExecutionContext.stash()));
        super.afterIfFailed(errors);
    }

    public static Iterable<Object[]> createParameters(int id, int count) throws IOException, RestTestParseException {
        //parse tests only if rest test group is enabled, otherwise rest tests might not even be available on file system
        List<RestTestCandidate> restTestCandidates = collectTestCandidates(id, count);
        List<Object[]> objects = new ArrayList<>();
        for (RestTestCandidate restTestCandidate : restTestCandidates) {
            objects.add(new Object[]{restTestCandidate});
        }
        return objects;
    }

    private static List<RestTestCandidate> collectTestCandidates(int id, int count) throws RestTestParseException, IOException {
        List<RestTestCandidate> testCandidates = new ArrayList<>();
        FileSystem fileSystem = getFileSystem();
        // don't make a try-with, getFileSystem returns null
        // ... and you can't close() the default filesystem
        try {
            String[] paths = resolvePathsProperty(REST_TESTS_SUITE, DEFAULT_TESTS_PATH);
            Map<String, Set<Path>> yamlSuites = FileUtils.findYamlSuites(fileSystem, DEFAULT_TESTS_PATH, paths);
            RestTestSuiteParser restTestSuiteParser = new RestTestSuiteParser();
            //yaml suites are grouped by directory (effectively by api)
            for (String api : yamlSuites.keySet()) {
                List<Path> yamlFiles = new ArrayList<>(yamlSuites.get(api));
                for (Path yamlFile : yamlFiles) {
                    String key = api + yamlFile.getFileName().toString();
                    if (mustExecute(key, id, count)) {
                        RestTestSuite restTestSuite = restTestSuiteParser.parse(api, yamlFile);
                        for (TestSection testSection : restTestSuite.getTestSections()) {
                            testCandidates.add(new RestTestCandidate(restTestSuite, testSection));
                        }
                    }
                }
            }
        } finally {
            IOUtils.close(fileSystem);
        }

        //sort the candidates so they will always be in the same order before being shuffled, for repeatability
        Collections.sort(testCandidates, new Comparator<RestTestCandidate>() {
            @Override
            public int compare(RestTestCandidate o1, RestTestCandidate o2) {
                return o1.getTestPath().compareTo(o2.getTestPath());
            }
        });

        return testCandidates;
    }

    private static boolean mustExecute(String test, int id, int count) {
        int hash = (int) (Math.abs((long)test.hashCode()) % count);
        return hash == id;
    }

    private static String[] resolvePathsProperty(String propertyName, String defaultValue) {
        String property = System.getProperty(propertyName);
        if (!Strings.hasLength(property)) {
            return defaultValue == null ? Strings.EMPTY_ARRAY : new String[]{defaultValue};
        } else {
            return property.split(PATHS_SEPARATOR);
        }
    }

    /**
     * Returns a new FileSystem to read REST resources, or null if they
     * are available from classpath.
     */
    @SuppressForbidden(reason = "proper use of URL, hack around a JDK bug")
    static FileSystem getFileSystem() throws IOException {
        // REST suite handling is currently complicated, with lots of filtering and so on
        // For now, to work embedded in a jar, return a ZipFileSystem over the jar contents.
        URL codeLocation = FileUtils.class.getProtectionDomain().getCodeSource().getLocation();
        boolean loadPackaged = RandomizedTest.systemPropertyAsBoolean(REST_LOAD_PACKAGED_TESTS, true);
        if (codeLocation.getFile().endsWith(".jar") && loadPackaged) {
            try {
                // hack around a bug in the zipfilesystem implementation before java 9,
                // its checkWritable was incorrect and it won't work without write permissions.
                // if we add the permission, it will open jars r/w, which is too scary! so copy to a safe r-w location.
                Path tmp = Files.createTempFile(null, ".jar");
                try (InputStream in = codeLocation.openStream()) {
                    Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                }
                return FileSystems.newFileSystem(new URI("jar:" + tmp.toUri()), Collections.<String,Object>emptyMap());
            } catch (URISyntaxException e) {
                throw new IOException("couldn't open zipfilesystem: ", e);
            }
        } else {
            return null;
        }
    }

    @BeforeClass
    public static void initExecutionContext() throws IOException, RestException {
        String[] specPaths = resolvePathsProperty(REST_TESTS_SPEC, DEFAULT_SPEC_PATH);
        RestSpec restSpec = null;
        FileSystem fileSystem = getFileSystem();
        // don't make a try-with, getFileSystem returns null
        // ... and you can't close() the default filesystem
        try {
            restSpec = RestSpec.parseFrom(fileSystem, DEFAULT_SPEC_PATH, specPaths);
        } finally {
            IOUtils.close(fileSystem);
        }
        validateSpec(restSpec);
        restTestExecutionContext = new RestTestExecutionContext(restSpec);
        adminExecutionContext = new RestTestExecutionContext(restSpec);
    }

    protected RestTestExecutionContext getAdminExecutionContext() {
        return adminExecutionContext;
    }

    private static void validateSpec(RestSpec restSpec) {
        boolean validateSpec = RandomizedTest.systemPropertyAsBoolean(REST_TESTS_VALIDATE_SPEC, true);
        if (validateSpec) {
            StringBuilder errorMessage = new StringBuilder();
            for (RestApi restApi : restSpec.getApis()) {
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

    @After
    public void wipeCluster() throws Exception {
        // wipe indices
        Map<String, String> deleteIndicesArgs = new HashMap<>();
        deleteIndicesArgs.put("index", "*");
        try {
            adminExecutionContext.callApi("indices.delete", deleteIndicesArgs, Collections.emptyList(), Collections.emptyMap());
        } catch (RestException e) {
            // 404 here just means we had no indexes
            if (e.statusCode() != 404) {
                throw e;
            }
        }

        // wipe index templates
        Map<String, String> deleteTemplatesArgs = new HashMap<>();
        deleteTemplatesArgs.put("name", "*");
        adminExecutionContext.callApi("indices.delete_template", deleteTemplatesArgs, Collections.emptyList(), Collections.emptyMap());

        // wipe snapshots
        Map<String, String> deleteSnapshotsArgs = new HashMap<>();
        deleteSnapshotsArgs.put("repository", "*");
        adminExecutionContext.callApi("snapshot.delete_repository", deleteSnapshotsArgs, Collections.emptyList(), Collections.emptyMap());
    }

    /**
     * Logs a message if there are still running tasks. The reasoning is that any tasks still running are state the is trying to bleed into
     * other tests.
     */
    @After
    public void logIfThereAreRunningTasks() throws InterruptedException, IOException, RestException {
        RestResponse tasks = adminExecutionContext.callApi("tasks.list", emptyMap(), emptyList(), emptyMap());
        Set<String> runningTasks = runningTasks(tasks);
        // Ignore the task list API - it doens't count against us
        runningTasks.remove(ListTasksAction.NAME);
        runningTasks.remove(ListTasksAction.NAME + "[n]");
        if (runningTasks.isEmpty()) {
            return;
        }
        List<String> stillRunning = new ArrayList<>(runningTasks);
        sort(stillRunning);
        logger.info("There are still tasks running after this test that might break subsequent tests {}.", stillRunning);
        /*
         * This isn't a higher level log or outright failure because some of these tasks are run by the cluster in the background. If we
         * could determine that some tasks are run by the user we'd fail the tests if those tasks were running and ignore any background
         * tasks.
         */
    }

    @AfterClass
    public static void close() {
        if (restTestExecutionContext != null) {
            restTestExecutionContext.close();
            adminExecutionContext.close();
            restTestExecutionContext = null;
            adminExecutionContext = null;
        }
    }

    /**
     * Used to obtain settings for the REST client that is used to send REST requests.
     */
    protected Settings restClientSettings() {
        return Settings.EMPTY;
    }

    /** Returns the REST client settings used for admin actions like cleaning up after the test has completed. */
    protected Settings restAdminSettings() {
        return restClientSettings(); // default to the same client settings
    }

    /** Returns the addresses the client uses to connect to the test cluster. */
    protected URL[] getClusterUrls() {
        return clusterUrls;
    }

    @Before
    public void reset() throws IOException, RestException {
        // admin context must be available for @After always, regardless of whether the test was blacklisted
        adminExecutionContext.initClient(clusterUrls, restAdminSettings());
        adminExecutionContext.clear();

        //skip test if it matches one of the blacklist globs
        for (BlacklistedPathPatternMatcher blacklistedPathMatcher : blacklistPathMatchers) {
            String testPath = testCandidate.getSuitePath() + "/" + testCandidate.getTestSection().getName();
            assumeFalse("[" + testCandidate.getTestPath() + "] skipped, reason: blacklisted", blacklistedPathMatcher.isSuffixMatch(testPath));
        }
        //The client needs non static info to get initialized, therefore it can't be initialized in the before class
        restTestExecutionContext.initClient(clusterUrls, restClientSettings());
        restTestExecutionContext.clear();

        //skip test if the whole suite (yaml file) is disabled
        assumeFalse(buildSkipMessage(testCandidate.getSuitePath(), testCandidate.getSetupSection().getSkipSection()),
                testCandidate.getSetupSection().getSkipSection().skip(restTestExecutionContext.esVersion()));
        //skip test if test section is disabled
        assumeFalse(buildSkipMessage(testCandidate.getTestPath(), testCandidate.getTestSection().getSkipSection()),
                testCandidate.getTestSection().getSkipSection().skip(restTestExecutionContext.esVersion()));
    }

    private static String buildSkipMessage(String description, SkipSection skipSection) {
        StringBuilder messageBuilder = new StringBuilder();
        if (skipSection.isVersionCheck()) {
            messageBuilder.append("[").append(description).append("] skipped, reason: [").append(skipSection.getReason()).append("] ");
        } else {
            messageBuilder.append("[").append(description).append("] skipped, reason: features ").append(skipSection.getFeatures()).append(" not supported");
        }
        return messageBuilder.toString();
    }

    public void test() throws IOException {
        //let's check that there is something to run, otherwise there might be a problem with the test section
        if (testCandidate.getTestSection().getExecutableSections().size() == 0) {
            throw new IllegalArgumentException("No executable sections loaded for [" + testCandidate.getTestPath() + "]");
        }

        if (!testCandidate.getSetupSection().isEmpty()) {
            logger.info("start setup test [{}]", testCandidate.getTestPath());
            for (DoSection doSection : testCandidate.getSetupSection().getDoSections()) {
                doSection.execute(restTestExecutionContext);
            }
            logger.info("end setup test [{}]", testCandidate.getTestPath());
        }

        restTestExecutionContext.clear();

        for (ExecutableSection executableSection : testCandidate.getTestSection().getExecutableSections()) {
            executableSection.execute(restTestExecutionContext);
        }
    }

    @SuppressWarnings("unchecked")
    public Set<String> runningTasks(RestResponse response) throws IOException {
        Set<String> runningTasks = new HashSet<>();
        Map<String, Object> nodes = (Map<String, Object>) response.evaluate("nodes");
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            Map<String, Object> nodeInfo = (Map<String, Object>) node.getValue();
            Map<String, Object> nodeTasks = (Map<String, Object>) nodeInfo.get("tasks");
            for (Map.Entry<String, Object> taskAndName : nodeTasks.entrySet()) {
                Map<String, Object> task = (Map<String, Object>) taskAndName.getValue();
                runningTasks.add(task.get("action").toString());
            }
        }
        return runningTasks;
    }
}
