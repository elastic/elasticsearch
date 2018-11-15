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

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 * Superclass for tests that interact with an external test cluster using Elasticsearch's {@link RestClient}.
 */
public abstract class ESRestTestCase extends ESTestCase {
    public static final String TRUSTSTORE_PATH = "truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "truststore.password";
    public static final String CLIENT_RETRY_TIMEOUT = "client.retry.timeout";
    public static final String CLIENT_SOCKET_TIMEOUT = "client.socket.timeout";
    public static final String CLIENT_PATH_PREFIX = "client.path.prefix";

    /**
     * Convert the entity from a {@link Response} into a map of maps.
     */
    public static Map<String, Object> entityAsMap(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (XContentParser parser = xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.getEntity().getContent())) {
            return parser.map();
        }
    }

    /**
     * Does any node in the cluster being tested have x-pack installed?
     */
    public static boolean hasXPack() throws IOException {
        if (hasXPack == null) {
            throw new IllegalStateException("must be called inside of a rest test case test");
        }
        return hasXPack;
    }

    private static List<HttpHost> clusterHosts;
    /**
     * A client for the running Elasticsearch cluster
     */
    private static RestClient client;
    /**
     * A client for the running Elasticsearch cluster configured to take test administrative actions like remove all indexes after the test
     * completes
     */
    private static RestClient adminClient;
    private static Boolean hasXPack;
    private static TreeSet<Version> nodeVersions;

    @Before
    public void initClient() throws IOException {
        if (client == null) {
            assert adminClient == null;
            assert clusterHosts == null;
            assert hasXPack == null;
            assert nodeVersions == null;
            String cluster = System.getProperty("tests.rest.cluster");
            if (cluster == null) {
                throw new RuntimeException("Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] "
                        + "to which to send REST requests");
            }
            String[] stringUrls = cluster.split(",");
            List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
            for (String stringUrl : stringUrls) {
                int portSeparator = stringUrl.lastIndexOf(':');
                if (portSeparator < 0) {
                    throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
                }
                String host = stringUrl.substring(0, portSeparator);
                int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
                hosts.add(buildHttpHost(host, port));
            }
            clusterHosts = unmodifiableList(hosts);
            logger.info("initializing REST clients against {}", clusterHosts);
            client = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));
            adminClient = buildClient(restAdminSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));

            hasXPack = false;
            nodeVersions = new TreeSet<>();
            Map<?, ?> response = entityAsMap(adminClient.performRequest(new Request("GET", "_nodes/plugins")));
            Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
            for (Map.Entry<?, ?> node : nodes.entrySet()) {
                Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
                nodeVersions.add(Version.fromString(nodeInfo.get("version").toString()));
                for (Object module: (List<?>) nodeInfo.get("modules")) {
                    Map<?, ?> moduleInfo = (Map<?, ?>) module;
                    if (moduleInfo.get("name").toString().startsWith("x-pack-")) {
                        hasXPack = true;
                    }
                }
            }
        }
        assert client != null;
        assert adminClient != null;
        assert clusterHosts != null;
        assert hasXPack != null;
        assert nodeVersions != null;
    }

    /**
     * Construct an HttpHost from the given host and port
     */
    protected HttpHost buildHttpHost(String host, int port) {
        return new HttpHost(host, port, getProtocol());
    }

    /**
     * Clean up after the test case.
     */
    @After
    public final void cleanUpCluster() throws Exception {
        if (preserveClusterUponCompletion() == false) {
            wipeCluster();
            waitForClusterStateUpdatesToFinish();
            logIfThereAreRunningTasks();
        }
    }

    @AfterClass
    public static void closeClients() throws IOException {
        try {
            IOUtils.close(client, adminClient);
        } finally {
            clusterHosts = null;
            client = null;
            adminClient = null;
            hasXPack = null;
            nodeVersions = null;
        }
    }

    /**
     * Get the client used for ordinary api calls while writing a test
     */
    protected static RestClient client() {
        return client;
    }

    /**
     * Get the client used for test administrative actions. Do not use this while writing a test. Only use it for cleaning up after tests.
     */
    protected static RestClient adminClient() {
        return adminClient;
    }

    /**
     * Wait for outstanding tasks to complete. The specified admin client is used to check the outstanding tasks and this is done using
     * {@link ESTestCase#assertBusy(CheckedRunnable)} to give a chance to any outstanding tasks to complete.
     *
     * @param adminClient the admin client
     * @throws Exception if an exception is thrown while checking the outstanding tasks
     */
    public static void waitForPendingTasks(final RestClient adminClient) throws Exception {
        waitForPendingTasks(adminClient, taskName -> false);
    }

    /**
     * Wait for outstanding tasks to complete. The specified admin client is used to check the outstanding tasks and this is done using
     * {@link ESTestCase#assertBusy(CheckedRunnable)} to give a chance to any outstanding tasks to complete. The specified filter is used
     * to filter out outstanding tasks that are expected to be there.
     *
     * @param adminClient the admin client
     * @param taskFilter  predicate used to filter tasks that are expected to be there
     * @throws Exception if an exception is thrown while checking the outstanding tasks
     */
    public static void waitForPendingTasks(final RestClient adminClient, final Predicate<String> taskFilter) throws Exception {
        assertBusy(() -> {
            try {
                final Request request = new Request("GET", "/_cat/tasks");
                request.addParameter("detailed", "true");
                final Response response = adminClient.performRequest(request);
                /*
                 * Check to see if there are outstanding tasks; we exclude the list task itself, and any expected outstanding tasks using
                 * the specified task filter.
                 */
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    try (BufferedReader responseReader = new BufferedReader(
                            new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
                        int activeTasks = 0;
                        String line;
                        final StringBuilder tasksListString = new StringBuilder();
                        while ((line = responseReader.readLine()) != null) {
                            final String taskName = line.split("\\s+")[0];
                            if (taskName.startsWith(ListTasksAction.NAME) || taskFilter.test(taskName)) {
                                continue;
                            }
                            activeTasks++;
                            tasksListString.append(line);
                            tasksListString.append('\n');
                        }
                        assertEquals(activeTasks + " active tasks found:\n" + tasksListString, 0, activeTasks);
                    }
                }
            } catch (final IOException e) {
                throw new AssertionError("error getting active tasks list", e);
            }
        });
    }

    /**
     * Returns whether to preserve the state of the cluster upon completion of this test. Defaults to false. If true, overrides the value of
     * {@link #preserveIndicesUponCompletion()}, {@link #preserveTemplatesUponCompletion()}, {@link #preserveReposUponCompletion()},
     * {@link #preserveSnapshotsUponCompletion()},{@link #preserveRollupJobsUponCompletion()},
     * and {@link #preserveILMPoliciesUponCompletion()}.
     *
     * @return true if the state of the cluster should be preserved
     */
    protected boolean preserveClusterUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve the indices created during this test on completion of this test.
     * Defaults to {@code false}. Override this method if indices should be preserved after the test,
     * with the assumption that some other process or test will clean up the indices afterward.
     * This is useful if the data directory and indices need to be preserved between test runs
     * (for example, when testing rolling upgrades).
     */
    protected boolean preserveIndicesUponCompletion() {
        return false;
    }

    /**
     * Controls whether or not to preserve templates upon completion of this test. The default implementation is to delete not preserve
     * templates.
     *
     * @return whether or not to preserve templates
     */
    protected boolean preserveTemplatesUponCompletion() {
        return false;
    }

    /**
     * Controls whether or not to preserve cluster settings upon completion of the test. The default implementation is to remove all cluster
     * settings.
     *
     * @return true if cluster settings should be preserved and otherwise false
     */
    protected boolean preserveClusterSettings() {
        return false;
    }

    /**
     * Returns whether to preserve the repositories on completion of this test.
     * Defaults to not preserving repos. See also
     * {@link #preserveSnapshotsUponCompletion()}.
     */
    protected boolean preserveReposUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve the snapshots in repositories on completion of this
     * test. Defaults to not preserving snapshots. Only works for {@code fs} repositories.
     */
    protected boolean preserveSnapshotsUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve the rollup jobs of this test. Defaults to
     * not preserving them. Only runs at all if xpack is installed on the
     * cluster being tested.
     */
    protected boolean preserveRollupJobsUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve ILM Policies of this test. Defaults to not
     * preserviing them. Only runs at all if xpack is installed on the cluster
     * being tested.
     */
    protected boolean preserveILMPoliciesUponCompletion() {
        return false;
    }

    private void wipeCluster() throws Exception {
        if (preserveIndicesUponCompletion() == false) {
            // wipe indices
            try {
                adminClient().performRequest(new Request("DELETE", "*"));
            } catch (ResponseException e) {
                // 404 here just means we had no indexes
                if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                    throw e;
                }
            }
        }

        // wipe index templates
        if (preserveTemplatesUponCompletion() == false) {
            if (hasXPack) {
                /*
                 * Delete only templates that xpack doesn't automatically
                 * recreate. Deleting them doesn't hurt anything, but it
                 * slows down the test because xpack will just recreate
                 * them.
                 */
                Request request = new Request("GET", "_cat/templates");
                request.addParameter("h", "name");
                String templates = EntityUtils.toString(adminClient().performRequest(request).getEntity());
                if (false == "".equals(templates)) {
                    for (String template : templates.split("\n")) {
                        if (isXPackTemplate(template)) continue;
                        if ("".equals(template)) {
                            throw new IllegalStateException("empty template in templates list:\n" + templates);
                        }
                        logger.debug("Clearing template [{}]", template);
                        adminClient().performRequest(new Request("DELETE", "_template/" + template));
                    }
                }
            } else {
                logger.debug("Clearing all templates");
                adminClient().performRequest(new Request("DELETE", "_template/*"));
            }
        }

        wipeSnapshots();

        // wipe cluster settings
        if (preserveClusterSettings() == false) {
            wipeClusterSettings();
        }

        if (hasXPack && false == preserveRollupJobsUponCompletion()) {
            wipeRollupJobs();
            waitForPendingRollupTasks();
        }

        if (hasXPack && false == preserveILMPoliciesUponCompletion()) {
            deleteAllPolicies();
        }
    }

    /**
     * Wipe fs snapshots we created one by one and all repositories so that the next test can create the repositories fresh and they'll
     * start empty. There isn't an API to delete all snapshots. There is an API to delete all snapshot repositories but that leaves all of
     * the snapshots intact in the repository.
     */
    private void wipeSnapshots() throws IOException {
        for (Map.Entry<String, ?> repo : entityAsMap(adminClient.performRequest(new Request("GET", "/_snapshot/_all"))).entrySet()) {
            String repoName = repo.getKey();
            Map<?, ?> repoSpec = (Map<?, ?>) repo.getValue();
            String repoType = (String) repoSpec.get("type");
            if (false == preserveSnapshotsUponCompletion() && repoType.equals("fs")) {
                // All other repo types we really don't have a chance of being able to iterate properly, sadly.
                Request listRequest = new Request("GET", "/_snapshot/" + repoName + "/_all");
                listRequest.addParameter("ignore_unavailable", "true");
                List<?> snapshots = (List<?>) entityAsMap(adminClient.performRequest(listRequest)).get("snapshots");
                for (Object snapshot : snapshots) {
                    Map<?, ?> snapshotInfo = (Map<?, ?>) snapshot;
                    String name = (String) snapshotInfo.get("snapshot");
                    logger.debug("wiping snapshot [{}/{}]", repoName, name);
                    adminClient().performRequest(new Request("DELETE", "/_snapshot/" + repoName + "/" + name));
                }
            }
            if (preserveReposUponCompletion() == false) {
                logger.debug("wiping snapshot repository [{}]", repoName);
                adminClient().performRequest(new Request("DELETE", "_snapshot/" + repoName));
            }
        }
    }

    /**
     * Remove any cluster settings.
     */
    private void wipeClusterSettings() throws IOException {
        Map<?, ?> getResponse = entityAsMap(adminClient().performRequest(new Request("GET", "/_cluster/settings")));

        boolean mustClear = false;
        XContentBuilder clearCommand = JsonXContent.contentBuilder();
        clearCommand.startObject();
        for (Map.Entry<?, ?> entry : getResponse.entrySet()) {
            String type = entry.getKey().toString();
            Map<?, ?> settings = (Map<?, ?>) entry.getValue();
            if (settings.isEmpty()) {
                continue;
            }
            mustClear = true;
            clearCommand.startObject(type);
            for (Object key: settings.keySet()) {
                clearCommand.field(key + ".*").nullValue();
            }
            clearCommand.endObject();
        }
        clearCommand.endObject();

        if (mustClear) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(Strings.toString(clearCommand));
            adminClient().performRequest(request);
        }
    }

    private void wipeRollupJobs() throws IOException, InterruptedException {
        Response response = adminClient().performRequest(new Request("GET", "/_xpack/rollup/job/_all"));
        Map<String, Object> jobs = entityAsMap(response);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobConfigs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", jobs);

        if (jobConfigs == null) {
            return;
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            @SuppressWarnings("unchecked")
            String jobId = (String) ((Map<String, Object>) jobConfig.get("config")).get("id");
            Request request = new Request("POST", "/_xpack/rollup/job/" + jobId + "/_stop");
            request.addParameter("ignore", "404");
            request.addParameter("wait_for_completion", "true");
            request.addParameter("timeout", "10s");
            logger.debug("stopping rollup job [{}]", jobId);
            adminClient().performRequest(request);
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            @SuppressWarnings("unchecked")
            String jobId = (String) ((Map<String, Object>) jobConfig.get("config")).get("id");
            Request request = new Request("DELETE", "/_xpack/rollup/job/" + jobId);
            request.addParameter("ignore", "404"); // Ignore 404s because they imply someone was racing us to delete this
            logger.debug("deleting rollup job [{}]", jobId);
            adminClient().performRequest(request);
        }
    }

    private void waitForPendingRollupTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith("xpack/rollup/job") == false);
    }

    private static void deleteAllPolicies() throws IOException {
        Map<String, Object> policies;

        try {
            Response response = adminClient().performRequest(new Request("GET", "/_ilm/policy"));
            policies = entityAsMap(response);
        } catch (ResponseException e) {
            if (RestStatus.METHOD_NOT_ALLOWED.getStatus() == e.getResponse().getStatusLine().getStatusCode()) {
                // If bad request returned, ILM is not enabled.
                return;
            }
            throw e;
        }

        if (policies == null || policies.isEmpty()) {
            return;
        }

        for (String policyName : policies.keySet()) {
            adminClient().performRequest(new Request("DELETE", "/_ilm/policy/" + policyName));
        }
    }

    /**
     * Logs a message if there are still running tasks. The reasoning is that any tasks still running are state the is trying to bleed into
     * other tests.
     */
    private void logIfThereAreRunningTasks() throws InterruptedException, IOException {
        Set<String> runningTasks = runningTasks(adminClient().performRequest(new Request("GET", "/_tasks")));
        // Ignore the task list API - it doesn't count against us
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

    /**
     * Waits for the cluster state updates to have been processed, so that no cluster
     * state updates are still in-progress when the next test starts.
     */
    private void waitForClusterStateUpdatesToFinish() throws Exception {
        assertBusy(() -> {
            try {
                Response response = adminClient().performRequest(new Request("GET", "/_cluster/pending_tasks"));
                List<?> tasks = (List<?>) entityAsMap(response).get("tasks");
                if (false == tasks.isEmpty()) {
                    StringBuilder message = new StringBuilder("there are still running tasks:");
                    for (Object task: tasks) {
                        message.append('\n').append(task.toString());
                    }
                    fail(message.toString());
                }
            } catch (IOException e) {
                fail("cannot get cluster's pending tasks: " + e.getMessage());
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Used to obtain settings for the REST client that is used to send REST requests.
     */
    protected Settings restClientSettings() {
        Settings.Builder builder = Settings.builder();
        if (System.getProperty("tests.rest.client_path_prefix") != null) {
            builder.put(CLIENT_PATH_PREFIX, System.getProperty("tests.rest.client_path_prefix"));
        }
        return builder.build();
    }

    /**
     * Returns the REST client settings used for admin actions like cleaning up after the test has completed.
     */
    protected Settings restAdminSettings() {
        return restClientSettings(); // default to the same client settings
    }

    /**
     * Get the list of hosts in the cluster.
     */
    protected final List<HttpHost> getClusterHosts() {
        return clusterHosts;
    }

    /**
     * Override this to switch to testing https.
     */
    protected String getProtocol() {
        return "http";
    }

    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(getStrictDeprecationMode());
        return builder.build();
    }

    /**
     * Whether the used REST client should return any response containing at
     * least one warning header as a failure.
     */
    protected boolean getStrictDeprecationMode() {
        return true;
    }

    protected static void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        String keystorePath = settings.get(TRUSTSTORE_PATH);
        if (keystorePath != null) {
            final String keystorePass = settings.get(TRUSTSTORE_PASSWORD);
            if (keystorePass == null) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is provided but not " + TRUSTSTORE_PASSWORD);
            }
            Path path = PathUtils.get(keystorePath);
            if (!Files.exists(path)) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is set but points to a non-existing file");
            }
            try {
                KeyStore keyStore = KeyStore.getInstance("jks");
                try (InputStream is = Files.newInputStream(path)) {
                    keyStore.load(is, keystorePass.toCharArray());
                }
                SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
                SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslcontext);
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy));
            } catch (KeyStoreException |NoSuchAlgorithmException |KeyManagementException |CertificateException e) {
                throw new RuntimeException("Error setting up ssl", e);
            }
        }
        try (ThreadContext threadContext = new ThreadContext(settings)) {
            Header[] defaultHeaders = new Header[threadContext.getHeaders().size()];
            int i = 0;
            for (Map.Entry<String, String> entry : threadContext.getHeaders().entrySet()) {
                defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
            }
            builder.setDefaultHeaders(defaultHeaders);
        }
        final String requestTimeoutString = settings.get(CLIENT_RETRY_TIMEOUT);
        if (requestTimeoutString != null) {
            final TimeValue maxRetryTimeout = TimeValue.parseTimeValue(requestTimeoutString, CLIENT_RETRY_TIMEOUT);
            builder.setMaxRetryTimeoutMillis(Math.toIntExact(maxRetryTimeout.getMillis()));
        }
        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        if (socketTimeoutString != null) {
            final TimeValue socketTimeout = TimeValue.parseTimeValue(socketTimeoutString, CLIENT_SOCKET_TIMEOUT);
            builder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
        }
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> runningTasks(Response response) throws IOException {
        Set<String> runningTasks = new HashSet<>();

        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(response).get("nodes");
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

    protected static void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    /**
     * checks that the specific index is green. we force a selection of an index as the tests share a cluster and often leave indices
     * in an non green state
     * @param index index to test for
     **/
    protected static void ensureGreen(String index) throws IOException {
        Request request = new Request("GET", "/_cluster/health/" + index);
        request.addParameter("wait_for_status", "green");
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("timeout", "70s");
        request.addParameter("level", "shards");
        client().performRequest(request);
    }

    /**
     * waits until all shard initialization is completed. This is a handy alternative to ensureGreen as it relates to all shards
     * in the cluster and doesn't require to know how many nodes/replica there are.
     */
    protected static void ensureNoInitializingShards() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        request.addParameter("wait_for_no_initializing_shards", "true");
        request.addParameter("timeout", "70s");
        request.addParameter("level", "shards");
        client().performRequest(request);
    }

    protected static void createIndex(String name, Settings settings) throws IOException {
        createIndex(name, settings, "");
    }

    protected static void createIndex(String name, Settings settings, String mapping) throws IOException {
        Request request = new Request("PUT", "/" + name);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings)
                + ", \"mappings\" : {" + mapping + "} }");
        client().performRequest(request);
    }

    protected static void createIndex(String name, Settings settings, String mapping, String aliases) throws IOException {
        Request request = new Request("PUT", "/" + name);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings)
            + ", \"mappings\" : {" + mapping + "}"
            + ", \"aliases\": {" + aliases + "} }");
        client().performRequest(request);
    }

    protected static void deleteIndex(String name) throws IOException {
        Request request = new Request("DELETE", "/" + name);
        client().performRequest(request);
    }

    protected static void updateIndexSettings(String index, Settings.Builder settings) throws IOException {
        updateIndexSettings(index, settings.build());
    }

    private static void updateIndexSettings(String index, Settings settings) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_settings");
        request.setJsonEntity(Strings.toString(settings));
        client().performRequest(request);
    }

    protected static Map<String, Object> getIndexSettings(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        Response response = client().performRequest(request);
        try (InputStream is = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }
    }

    protected static boolean indexExists(String index) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/" + index));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    protected static void closeIndex(String index) throws IOException {
        Response response = client().performRequest(new Request("POST", "/" + index + "/_close"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    protected static void openIndex(String index) throws IOException {
        Response response = client().performRequest(new Request("POST", "/" + index + "/_open"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    protected static boolean aliasExists(String alias) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/_alias/" + alias));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    protected static boolean aliasExists(String index, String alias) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/" + index + "/_alias/" + alias));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> getAlias(final String index, final String alias) throws IOException {
        String endpoint = "/_alias";
        if (false == Strings.isEmpty(index)) {
            endpoint = index + endpoint;
        }
        if (false == Strings.isEmpty(alias)) {
            endpoint = endpoint + "/" + alias;
        }
        Map<String, Object> getAliasResponse = getAsMap(endpoint);
        return (Map<String, Object>)XContentMapValues.extractValue(index + ".aliases." + alias, getAliasResponse);
    }

    protected static Map<String, Object> getAsMap(final String endpoint) throws IOException {
        Response response = client().performRequest(new Request("GET", endpoint));
        XContentType entityContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        Map<String, Object> responseEntity = XContentHelper.convertToMap(entityContentType.xContent(),
                response.getEntity().getContent(), false);
        assertNotNull(responseEntity);
        return responseEntity;
    }

    /**
     * Is this template one that is automatically created by xpack?
     */
    private static boolean isXPackTemplate(String name) {
        if (name.startsWith(".monitoring-")) {
            return true;
        }
        if (name.startsWith(".watch") || name.startsWith(".triggered_watches")) {
            return true;
        }
        if (name.startsWith(".ml-")) {
            return true;
        }
        switch (name) {
        case ".triggered_watches":
        case ".watches":
        case "logstash-index-template":
        case "security_audit_log":
            return true;
        default:
            return false;
        }
    }

}
