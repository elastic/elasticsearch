/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RequestOptions.Builder;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;

import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.action.admin.indices.RestCloseIndexAction.WAIT_FOR_ACTIVE_SHARDS_DEFAULT_DEPRECATION_MESSAGE;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Superclass for tests that interact with an external test cluster using Elasticsearch's {@link RestClient}.
 */
public abstract class ESRestTestCase extends ESTestCase {
    public static final String TRUSTSTORE_PATH = "truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "truststore.password";

    public static final String CERTIFICATE_AUTHORITIES = "certificate_authorities";

    public static final String CLIENT_CERT_PATH = "client.cert.path";
    public static final String CLIENT_KEY_PATH = "client.key.path";
    public static final String CLIENT_KEY_PASSWORD = "client.key.password";

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
     * Convert the entity from a {@link Response} into a list of maps.
     */
    public static List<Object> entityAsList(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (XContentParser parser = xContentType.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            response.getEntity().getContent())) {
            return parser.list();
        }
    }

    /**
     * Does any node in the cluster being tested have x-pack installed?
     */
    public static boolean hasXPack() {
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
            String cluster = getTestRestCluster();
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

    protected String getTestRestCluster() {
        String cluster = System.getProperty("tests.rest.cluster");
        if (cluster == null) {
            throw new RuntimeException("Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] "
                + "to which to send REST requests");
        }
        return cluster;
    }

    /**
     * Helper class to check warnings in REST responses with sensitivity to versions
     * used in the target cluster.
     */
    public static class VersionSensitiveWarningsHandler implements WarningsHandler {
        Set<String> requiredSameVersionClusterWarnings = new HashSet<>();
        Set<String> allowedWarnings = new HashSet<>();
        final Set<Version> testNodeVersions;

        public VersionSensitiveWarningsHandler(Set<Version> nodeVersions) {
            this.testNodeVersions = nodeVersions;
        }

        /**
         * Adds to the set of warnings that are all required in responses if the cluster
         * is formed from nodes all running the exact same version as the client.
         * @param requiredWarnings a set of required warnings
         */
        public void current(String... requiredWarnings) {
            requiredSameVersionClusterWarnings.addAll(Arrays.asList(requiredWarnings));
        }

        /**
         * Adds to the set of warnings that are permissible (but not required) when running
         * in mixed-version clusters or those that differ in version from the test client.
         * @param allowedWarnings optional warnings that will be ignored if received
         */
        public void compatible(String... allowedWarnings) {
            this.allowedWarnings.addAll(Arrays.asList(allowedWarnings));
        }

        @Override
        public boolean warningsShouldFailRequest(List<String> warnings) {
            if (isExclusivelyTargetingCurrentVersionCluster()) {
                // absolute equality required in expected and actual.
                Set<String> actual = new HashSet<>(warnings);
                return false == requiredSameVersionClusterWarnings.equals(actual);
            } else {
                // Some known warnings can safely be ignored
                for (String actualWarning : warnings) {
                    if (false == allowedWarnings.contains(actualWarning) &&
                        false == requiredSameVersionClusterWarnings.contains(actualWarning)) {
                        return true;
                    }
                }
                return false;
            }
        }

        private boolean isExclusivelyTargetingCurrentVersionCluster() {
            assertFalse("Node versions running in the cluster are missing", testNodeVersions.isEmpty());
            return testNodeVersions.size() == 1 &&
                    testNodeVersions.iterator().next().equals(Version.CURRENT);
        }

    }

    public static RequestOptions expectVersionSpecificWarnings(Consumer<VersionSensitiveWarningsHandler> expectationsSetter) {
        Builder builder = RequestOptions.DEFAULT.toBuilder();
        VersionSensitiveWarningsHandler warningsHandler = new VersionSensitiveWarningsHandler(nodeVersions);
        expectationsSetter.accept(warningsHandler);
        builder.setWarningsHandler(warningsHandler);
        return builder.build();
    }

    /**
     * Creates request options designed to be used when making a call that can return warnings, for example a
     * deprecated request. The options will ensure that the given warnings are returned if all nodes are on
     * {@link Version#CURRENT} and will allow (but not require) the warnings if any node is running an older version.
     *
     * @param warnings The expected warnings.
     */
    public static RequestOptions expectWarnings(String... warnings) {
        return expectVersionSpecificWarnings(consumer -> consumer.current(warnings));
    }

    /**
     * Construct a Basic auth header
     * @param username user name
     * @param passwd user password
     */
    public static String basicAuthHeaderValue(String username, SecureString passwd) {
        CharBuffer chars = CharBuffer.allocate(username.length() + passwd.length() + 1);
        byte[] charBytes = null;
        try {
            chars.put(username).put(':').put(passwd.getChars());
            charBytes = CharArrays.toUtf8Bytes(chars.array());

            String basicToken = Base64.getEncoder().encodeToString(charBytes);
            return "Basic " + basicToken;
        } finally {
            Arrays.fill(chars.array(), (char) 0);
            if (charBytes != null) {
                Arrays.fill(charBytes, (byte) 0);
            }
        }
    }

    /**
     * Creates RequestOptions designed to ignore [types removal] warnings but nothing else
     * @deprecated this method is only required while we deprecate types and can be removed in 8.0
     */
    @Deprecated
    public static RequestOptions allowTypesRemovalWarnings() {
        Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setWarningsHandler(new WarningsHandler() {
                @Override
                public boolean warningsShouldFailRequest(List<String> warnings) {
                    for (String warning : warnings) {
                        if(warning.startsWith("[types removal]") == false) {
                            //Something other than a types removal message - return true
                            return true;
                        }
                    }
                    return false;
                }
            });
        return builder.build();
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
            if (nodeVersions.stream().noneMatch(version -> version.before(Version.V_6_2_0))) {
                // wait_for_no_initializing_shards added in 6.2
                ensureNoInitializingShards();
            }
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
        }, 30L, TimeUnit.SECONDS);
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
     * Determines if data streams are preserved upon completion of this test. The default implementation wipes data streams.
     *
     * @return whether or not to preserve data streams
     */
    protected boolean preserveDataStreamsUponCompletion() {
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
     * preserving them. Only runs at all if xpack is installed on the cluster
     * being tested.
     */
    protected boolean preserveILMPoliciesUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve SLM Policies of this test. Defaults to not
     * preserving them. Only runs at all if xpack is installed on the cluster
     * being tested.
     */
    protected boolean preserveSLMPoliciesUponCompletion() {
        return false;
    }

    /**
     * A set of ILM policies that should be preserved between runs.
     */
    protected Set<String> preserveILMPolicyIds() {
        return Sets.newHashSet("ilm-history-ilm-policy", "slm-history-ilm-policy",
            "watch-history-ilm-policy", "ml-size-based-ilm-policy", "logs", "metrics");
    }

    /**
     * Returns whether to preserve auto-follow patterns. Defaults to not
     * preserving them. Only runs at all if xpack is installed on the cluster
     * being tested.
     */
    protected boolean preserveAutoFollowPatternsUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve searchable snapshots indices. Defaults to not
     * preserving them. Only runs at all if xpack is installed on the cluster
     * being tested.
     */
    protected boolean preserveSearchableSnapshotsIndicesUponCompletion() {
        return false;
    }

    /**
     * Returns whether to wait to make absolutely certain that all snapshots
     * have been deleted.
     */
    protected boolean waitForAllSnapshotsWiped() { return false; }

    private void wipeCluster() throws Exception {

        // Cleanup rollup before deleting indices.  A rollup job might have bulks in-flight,
        // so we need to fully shut them down first otherwise a job might stall waiting
        // for a bulk to finish against a non-existing index (and then fail tests)
        if (hasXPack && false == preserveRollupJobsUponCompletion()) {
            wipeRollupJobs();
            waitForPendingRollupTasks();
        }

        // Clean up SLM policies before trying to wipe snapshots so that no new ones get started by SLM after wiping
        if (nodeVersions.first().onOrAfter(Version.V_7_4_0)) { // SLM was introduced in version 7.4
            if (preserveSLMPoliciesUponCompletion() == false) {
                // Clean up SLM policies before trying to wipe snapshots so that no new ones get started by SLM after wiping
                deleteAllSLMPolicies();
            }
        }

        // Clean up searchable snapshots indices before deleting snapshots and repositories
        if (hasXPack() && nodeVersions.first().onOrAfter(Version.V_7_8_0) && preserveSearchableSnapshotsIndicesUponCompletion() == false) {
            wipeSearchableSnapshotsIndices();
        }

        SetOnce<Map<String, List<Map<?,?>>>> inProgressSnapshots = new SetOnce<>();
        if (waitForAllSnapshotsWiped()) {
            AtomicReference<Map<String, List<Map<?,?>>>> snapshots = new AtomicReference<>();
            try {
                // Repeatedly delete the snapshots until there aren't any
                assertBusy(() -> {
                    snapshots.set(wipeSnapshots());
                    assertThat(snapshots.get(), anEmptyMap());
                }, 2, TimeUnit.MINUTES);
                // At this point there should be no snaphots
                inProgressSnapshots.set(snapshots.get());
            } catch (AssertionError e) {
                // This will cause an error at the end of this method, but do the rest of the cleanup first
                inProgressSnapshots.set(snapshots.get());
            }
        } else {
            inProgressSnapshots.set(wipeSnapshots());
        }

        // wipe data streams before indices so that the backing indices for data streams are handled properly
        if (preserveDataStreamsUponCompletion() == false) {
            wipeDataStreams();
        }

        if (preserveIndicesUponCompletion() == false) {
            // wipe indices
            wipeAllIndices();
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
                // In case of bwc testing, if all nodes are before 7.7.0 then no need to attempt to delete component and composable
                // index templates, because these were introduced in 7.7.0:
                if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_7_7_0))) {
                    try {
                        Request getTemplatesRequest = new Request("GET", "_index_template");
                        getTemplatesRequest.setOptions(allowTypesRemovalWarnings());
                        Map<String, Object> composableIndexTemplates = XContentHelper.convertToMap(JsonXContent.jsonXContent,
                            EntityUtils.toString(adminClient().performRequest(getTemplatesRequest).getEntity()), false);
                        List<String> names = ((List<?>) composableIndexTemplates.get("index_templates")).stream()
                            .map(ct -> (String) ((Map<?, ?>) ct).get("name"))
                            .filter(name -> isXPackTemplate(name) == false)
                            .collect(Collectors.toList());
                        if (names.isEmpty() == false) {
                            // Ideally we would want to check the version of the elected master node and
                            // send the delete request directly to that node.
                            if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_7_13_0))) {
                                try {
                                    adminClient().performRequest(new Request("DELETE", "_index_template/" + String.join(",", names)));
                                } catch (ResponseException e) {
                                    logger.warn(
                                        new ParameterizedMessage("unable to remove multiple composable index templates {}", names), e);
                                }
                            } else {
                                for (String name : names) {
                                    try {
                                        adminClient().performRequest(new Request("DELETE", "_index_template/" + name));
                                    } catch (ResponseException e) {
                                        logger.warn(new ParameterizedMessage("unable to remove composable index template {}", name), e);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.debug("ignoring exception removing all composable index templates", e);
                        // We hit a version of ES that doesn't support index templates v2 yet, so it's safe to ignore
                    }
                    try {
                        Request compReq = new Request("GET", "_component_template");
                        compReq.setOptions(allowTypesRemovalWarnings());
                        String componentTemplates = EntityUtils.toString(adminClient().performRequest(compReq).getEntity());
                        Map<String, Object> cTemplates = XContentHelper.convertToMap(JsonXContent.jsonXContent, componentTemplates, false);
                        List<String> names = ((List<?>) cTemplates.get("component_templates")).stream()
                            .map(ct -> (String) ((Map<?, ?>) ct).get("name"))
                            .filter(name -> isXPackTemplate(name) == false)
                            .collect(Collectors.toList());
                        if (names.isEmpty() == false) {
                            // Ideally we would want to check the version of the elected master node and
                            // send the delete request directly to that node.
                            if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_7_13_0))) {
                                try {
                                    adminClient().performRequest(new Request("DELETE", "_component_template/" + String.join(",", names)));
                                } catch (ResponseException e) {
                                    logger.warn(new ParameterizedMessage("unable to remove multiple component templates {}", names), e);
                                }
                            } else {
                                for (String componentTemplate : names) {
                                    try {
                                        adminClient().performRequest(new Request("DELETE", "_component_template/" + componentTemplate));
                                    } catch (ResponseException e) {
                                        logger.warn(
                                            new ParameterizedMessage("unable to remove component template {}", componentTemplate), e);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.debug("ignoring exception removing all component templates", e);
                        // We hit a version of ES that doesn't support index templates v2 yet, so it's safe to ignore
                    }
                }
                // Always check for legacy templates:
                Request getLegacyTemplatesRequest = new Request("GET", "_template");
                getLegacyTemplatesRequest.setOptions(allowTypesRemovalWarnings());
                Map<String, Object> legacyTemplates = XContentHelper.convertToMap(JsonXContent.jsonXContent,
                    EntityUtils.toString(adminClient().performRequest(getLegacyTemplatesRequest).getEntity()), false);
                for (String name : legacyTemplates.keySet()) {
                    if (isXPackTemplate(name)) {
                        continue;
                    }
                    try {
                        adminClient().performRequest(new Request("DELETE", "_template/" + name));
                    } catch (ResponseException e) {
                        logger.debug(new ParameterizedMessage("unable to remove index template {}", name), e);
                    }
                }
            } else {
                logger.debug("Clearing all templates");
                adminClient().performRequest(new Request("DELETE", "_template/*"));
                try {
                    adminClient().performRequest(new Request("DELETE", "_index_template/*"));
                    adminClient().performRequest(new Request("DELETE", "_component_template/*"));
                } catch (ResponseException e) {
                    // We hit a version of ES that doesn't support index templates v2 yet, so it's safe to ignore
                }
            }
        }

        // wipe cluster settings
        if (preserveClusterSettings() == false) {
            wipeClusterSettings();
        }

        if (hasXPack && false == preserveILMPoliciesUponCompletion()) {
            deleteAllILMPolicies(preserveILMPolicyIds());
        }

        if (hasXPack && false == preserveAutoFollowPatternsUponCompletion()) {
            deleteAllAutoFollowPatterns();
        }

        deleteAllNodeShutdownMetadata();

        assertThat("Found in progress snapshots [" + inProgressSnapshots.get() + "].", inProgressSnapshots.get(), anEmptyMap());
    }

    /**
     * If any nodes are registered for shutdown, removes their metadata.
     */
    @SuppressWarnings("unchecked")
    private static void deleteAllNodeShutdownMetadata() throws IOException {
        Request getShutdownStatus = new Request("GET", "_nodes/shutdown");
        Map<String, Object> statusResponse = responseAsMap(adminClient().performRequest(getShutdownStatus));
        if (statusResponse.containsKey("_nodes") && statusResponse.containsKey("cluster_name")) {
            // If the response contains these two keys, the feature flag isn't enabled on this cluster, so skip out now.
            // We can't check the system property directly because it only gets set for the cluster under test's JVM, not for the test
            // runner's JVM.
            return;
        }
            List<Map<String, Object>> nodesArray = (List<Map<String, Object>>) statusResponse.get("nodes");
            List<String> nodeIds = nodesArray.stream()
                .map(nodeShutdownMetadata -> (String) nodeShutdownMetadata.get("node_id"))
                .collect(Collectors.toList());
            for (String nodeId : nodeIds) {
                Request deleteRequest = new Request("DELETE", "_nodes/" + nodeId + "/shutdown");
                assertOK(adminClient().performRequest(deleteRequest));
        }
    }

    protected static void wipeAllIndices() throws IOException {
        boolean includeHidden = minimumNodeVersion().onOrAfter(Version.V_7_7_0);
        try {
            //remove all indices except ilm history which can pop up after deleting all data streams but shouldn't interfere
            final Request deleteRequest = new Request("DELETE", "*,-.ds-ilm-history-*");
            deleteRequest.addParameter("expand_wildcards", "open,closed" + (includeHidden ? ",hidden" : ""));
            RequestOptions.Builder allowSystemIndexAccessWarningOptions = RequestOptions.DEFAULT.toBuilder();
            allowSystemIndexAccessWarningOptions.setWarningsHandler(warnings -> {
                    if (warnings.size() == 0) {
                        return false;
                    } else if (warnings.size() > 1) {
                        return true;
                    }
                    // We don't know exactly which indices we're cleaning up in advance, so just accept all system index access warnings.
                    final String warning = warnings.get(0);
                    final boolean isSystemIndexWarning = warning.contains("this request accesses system indices")
                        && warning.contains("but in a future major version, direct access to system indices will be prevented by default");
                    return isSystemIndexWarning == false;
                });
            deleteRequest.setOptions(allowSystemIndexAccessWarningOptions);
            final Response response = adminClient().performRequest(deleteRequest);
            try (InputStream is = response.getEntity().getContent()) {
                assertTrue((boolean) XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true).get("acknowledged"));
            }
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    protected static void wipeDataStreams() throws IOException {
        try {
            if (hasXPack() && nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_7_9_0))) {
                adminClient().performRequest(new Request("DELETE", "_data_stream/*?expand_wildcards=all"));
            }
        } catch (ResponseException e) {
            // We hit a version of ES that doesn't understand expand_wildcards, try again without it
            try {
                if (hasXPack()) {
                    adminClient().performRequest(new Request("DELETE", "_data_stream/*"));
                }
            } catch (ResponseException ee) {
                // We hit a version of ES that doesn't serialize DeleteDataStreamAction.Request#wildcardExpressionsOriginallySpecified
                //field or that doesn't support data streams so it's safe to ignore
                int statusCode = ee.getResponse().getStatusLine().getStatusCode();
                if (org.elasticsearch.core.Set.of(404, 405, 500).contains(statusCode) == false) {
                    throw ee;
                }
            }
        }
    }

    protected void wipeSearchableSnapshotsIndices() throws IOException {
        // retrieves all indices with a type of store equals to "snapshot"
        final Request request = new Request("GET", "_cluster/state/metadata");
        request.addParameter("filter_path", "metadata.indices.*.settings.index.store.snapshot");

        final Response response = adminClient().performRequest(request);
        @SuppressWarnings("unchecked")
        Map<String, ?> indices = (Map<String, ?>) XContentMapValues.extractValue("metadata.indices", entityAsMap(response));
        if (indices != null) {
            for (String index : indices.keySet()) {
                try {
                    assertAcked("Failed to delete searchable snapshot index [" + index + ']',
                        adminClient().performRequest(new Request("DELETE", index)));
                } catch (ResponseException e) {
                    if (isNotFoundResponseException(e) == false) {
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * Wipe fs snapshots we created one by one and all repositories so that the next test can create the repositories fresh and they'll
     * start empty. There isn't an API to delete all snapshots. There is an API to delete all snapshot repositories but that leaves all of
     * the snapshots intact in the repository.
     * @return Map of repository name to list of snapshots found in unfinished state
     */
    protected Map<String, List<Map<?, ?>>> wipeSnapshots() throws IOException {
        final Map<String, List<Map<?, ?>>> inProgressSnapshots = new HashMap<>();
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
                    if (SnapshotState.valueOf((String) snapshotInfo.get("state")).completed() == false) {
                        inProgressSnapshots.computeIfAbsent(repoName, key -> new ArrayList<>()).add(snapshotInfo);
                    }
                    logger.debug("wiping snapshot [{}/{}]", repoName, name);
                    adminClient().performRequest(new Request("DELETE", "/_snapshot/" + repoName + "/" + name));
                }
            }
            if (preserveReposUponCompletion() == false) {
                deleteRepository(repoName);
            }
        }
        return inProgressSnapshots;
    }

    protected void deleteRepository(String repoName) throws IOException {
        logger.debug("wiping snapshot repository [{}]", repoName);
        adminClient().performRequest(new Request("DELETE", "_snapshot/" + repoName));
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

    private void wipeRollupJobs() throws IOException {
        final Response response;
        try {
            response = adminClient().performRequest(new Request("GET", "/_rollup/job/_all"));
        } catch (ResponseException e) {
            // If we don't see the rollup endpoint (possibly because of running against an older ES version) we just bail
            if (e.getResponse().getStatusLine().getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
                return;
            }
            throw e;
        }
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
            Request request = new Request("POST", "/_rollup/job/" + jobId + "/_stop");
            request.addParameter("ignore", "404");
            request.addParameter("wait_for_completion", "true");
            request.addParameter("timeout", "10s");
            logger.debug("stopping rollup job [{}]", jobId);
            adminClient().performRequest(request);
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            @SuppressWarnings("unchecked")
            String jobId = (String) ((Map<String, Object>) jobConfig.get("config")).get("id");
            Request request = new Request("DELETE", "/_rollup/job/" + jobId);
            request.addParameter("ignore", "404"); // Ignore 404s because they imply someone was racing us to delete this
            logger.debug("deleting rollup job [{}]", jobId);
            adminClient().performRequest(request);
        }
    }

    protected void refreshAllIndices() throws IOException {
        boolean includeHidden = minimumNodeVersion().onOrAfter(Version.V_7_7_0);
        Request refreshRequest = new Request("POST", "/_refresh");
        refreshRequest.addParameter("expand_wildcards", "open" + (includeHidden ? ",hidden" : ""));
        // Allow system index deprecation warnings
        final Builder requestOptions = RequestOptions.DEFAULT.toBuilder();
        requestOptions.setWarningsHandler(warnings -> {
            if (warnings.isEmpty()) {
                return false;
            } else if (warnings.size() > 1) {
                return true;
            } else {
                return warnings.get(0).startsWith("this request accesses system indices:") == false;
            }
        });
        refreshRequest.setOptions(requestOptions);
        client().performRequest(refreshRequest);
    }

    private void waitForPendingRollupTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith("xpack/rollup/job") == false);
    }

    private static void deleteAllILMPolicies(Set<String> exclusions) throws IOException {
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

        policies.keySet().stream()
            .filter(p -> exclusions.contains(p) == false)
            .forEach(policyName -> {
                try {
                    adminClient().performRequest(new Request("DELETE", "/_ilm/policy/" + policyName));
                } catch (IOException e) {
                    throw new RuntimeException("failed to delete policy: " + policyName, e);
                }
            });
    }

    private static void deleteAllSLMPolicies() throws IOException {
        Map<String, Object> policies;

        try {
            Response response = adminClient().performRequest(new Request("GET", "/_slm/policy"));
            policies = entityAsMap(response);
        } catch (ResponseException e) {
            if (RestStatus.METHOD_NOT_ALLOWED.getStatus() == e.getResponse().getStatusLine().getStatusCode()) {
                // If bad request returned, SLM is not enabled.
                return;
            }
            throw e;
        }

        if (policies == null || policies.isEmpty()) {
            return;
        }

        for (String policyName : policies.keySet()) {
            adminClient().performRequest(new Request("DELETE", "/_slm/policy/" + policyName));
        }
    }

    @SuppressWarnings("unchecked")
    private static void deleteAllAutoFollowPatterns() throws IOException {
        final List<Map<?, ?>> patterns;

        try {
            Response response = adminClient().performRequest(new Request("GET", "/_ccr/auto_follow"));
            patterns = (List<Map<?, ?>>) entityAsMap(response).get("patterns");
        } catch (ResponseException e) {
            if (RestStatus.METHOD_NOT_ALLOWED.getStatus() == e.getResponse().getStatusLine().getStatusCode() ||
                RestStatus.BAD_REQUEST.getStatus() == e.getResponse().getStatusLine().getStatusCode()) {
                // If bad request returned, CCR is not enabled.
                return;
            }
            throw e;
        }

        if (patterns == null || patterns.isEmpty()) {
            return;
        }

        for (Map<?, ?> pattern : patterns) {
            String patternName = (String) pattern.get("name");
            adminClient().performRequest(new Request("DELETE", "/_ccr/auto_follow/" + patternName));
        }
    }

    /**
     * Logs a message if there are still running tasks. The reasoning is that any tasks still running are state the is trying to bleed into
     * other tests.
     */
    private void logIfThereAreRunningTasks() throws IOException {
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
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    protected static void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        String truststorePath = settings.get(TRUSTSTORE_PATH);
        String certificateAuthorities = settings.get(CERTIFICATE_AUTHORITIES);
        String clientCertificatePath = settings.get(CLIENT_CERT_PATH);

        if (certificateAuthorities != null && truststorePath != null) {
            throw new IllegalStateException("Cannot set both " + CERTIFICATE_AUTHORITIES + " and " + TRUSTSTORE_PATH
                + ". Please configure one of these.");

        }
        if (truststorePath != null) {
            if (inFipsJvm()) {
                throw new IllegalStateException("Keystore " + truststorePath + "cannot be used in FIPS 140 mode. Please configure "
                    + CERTIFICATE_AUTHORITIES + " with a PEM encoded trusted CA/certificate instead");
            }
            final String keystorePass = settings.get(TRUSTSTORE_PASSWORD);
            if (keystorePass == null) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is provided but not " + TRUSTSTORE_PASSWORD);
            }
            Path path = PathUtils.get(truststorePath);
            if (Files.exists(path) == false) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is set but points to a non-existing file");
            }
            try {
                final String keyStoreType = truststorePath.endsWith(".p12") ? "PKCS12" : "jks";
                KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                try (InputStream is = Files.newInputStream(path)) {
                    keyStore.load(is, keystorePass.toCharArray());
                }
                SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
                SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslcontext);
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy));
            } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException | CertificateException e) {
                throw new RuntimeException("Error setting up ssl", e);
            }
        }
        if (certificateAuthorities != null) {
            Path caPath = PathUtils.get(certificateAuthorities);
            if (Files.exists(caPath) == false) {
                throw new IllegalStateException(CERTIFICATE_AUTHORITIES + " is set but points to a non-existing file");
            }
            try {
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                keyStore.load(null, null);
                Certificate caCert = PemUtils.readCertificates(Collections.singletonList(caPath)).get(0);
                keyStore.setCertificateEntry(caCert.toString(), caCert);
                final SSLContextBuilder sslContextBuilder = SSLContexts.custom();
                if (clientCertificatePath != null) {
                    final Path certPath = PathUtils.get(clientCertificatePath);
                    final Path keyPath = PathUtils.get(Objects.requireNonNull(settings.get(CLIENT_KEY_PATH), "No key provided"));
                    final String password = settings.get(CLIENT_KEY_PASSWORD);
                    final char[] passwordChars = password == null ? null : password.toCharArray();
                    final PrivateKey key = PemUtils.readPrivateKey(keyPath, () -> passwordChars);
                    final Certificate[] clientCertChain
                        = PemUtils.readCertificates(Collections.singletonList(certPath)).toArray(new Certificate[1]);
                    keyStore.setKeyEntry("client", key, passwordChars, clientCertChain);
                    sslContextBuilder.loadKeyMaterial(keyStore, passwordChars);
                }
                SSLContext sslcontext = sslContextBuilder.loadTrustMaterial(keyStore, null).build();
                SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslcontext);
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy));
            } catch (GeneralSecurityException e) {
                throw new RuntimeException("Error setting up ssl", e);
            }
        } else if (clientCertificatePath != null) {
            throw new IllegalStateException("Client certificates are currently only supported when using a custom CA");
        }
        Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout =
            TimeValue.parseTimeValue(socketTimeoutString == null ? "60s" : socketTimeoutString, CLIENT_SOCKET_TIMEOUT);
        builder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
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

    public static void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    /**
     * Permits subclasses to increase the default timeout when waiting for green health
     */
    @Nullable
    protected String getEnsureGreenTimeout() {
        return null;
    }

    /**
     * checks that the specific index is green. we force a selection of an index as the tests share a cluster and often leave indices
     * in an non green state
     * @param index index to test for
     **/
    public final void ensureGreen(String index) throws IOException {
        ensureHealth(index, (request) -> {
            request.addParameter("wait_for_status", "green");
            request.addParameter("wait_for_no_relocating_shards", "true");
            final String ensureGreenTimeout = getEnsureGreenTimeout();
            if (ensureGreenTimeout != null) {
                request.addParameter("timeout", ensureGreenTimeout);
            }
            request.addParameter("level", "shards");
        });
    }

    protected static void ensureHealth(Consumer<Request> requestConsumer) throws IOException {
        ensureHealth("", requestConsumer);
    }

    public static void ensureHealth(String index, Consumer<Request> requestConsumer) throws IOException {
        ensureHealth(client(), index, requestConsumer);
    }

    protected static void ensureHealth(RestClient client, String index, Consumer<Request> requestConsumer) throws IOException {
        Request request = new Request("GET", "/_cluster/health" + (index.trim().isEmpty() ? "" : "/" + index));
        requestConsumer.accept(request);
        try {
            client.performRequest(request);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_REQUEST_TIMEOUT) {
                try {
                    final Response clusterStateResponse = client.performRequest(new Request("GET", "/_cluster/state?pretty"));
                    fail("timed out waiting for green state for index [" + index + "] " +
                        "cluster state [" + EntityUtils.toString(clusterStateResponse.getEntity()) + "]");
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
            }
            throw e;
        }
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
        adminClient().performRequest(request);
    }

    protected static void createIndex(String name, Settings settings) throws IOException {
        createIndex(name, settings, null);
    }

    protected static void createIndex(String name, Settings settings, String mapping) throws IOException {
        createIndex(name, settings, mapping, null);
    }

    protected static void createIndex(String name, Settings settings, String mapping, String aliases) throws IOException {
        Request request = new Request("PUT", "/" + name);
        String entity = "{\"settings\": " + Strings.toString(settings);
        if (mapping != null) {
            entity += ",\"mappings\" : {" + mapping + "}";
        }
        if (aliases != null) {
            entity += ",\"aliases\": {" + aliases + "}";
        }
        entity += "}";
        if (settings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) == false) {
            expectSoftDeletesWarning(request, name);
        } else if (settings.hasValue(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey()) ||
            settings.hasValue(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey())) {
            expectTranslogRetentionWarning(request);
        }
        request.setJsonEntity(entity);
        client().performRequest(request);
    }

    protected static void deleteIndex(String name) throws IOException {
        deleteIndex(client(), name);
    }

    protected static void deleteIndex(RestClient client, String name) throws IOException {
        Request request = new Request("DELETE", "/" + name);
        client.performRequest(request);
    }

    protected static void updateIndexSettings(String index, Settings.Builder settings) throws IOException {
        updateIndexSettings(index, settings.build());
    }

    private static void updateIndexSettings(String index, Settings settings) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_settings");
        request.setJsonEntity(Strings.toString(settings));
        client().performRequest(request);
    }

    protected static void expectSoftDeletesWarning(Request request, String indexName) {
        final List<String> expectedWarnings = Collections.singletonList(
            "Creating indices with soft-deletes disabled is deprecated and will be removed in future Elasticsearch versions. " +
            "Please do not specify value for setting [index.soft_deletes.enabled] of index [" + indexName + "].");
        final Builder requestOptions = RequestOptions.DEFAULT.toBuilder();
        if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_7_6_0))) {
            requestOptions.setWarningsHandler(warnings -> warnings.equals(expectedWarnings) == false);
            request.setOptions(requestOptions);
        } else if (nodeVersions.stream().anyMatch(version -> version.onOrAfter(Version.V_7_6_0))) {
            requestOptions.setWarningsHandler(warnings -> warnings.isEmpty() == false && warnings.equals(expectedWarnings) == false);
            request.setOptions(requestOptions);
        }
    }

    protected static void expectTranslogRetentionWarning(Request request) {
        final List<String> expectedWarnings = Collections.singletonList(
            "Translog retention settings [index.translog.retention.age] "
                + "and [index.translog.retention.size] are deprecated and effectively ignored. They will be removed in a future version.");
        final Builder requestOptions = RequestOptions.DEFAULT.toBuilder();
        if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_7_7_0))) {
            requestOptions.setWarningsHandler(warnings -> warnings.equals(expectedWarnings) == false);
            request.setOptions(requestOptions);
        } else if (nodeVersions.stream().anyMatch(version -> version.onOrAfter(Version.V_7_7_0))) {
            requestOptions.setWarningsHandler(warnings -> warnings.isEmpty() == false && warnings.equals(expectedWarnings) == false);
            request.setOptions(requestOptions);
        }
    }

    protected static Map<String, Object> getIndexSettings(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        Response response = client().performRequest(request);
        try (InputStream is = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getIndexSettingsAsMap(String index) throws IOException {
        Map<String, Object> indexSettings = getIndexSettings(index);
        return (Map<String, Object>)((Map<String, Object>) indexSettings.get(index)).get("settings");
    }

    protected static boolean indexExists(String index) throws IOException {
        final Request getRequest = new Request("HEAD", "/" + index);
        getRequest.setOptions(expectVersionSpecificWarnings(v -> {
            final String typesWarning = "[types removal] The parameter include_type_name should be explicitly specified in get indices " +
                "requests to prepare for 7.0. In 7.0 include_type_name will default to 'false', which means responses will omit the type " +
                "name in mapping definitions.";
            v.compatible(typesWarning);
        }));
        Response response = client().performRequest(getRequest);
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    protected static void closeIndex(String index) throws IOException {
        final Request closeRequest = new Request(HttpPost.METHOD_NAME, "/" + index + "/_close");
        closeRequest.setOptions(expectVersionSpecificWarnings(v -> {
            v.current(WAIT_FOR_ACTIVE_SHARDS_DEFAULT_DEPRECATION_MESSAGE);
            v.compatible(WAIT_FOR_ACTIVE_SHARDS_DEFAULT_DEPRECATION_MESSAGE);
        }));
        assertOK(client().performRequest(closeRequest));
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
        return responseAsMap(response);
    }

    protected static Map<String, Object> responseAsMap(Response response) throws IOException {
        XContentType entityContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        Map<String, Object> responseEntity = XContentHelper.convertToMap(entityContentType.xContent(),
                response.getEntity().getContent(), false);
        assertNotNull(responseEntity);
        return responseEntity;
    }

    protected static void registerRepository(String repository, String type, boolean verify, Settings settings) throws IOException {
        registerRepository(client(), repository, type, verify, settings);
    }

    protected static void registerRepository(
        RestClient client,
        String repository,
        String type,
        boolean verify,
        Settings settings
    ) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository);
        request.addParameter("verify", Boolean.toString(verify));
        request.setJsonEntity(Strings.toString(new PutRepositoryRequest(repository).type(type).settings(settings)));

        final Response response = client.performRequest(request);
        assertAcked("Failed to create repository [" + repository + "] of type [" + type + "]: " + response, response);
    }

    protected static void createSnapshot(String repository, String snapshot, boolean waitForCompletion) throws IOException {
        createSnapshot(client(), repository, snapshot, waitForCompletion);
    }

    protected static void createSnapshot(
        RestClient client,
        String repository,
        String snapshot,
        boolean waitForCompletion
    ) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        final Response response = client.performRequest(request);
        assertThat(
            "Failed to create snapshot [" + snapshot + "] in repository [" + repository + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    protected static void restoreSnapshot(String repository, String snapshot, boolean waitForCompletion) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot + "/_restore");
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        final Response response = client().performRequest(request);
        assertThat(
            "Failed to restore snapshot [" + snapshot + "] from repository [" + repository + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    protected static void deleteSnapshot(String repository, String snapshot, boolean ignoreMissing) throws IOException {
        deleteSnapshot(client(), repository, snapshot, ignoreMissing);
    }

    protected static void deleteSnapshot(RestClient client, String repository, String snapshot, boolean ignoreMissing) throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        if (ignoreMissing) {
            request.addParameter("ignore", "404");
        }
        final Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(),  ignoreMissing ? anyOf(equalTo(200), equalTo(404)) : equalTo(200));
    }

    @SuppressWarnings("unchecked")
    private static void assertAcked(String message, Response response) throws IOException {
        final int responseStatusCode = response.getStatusLine().getStatusCode();
        assertThat(
            message + ": expecting response code [200] but got [" + responseStatusCode + ']',
            responseStatusCode,
            equalTo(RestStatus.OK.getStatus())
        );
        final Map<String, Object> responseAsMap = responseAsMap(response);
        Boolean acknowledged = (Boolean) XContentMapValues.extractValue(responseAsMap, "acknowledged");
        assertThat(message + ": response is not acknowledged", acknowledged, equalTo(Boolean.TRUE));
    }

    /**
     * Is this template one that is automatically created by xpack?
     */
    protected static boolean isXPackTemplate(String name) {
        if (name.startsWith(".monitoring-")) {
            return true;
        }
        if (name.startsWith(".watch") || name.startsWith(".triggered_watches")) {
            return true;
        }
        if (name.startsWith(".data-frame-")) {
            return true;
        }
        if (name.startsWith(".ml-")) {
            return true;
        }
        if (name.startsWith(".transform-")) {
            return true;
        }
        if (name.startsWith(".deprecation-")) {
            return true;
        }
        switch (name) {
            case ".watches":
            case "security_audit_log":
            case ".slm-history":
            case ".async-search":
            case "saml-service-provider":
            case "logs":
            case "logs-settings":
            case "logs-mappings":
            case "metrics":
            case "metrics-settings":
            case "metrics-mappings":
            case "synthetics":
            case "synthetics-settings":
            case "synthetics-mappings":
            case ".snapshot-blob-cache":
            case "ilm-history":
            case "logstash-index-template":
            case "security-index-template":
            case "data-streams-mappings":
                return true;
            default:
                return false;
        }
    }

    public void flush(String index, boolean force) throws IOException {
        logger.info("flushing index {} force={}", index, force);
        final Request flushRequest = new Request("POST", "/" + index + "/_flush");
        flushRequest.addParameter("force", Boolean.toString(force));
        flushRequest.addParameter("wait_if_ongoing", "true");
        assertOK(client().performRequest(flushRequest));
    }

    /**
     * Asserts that replicas on nodes satisfying the {@code targetNode} should have perform operation-based recoveries.
     */
    public void assertNoFileBasedRecovery(String indexName, Predicate<String> targetNode) throws IOException {
        Map<String, Object> recoveries = entityAsMap(client().performRequest(new Request("GET", indexName + "/_recovery?detailed=true")));
        @SuppressWarnings("unchecked")
        List<Map<String, ?>> shards = (List<Map<String, ?>>) XContentMapValues.extractValue(indexName + ".shards", recoveries);
        assertNotNull(shards);
        boolean foundReplica = false;
        logger.info("index {} recovery stats {}", indexName, shards);
        for (Map<String, ?> shard : shards) {
            if (shard.get("primary") == Boolean.FALSE && targetNode.test((String) XContentMapValues.extractValue("target.name", shard))) {
                List<?> details = (List<?>) XContentMapValues.extractValue("index.files.details", shard);
                // once detailed recoveries works, remove this if.
                if (details == null) {
                    long totalFiles = ((Number) XContentMapValues.extractValue("index.files.total", shard)).longValue();
                    long reusedFiles = ((Number) XContentMapValues.extractValue("index.files.reused", shard)).longValue();
                    logger.info("total [{}] reused [{}]", totalFiles, reusedFiles);
                    assertThat("must reuse all files, recoveries [" + recoveries + "]", totalFiles, equalTo(reusedFiles));
                } else {
                    assertNotNull(details);
                    assertThat(details, Matchers.empty());
                }
                foundReplica = true;
            }
        }
        assertTrue("must find replica", foundReplica);
    }

    /**
     * Asserts that we do not retain any extra translog for the given index (i.e., turn off the translog retention)
     */
    public void assertEmptyTranslog(String index) throws Exception {
        Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
        assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.uncommitted_operations", stats), equalTo(0));
        assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.operations", stats), equalTo(0));
    }

    /**
     * Peer recovery retention leases are renewed and synced to replicas periodically (every 30 seconds). This ensures
     * that we have renewed every PRRL to the global checkpoint of the corresponding copy and properly synced to all copies.
     */
    public void ensurePeerRecoveryRetentionLeasesRenewedAndSynced(String index) throws Exception {
        final boolean alwaysExists = minimumNodeVersion().onOrAfter(Version.V_7_6_0);
        assertBusy(() -> {
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            @SuppressWarnings("unchecked") Map<String, List<Map<String, ?>>> shards =
                (Map<String, List<Map<String, ?>>>) XContentMapValues.extractValue("indices." + index + ".shards", stats);
            for (List<Map<String, ?>> shard : shards.values()) {
                for (Map<String, ?> copy : shard) {
                    Integer globalCheckpoint = (Integer) XContentMapValues.extractValue("seq_no.global_checkpoint", copy);
                    assertNotNull(globalCheckpoint);
                    assertThat(XContentMapValues.extractValue("seq_no.max_seq_no", copy), equalTo(globalCheckpoint));
                    @SuppressWarnings("unchecked") List<Map<String, ?>> retentionLeases =
                        (List<Map<String, ?>>) XContentMapValues.extractValue("retention_leases.leases", copy);
                    if (alwaysExists == false && retentionLeases == null) {
                        continue;
                    }
                    assertNotNull(retentionLeases);
                    for (Map<String, ?> retentionLease : retentionLeases) {
                        if (((String) retentionLease.get("id")).startsWith("peer_recovery/")) {
                            assertThat(retentionLease.get("retaining_seq_no"), equalTo(globalCheckpoint + 1));
                        }
                    }
                    if (alwaysExists) {
                        List<String> existingLeaseIds = retentionLeases.stream().map(lease -> (String) lease.get("id"))
                            .collect(Collectors.toList());
                        List<String> expectedLeaseIds = shard.stream()
                            .map(shr -> (String) XContentMapValues.extractValue("routing.node", shr))
                            .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId)
                            .collect(Collectors.toList());
                        assertThat("not every active copy has established its PPRL", expectedLeaseIds, everyItem(in(existingLeaseIds)));
                    }
                }
            }
        }, 60, TimeUnit.SECONDS);
    }

    public static Boolean getHasXPack() {
        return hasXPack;
    }

    /**
     * Returns the minimum node version among all nodes of the cluster
     */
    protected static Version minimumNodeVersion() throws IOException {
        final Request request = new Request("GET", "_nodes");
        request.addParameter("filter_path", "nodes.*.version");

        final Response response = adminClient().performRequest(request);
        final Map<String, Object> nodes = ObjectPath.createFromResponse(response).evaluate("nodes");

        Version minVersion = null;
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            @SuppressWarnings("unchecked")
            Version nodeVersion = Version.fromString((String) ((Map<String, Object>) node.getValue()).get("version"));
            if (minVersion == null || minVersion.after(nodeVersion)) {
                minVersion = nodeVersion;
            }
        }
        assertNotNull(minVersion);
        return minVersion;
    }

    protected static void performSyncedFlush(String indexName, boolean retryOnConflict) throws Exception {
        final Request request = new Request("POST", indexName + "/_flush/synced");
        final List<String> expectedWarnings = Collections.singletonList(SyncedFlushService.SYNCED_FLUSH_DEPRECATION_MESSAGE);
        // prior to v7.11, the deprecation message for synced flush was incorrect so we also need to handle that
        final List<String> incorrectWarningMessage = Collections.singletonList("Synced flush is deprecated and will be removed in 8.0." +
            " Use flush at _/flush or /{index}/_flush instead.");
        final Builder options = RequestOptions.DEFAULT.toBuilder();
        if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_7_11_0))) {
            options.setWarningsHandler(warnings -> warnings.equals(expectedWarnings) == false);
        } else if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_7_6_0))) {
            options.setWarningsHandler(warnings -> warnings.equals(expectedWarnings) == false &&
                warnings.equals(incorrectWarningMessage) == false);
        } else if (nodeVersions.stream().anyMatch(version -> version.onOrAfter(Version.V_7_6_0))) {
            options.setWarningsHandler(warnings -> warnings.isEmpty() == false &&
                warnings.equals(expectedWarnings) == false && warnings.equals(incorrectWarningMessage) == false);
        }
        request.setOptions(options);
        // We have to spin a synced-flush request because we fire the global checkpoint sync for the last write operation.
        // A synced-flush request considers the global checkpoint sync as an going operation because it acquires a shard permit.
        assertBusy(() -> {
            try {
                Response resp = client().performRequest(request);
                if (retryOnConflict) {
                    Map<String, Object> result = ObjectPath.createFromResponse(resp).evaluate("_shards");
                    assertThat(result.get("failed"), equalTo(0));
                }
            } catch (ResponseException ex) {
                assertThat(ex.getResponse().getStatusLine(), equalTo(HttpStatus.SC_CONFLICT));
                if (retryOnConflict) {
                    throw new AssertionError(ex); // cause assert busy to retry
                }
            }
        });
    }

    /**
     * Wait for the license to be applied and active. The specified admin client is used to check the license and this is done using
     * {@link ESTestCase#assertBusy(CheckedRunnable)} to give some time to the License to be applied on nodes.
     *
     * @param restClient the client to use
     * @throws Exception if an exception is thrown while checking the status of the license
     */
    protected static void waitForActiveLicense(final RestClient restClient) throws Exception {
        assertBusy(() -> {
            final Request request = new Request(HttpGet.METHOD_NAME, "/_xpack");
            request.setOptions(RequestOptions.DEFAULT.toBuilder());

            final Response response = restClient.performRequest(request);
            assertOK(response);

            try (InputStream is = response.getEntity().getContent()) {
                XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
                final Map<String, ?> map = XContentHelper.convertToMap(xContentType.xContent(), is, true);
                assertThat(map, notNullValue());
                assertThat("License must exist", map.containsKey("license"), equalTo(true));
                @SuppressWarnings("unchecked")
                final Map<String, ?> license = (Map<String, ?>) map.get("license");
                assertThat("Expecting non-null license", license, notNullValue());
                assertThat("License status must exist", license.containsKey("status"), equalTo(true));
                final String status = (String) license.get("status");
                assertThat("Expecting non-null license status", status, notNullValue());
                assertThat("Expecting active license", status, equalTo("active"));
            }
        });
    }

    //TODO: replace usages of this with warning_regex or allowed_warnings_regex
    static final Pattern CREATE_INDEX_MULTIPLE_MATCHING_TEMPLATES = Pattern.compile("^index \\[(.+)\\] matches multiple legacy " +
        "templates \\[(.+)\\], composable templates will only match a single template$");

    static final Pattern PUT_TEMPLATE_MULTIPLE_MATCHING_TEMPLATES = Pattern.compile("^index template \\[(.+)\\] has index patterns " +
        "\\[(.+)\\] matching patterns from existing older templates \\[(.+)\\] with patterns \\((.+)\\); this template \\[(.+)\\] will " +
        "take precedence during new index creation$");

    protected static void useIgnoreMultipleMatchingTemplatesWarningsHandler(Request request) throws IOException {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(warnings -> {
            if (warnings.size() > 0) {
                boolean matches = warnings.stream().anyMatch(
                    message -> CREATE_INDEX_MULTIPLE_MATCHING_TEMPLATES.matcher(message).matches() ||
                    PUT_TEMPLATE_MULTIPLE_MATCHING_TEMPLATES.matcher(message).matches());
                return matches == false;
            } else {
                return false;
            }
        });
        request.setOptions(options);
    }

    protected static boolean isNotFoundResponseException(IOException ioe) {
        if (ioe instanceof ResponseException) {
            Response response = ((ResponseException) ioe).getResponse();
            return response.getStatusLine().getStatusCode() == 404;
        }
        return false;
    }
}
