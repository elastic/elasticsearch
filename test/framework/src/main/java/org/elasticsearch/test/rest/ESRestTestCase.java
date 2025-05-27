/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import io.netty.handler.codec.http.HttpMethod;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.FieldCapsUtils;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RequestOptions.Builder;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractBroadcastResponseTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

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
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.client.RestClient.IGNORE_RESPONSE_CODES_PARAM;
import static org.elasticsearch.cluster.ClusterState.VERSION_INTRODUCING_TRANSPORT_VERSIONS;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_ACTION_NAME;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_ACTION_NAME;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.test.rest.TestFeatureService.ALL_FEATURES;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
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

    private static final Pattern SEMANTIC_VERSION_PATTERN = Pattern.compile("^(\\d+\\.\\d+\\.\\d+)\\D?.*");

    private static final Logger SUITE_LOGGER = LogManager.getLogger(ESRestTestCase.class);

    private static final String EXPECTED_ROLLUP_WARNING_MESSAGE =
        "The rollup functionality will be removed in Elasticsearch 10.0. See docs for more information.";
    public static final RequestOptions.Builder ROLLUP_REQUESTS_OPTIONS = RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> {
        if (warnings.isEmpty()) {
            return false;
        } else {
            // Sometimes multiple rollup deprecation warnings. Transport actions can be invoked multiple time on different nodes.
            for (String warning : warnings) {
                if (EXPECTED_ROLLUP_WARNING_MESSAGE.equals(warning) == false) {
                    return true;
                }
            }
            return false;
        }
    });

    /**
     * Convert the entity from a {@link Response} into a map of maps.
     * Consumes the underlying HttpEntity, releasing any resources it may be holding.
     */
    public static Map<String, Object> entityAsMap(Response response) throws IOException {
        return entityAsMap(response.getEntity());
    }

    /**
     * Convert the entity from a {@link Response} into a map of maps.
     * Consumes the underlying HttpEntity, releasing any resources it may be holding.
     */
    public static Map<String, Object> entityAsMap(HttpEntity entity) throws IOException {
        XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (
            XContentParser parser = xContentType.xContent()
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(NamedXContentRegistry.EMPTY)
                        .withDeprecationHandler(DeprecationHandler.THROW_UNSUPPORTED_OPERATION),
                    entity.getContent()
                )
        ) {
            return parser.map();
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
    }

    /**
     * Convert the entity from a {@link Response} into a list of maps.
     * Consumes the underlying HttpEntity, releasing any resources it may be holding.
     */
    public static List<Object> entityAsList(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (
            XContentParser parser = xContentType.xContent()
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(NamedXContentRegistry.EMPTY)
                        .withDeprecationHandler(DeprecationHandler.THROW_UNSUPPORTED_OPERATION),
                    response.getEntity().getContent()
                )
        ) {
            return parser.list();
        } finally {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    /**
     * Does any node in the cluster being tested have x-pack installed?
     */
    public static boolean hasXPack() {
        if (availableFeatures == null) {
            throw new IllegalStateException("must be called inside of a rest test case test");
        }
        return availableFeatures.contains(ProductFeature.XPACK);
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
    /**
     * A client for the running Elasticsearch cluster configured to clean up the cluster after tests
     */
    private static RestClient cleanupClient;

    private static boolean multiProjectEnabled;
    private static String activeProject;
    private static Set<String> extraProjects;
    private static boolean projectsConfigured = false;

    public enum ProductFeature {
        XPACK,
        ILM,
        SLM,
        ROLLUPS,
        CCR,
        SHUTDOWN,
        LEGACY_TEMPLATES,
        SEARCHABLE_SNAPSHOTS
    }

    private static EnumSet<ProductFeature> availableFeatures;
    private static Set<String> nodesVersions;

    protected static TestFeatureService testFeatureService = ALL_FEATURES;

    protected static Set<String> getCachedNodesVersions() {
        assert nodesVersions != null;
        return nodesVersions;
    }

    protected static Set<String> readVersionsFromNodesInfo(RestClient adminClient) throws IOException {
        return getNodesInfo(adminClient).values().stream().map(nodeInfo -> nodeInfo.get("version").toString()).collect(Collectors.toSet());
    }

    protected static Map<String, Map<?, ?>> getNodesInfo(RestClient adminClient) throws IOException {
        Map<?, ?> response = entityAsMap(adminClient.performRequest(new Request("GET", "_nodes/plugins")));
        Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");

        return nodes.entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(entry -> entry.getKey().toString(), entry -> (Map<?, ?>) entry.getValue()));
    }

    /**
     * Does the cluster being tested support the set of capabilities
     * for specified path and method.
     */
    protected static Optional<Boolean> clusterHasCapability(
        String method,
        String path,
        Collection<String> parameters,
        Collection<String> capabilities
    ) throws IOException {
        return clusterHasCapability(adminClient, method, path, parameters, capabilities);
    }

    /**
     * Does the cluster on the other side of {@code client} support the set
     * of capabilities for specified path and method.
     */
    protected static Optional<Boolean> clusterHasCapability(
        RestClient client,
        String method,
        String path,
        Collection<String> parameters,
        Collection<String> capabilities
    ) throws IOException {
        Request request = new Request("GET", "_capabilities");
        request.addParameter("method", method);
        request.addParameter("path", path);
        if (parameters.isEmpty() == false) {
            request.addParameter("parameters", String.join(",", parameters));
        }
        if (capabilities.isEmpty() == false) {
            request.addParameter("capabilities", String.join(",", capabilities));
        }
        try {
            Map<String, Object> response = entityAsMap(client.performRequest(request).getEntity());
            return Optional.ofNullable((Boolean) response.get("supported"));
        } catch (ResponseException responseException) {
            if (responseException.getResponse().getStatusLine().getStatusCode() / 100 == 4) {
                return Optional.empty(); // we don't know, the capabilities API is unsupported
            }
            throw responseException;
        }
    }

    protected static boolean clusterHasFeature(String featureId) {
        return testFeatureService.clusterHasFeature(featureId, false);
    }

    protected static boolean clusterHasFeature(NodeFeature feature) {
        return testFeatureService.clusterHasFeature(feature.id(), false);
    }

    protected static boolean testFeatureServiceInitialized() {
        return testFeatureService != ALL_FEATURES;
    }

    @BeforeClass
    public static void initializeProjectIds() {
        // The active project-id is slightly longer, and has a fixed prefix so that it's easier to pick in error messages etc.
        activeProject = "active00" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        extraProjects = randomSet(1, 3, () -> randomAlphaOfLength(12).toLowerCase(Locale.ROOT));
        multiProjectEnabled = Boolean.parseBoolean(System.getProperty("tests.multi_project.enabled"));
    }

    @Before
    public void initClient() throws IOException {
        if (client == null) {
            assert adminClient == null;
            assert clusterHosts == null;
            assert availableFeatures == null;
            assert nodesVersions == null;
            assert testFeatureServiceInitialized() == false;
            clusterHosts = parseClusterHosts(getTestRestCluster());
            logger.info("initializing REST clients against {}", clusterHosts);
            // We add the project ID to the client settings afterward because a lot of subclasses don't call super.restClientSettings(),
            // meaning the project ID would be removed from the settings.
            var clientSettings = addProjectIdToSettings(restClientSettings());
            var adminSettings = restAdminSettings();
            var cleanupSettings = cleanupClientSettings();
            var hosts = clusterHosts.toArray(new HttpHost[0]);
            client = buildClient(clientSettings, hosts);
            adminClient = clientSettings.equals(adminSettings) ? client : buildClient(adminSettings, hosts);
            cleanupClient = adminSettings.equals(cleanupSettings) ? adminClient : buildClient(cleanupSettings, hosts);

            availableFeatures = EnumSet.of(ProductFeature.LEGACY_TEMPLATES);
            Set<String> versions = new HashSet<>();
            boolean serverless = false;

            for (Map<?, ?> nodeInfo : getNodesInfo(adminClient).values()) {
                var nodeVersion = nodeInfo.get("version").toString();
                versions.add(nodeVersion);
                for (Object module : (List<?>) nodeInfo.get("modules")) {
                    Map<?, ?> moduleInfo = (Map<?, ?>) module;
                    final String moduleName = moduleInfo.get("name").toString();
                    if (moduleName.startsWith("x-pack")) {
                        availableFeatures.add(ProductFeature.XPACK);
                    }
                    if (moduleName.equals("x-pack-ilm")) {
                        availableFeatures.add(ProductFeature.ILM);
                        availableFeatures.add(ProductFeature.SLM);
                    }
                    if (moduleName.equals("x-pack-rollup")) {
                        availableFeatures.add(ProductFeature.ROLLUPS);
                    }
                    if (moduleName.equals("x-pack-ccr")) {
                        availableFeatures.add(ProductFeature.CCR);
                    }
                    if (moduleName.equals("x-pack-shutdown")) {
                        availableFeatures.add(ProductFeature.SHUTDOWN);
                    }
                    if (moduleName.equals("searchable-snapshots")) {
                        availableFeatures.add(ProductFeature.SEARCHABLE_SNAPSHOTS);
                    }
                    if (moduleName.startsWith("serverless-")) {
                        serverless = true;
                    }
                }
                if (serverless) {
                    availableFeatures.removeAll(
                        List.of(
                            ProductFeature.ILM,
                            ProductFeature.SLM,
                            ProductFeature.ROLLUPS,
                            ProductFeature.CCR,
                            ProductFeature.LEGACY_TEMPLATES
                        )
                    );
                }
            }
            nodesVersions = Collections.unmodifiableSet(versions);

            var semanticNodeVersions = nodesVersions.stream()
                .map(ESRestTestCase::parseLegacyVersion)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
            assert semanticNodeVersions.isEmpty() == false || serverless;
            testFeatureService = createTestFeatureService(getClusterStateFeatures(adminClient), semanticNodeVersions);

            configureProjects();
        }

        assert testFeatureServiceInitialized();
        assert client != null;
        assert adminClient != null;
        assert clusterHosts != null;
        assert availableFeatures != null;
        assert nodesVersions != null;
    }

    protected final TestFeatureService createTestFeatureService(
        Map<String, Set<String>> clusterStateFeatures,
        Set<Version> semanticNodeVersions
    ) {
        return new ESRestTestFeatureService(semanticNodeVersions, clusterStateFeatures.values());
    }

    protected static boolean has(ProductFeature feature) {
        return availableFeatures.contains(feature);
    }

    protected List<HttpHost> parseClusterHosts(String hostsString) {
        String[] stringUrls = hostsString.split(",");
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
        return unmodifiableList(hosts);
    }

    protected String getTestRestCluster() {
        String cluster = System.getProperty("tests.rest.cluster");
        if (cluster == null) {
            throw new RuntimeException(
                "Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] "
                    + "to which to send REST requests"
            );
        }

        return cluster;
    }

    protected String getTestReadinessPorts() {
        String ports = System.getProperty("tests.cluster.readiness");
        if (ports == null) {
            throw new RuntimeException(
                "Must specify [tests.rest.cluster.readiness] system property with a comma delimited list "
                    + "to which to send readiness requests"
            );
        }

        return ports;
    }

    /**
     * Helper class to check warnings in REST responses with sensitivity to versions
     * used in the target cluster.
     */
    public static class VersionSensitiveWarningsHandler implements WarningsHandler {
        Set<String> requiredSameVersionClusterWarnings = new HashSet<>();
        Set<String> allowedWarnings = new HashSet<>();
        private final Set<String> testNodeVersions;

        VersionSensitiveWarningsHandler(Set<String> nodeVersions) {
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
         * @param allowedWarningsToAdd optional warnings that will be ignored if received
         */
        public void compatible(String... allowedWarningsToAdd) {
            this.allowedWarnings.addAll(Arrays.asList(allowedWarningsToAdd));
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
                    if (false == allowedWarnings.contains(actualWarning)
                        && false == requiredSameVersionClusterWarnings.contains(actualWarning)) {
                        return true;
                    }
                }
                return false;
            }
        }

        private boolean isExclusivelyTargetingCurrentVersionCluster() {
            assertFalse("Node versions running in the cluster are missing", testNodeVersions.isEmpty());
            return testNodeVersions.size() == 1 && testNodeVersions.iterator().next().equals(Build.current().version());
        }

    }

    public static RequestOptions expectVersionSpecificWarnings(Consumer<VersionSensitiveWarningsHandler> expectationsSetter) {
        Builder builder = RequestOptions.DEFAULT.toBuilder();
        VersionSensitiveWarningsHandler warningsHandler = new VersionSensitiveWarningsHandler(getCachedNodesVersions());
        expectationsSetter.accept(warningsHandler);
        builder.setWarningsHandler(warningsHandler);
        return builder.build();
    }

    /**
     * Creates request options designed to be used when making a call that can return warnings, for example a
     * deprecated request. The options will ensure that the given warnings are returned if all nodes are on
     * the current version and will allow (but not require) the warnings if any node is running an older version.
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
            ensureNoInitializingShards();
            wipeCluster();
            waitForClusterStateUpdatesToFinish();
            checkForUnexpectedlyRecreatedObjects();
            logIfThereAreRunningTasks();
        }
    }

    @AfterClass
    public static void closeClients() throws IOException {
        try {
            IOUtils.close(client, adminClient, cleanupClient);
        } finally {
            clusterHosts = null;
            client = null;
            adminClient = null;
            cleanupClient = null;
            availableFeatures = null;
            nodesVersions = null;
            testFeatureService = ALL_FEATURES;
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
     * Get the client used for cleaning up the cluster.
     */
    protected static RestClient cleanupClient() {
        return cleanupClient;
    }

    /**
     * Wait for outstanding tasks to complete. The specified admin client is used to check the outstanding tasks and this is done using
     * {@link ESTestCase#assertBusy(CheckedRunnable)} to give a chance to any outstanding tasks to complete.
     *
     * @param restClient the admin client
     * @throws Exception if an exception is thrown while checking the outstanding tasks
     */
    public static void waitForPendingTasks(final RestClient restClient) throws Exception {
        waitForPendingTasks(restClient, taskName -> false);
    }

    /**
     * Wait for outstanding tasks to complete. The specified admin client is used to check the outstanding tasks and this is done using
     * {@link ESTestCase#assertBusy(CheckedRunnable)} to give a chance to any outstanding tasks to complete. The specified filter is used
     * to filter out outstanding tasks that are expected to be there. In addition to the expected tasks that are defined by the filter we
     * expect the list task to be there since it is created by the call and the health node task which is always running on the background.
     *
     * @param restClient the admin client
     * @param taskFilter  predicate used to filter tasks that are expected to be there
     * @throws Exception if an exception is thrown while checking the outstanding tasks
     */
    public static void waitForPendingTasks(final RestClient restClient, final Predicate<String> taskFilter) throws Exception {
        assertBusy(() -> {
            try {
                final Request request = new Request("GET", "/_cat/tasks");
                request.addParameter("detailed", "true");
                final Response response = restClient.performRequest(request);
                /*
                 * Check to see if there are outstanding tasks; we exclude the list task itself, and any expected outstanding tasks using
                 * the specified task filter.
                 */
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    try (
                        BufferedReader responseReader = new BufferedReader(
                            new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)
                        )
                    ) {
                        int activeTasks = 0;
                        String line;
                        final StringBuilder tasksListString = new StringBuilder();
                        while ((line = responseReader.readLine()) != null) {
                            final String taskName = line.split("\\s+")[0];
                            if (taskName.startsWith(TransportListTasksAction.TYPE.name())
                                || taskName.startsWith(HealthNode.TASK_NAME)
                                || taskName.startsWith(LEADER_CHECK_ACTION_NAME)
                                || taskName.startsWith(FOLLOWER_CHECK_ACTION_NAME)
                                || taskFilter.test(taskName)) {
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
     * Returns whether to preserve the security indices created during this test on completion of this test.
     * Defaults to {@code false}. Override this method if security indices should be preserved after the test,
     * with the assumption that some other process or test will clean up the indices afterward.
     * This is useful if the security entities need to be preserved between test runs
     */
    protected boolean preserveSecurityIndicesUponCompletion() {
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
     * Determines whether the system feature reset API should be invoked between tests. The default implementation is to reset
     * all feature states, deleting system indices, system associated indices, and system data streams.
     */
    protected boolean resetFeatureStates() {
        return true;
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
     * A set of ILM policies that should be preserved between runs.
     */
    protected Set<String> preserveILMPolicyIds() {
        return Sets.newHashSet(
            "ilm-history-ilm-policy",
            "slm-history-ilm-policy",
            "watch-history-ilm-policy",
            "watch-history-ilm-policy-16",
            "ml-size-based-ilm-policy",
            "logs",
            "logs@lifecycle",
            "metrics",
            "metrics@lifecycle",
            "profiling-60-days",
            "profiling-60-days@lifecycle",
            "synthetics",
            "agentless",
            "synthetics@lifecycle",
            "traces@lifecycle",
            "7-days-default",
            "7-days@lifecycle",
            "30-days-default",
            "30-days@lifecycle",
            "90-days-default",
            "90-days@lifecycle",
            "180-days-default",
            "180-days@lifecycle",
            "365-days-default",
            "365-days@lifecycle",
            ".fleet-files-ilm-policy",
            ".fleet-file-data-ilm-policy",
            ".fleet-actions-results-ilm-policy",
            ".fleet-file-fromhost-data-ilm-policy",
            ".fleet-file-fromhost-meta-ilm-policy",
            ".fleet-file-tohost-data-ilm-policy",
            ".fleet-file-tohost-meta-ilm-policy",
            ".deprecation-indexing-ilm-policy",
            ".monitoring-8-ilm-policy",
            "behavioral_analytics-events-default_policy",
            "logs-apm.app_logs-default_policy",
            "logs-apm.error_logs-default_policy",
            "metrics-apm.app_metrics-default_policy",
            "metrics-apm.internal_metrics-default_policy",
            "metrics-apm.service_destination_10m_metrics-default_policy",
            "metrics-apm.service_destination_1m_metrics-default_policy",
            "metrics-apm.service_destination_60m_metrics-default_policy",
            "metrics-apm.service_summary_10m_metrics-default_policy",
            "metrics-apm.service_summary_1m_metrics-default_policy",
            "metrics-apm.service_summary_60m_metrics-default_policy",
            "metrics-apm.service_transaction_10m_metrics-default_policy",
            "metrics-apm.service_transaction_1m_metrics-default_policy",
            "metrics-apm.service_transaction_60m_metrics-default_policy",
            "metrics-apm.transaction_10m_metrics-default_policy",
            "metrics-apm.transaction_1m_metrics-default_policy",
            "metrics-apm.transaction_60m_metrics-default_policy",
            "traces-apm.rum_traces-default_policy",
            "traces-apm.sampled_traces-default_policy",
            "traces-apm.traces-default_policy"
        );
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
     * Returns whether to preserve SLM Policies of this test. Defaults to not
     * preserving them. Only runs at all if xpack is installed on the cluster
     * being tested.
     */
    protected boolean preserveSLMPoliciesUponCompletion() {
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

    private void wipeCluster() throws Exception {
        waitForClusterUpdates();

        // Cleanup rollup before deleting indices. A rollup job might have bulks in-flight,
        // so we need to fully shut them down first otherwise a job might stall waiting
        // for a bulk to finish against a non-existing index (and then fail tests)
        if (has(ProductFeature.ROLLUPS) && false == preserveRollupJobsUponCompletion()) {
            wipeRollupJobs();
            waitForPendingRollupTasks();
        }

        if (has(ProductFeature.SLM) && preserveSLMPoliciesUponCompletion() == false) {
            // Clean up SLM policies before trying to wipe snapshots so that no new ones get started by SLM after wiping
            deleteAllSLMPolicies();
        }

        // Clean up searchable snapshots indices before deleting snapshots and repositories
        if (has(ProductFeature.SEARCHABLE_SNAPSHOTS)) {
            if (preserveSearchableSnapshotsIndicesUponCompletion() == false) {
                wipeSearchableSnapshotsIndices();
            }
        }

        wipeSnapshots();

        if (resetFeatureStates()) {
            final Request postRequest = new Request("POST", "/_features/_reset");
            cleanupClient().performRequest(postRequest);
        }

        // wipe data streams before indices so that the backing indices for data streams are handled properly
        if (preserveDataStreamsUponCompletion() == false) {
            wipeDataStreams();
        }

        if (preserveIndicesUponCompletion() == false) {
            // wipe indices
            wipeAllIndices(preserveSecurityIndicesUponCompletion());
        }

        // wipe index templates
        if (preserveTemplatesUponCompletion() == false) {
            if (hasXPack()) {
                /*
                 * Delete only templates that xpack doesn't automatically
                 * recreate. Deleting them doesn't hurt anything, but it
                 * slows down the test because xpack will just recreate
                 * them.
                 */
                try {
                    Request getTemplatesRequest = new Request("GET", "_index_template");
                    Map<String, Object> composableIndexTemplates = XContentHelper.convertToMap(
                        JsonXContent.jsonXContent,
                        EntityUtils.toString(cleanupClient().performRequest(getTemplatesRequest).getEntity()),
                        false
                    );
                    List<String> names = ((List<?>) composableIndexTemplates.get("index_templates")).stream()
                        .map(ct -> (String) ((Map<?, ?>) ct).get("name"))
                        .filter(name -> isXPackTemplate(name) == false)
                        .collect(Collectors.toList());
                    if (names.isEmpty() == false) {
                        try {
                            cleanupClient().performRequest(new Request("DELETE", "_index_template/" + String.join(",", names)));
                        } catch (ResponseException e) {
                            logger.warn(() -> format("unable to remove multiple composable index templates %s", names), e);
                        }
                    }
                } catch (Exception e) {
                    logger.debug("ignoring exception removing all composable index templates", e);
                    // We hit a version of ES that doesn't support index templates v2 yet, so it's safe to ignore
                }
                try {
                    Request compReq = new Request("GET", "_component_template");
                    String componentTemplates = EntityUtils.toString(cleanupClient().performRequest(compReq).getEntity());
                    Map<String, Object> cTemplates = XContentHelper.convertToMap(JsonXContent.jsonXContent, componentTemplates, false);
                    List<String> names = ((List<?>) cTemplates.get("component_templates")).stream()
                        .map(ct -> (String) ((Map<?, ?>) ct).get("name"))
                        .filter(name -> isXPackTemplate(name) == false)
                        .collect(Collectors.toList());
                    if (names.isEmpty() == false) {
                        try {
                            cleanupClient().performRequest(new Request("DELETE", "_component_template/" + String.join(",", names)));
                        } catch (ResponseException e) {
                            logger.warn(() -> format("unable to remove multiple component templates %s", names), e);
                        }
                    }
                } catch (Exception e) {
                    logger.debug("ignoring exception removing all component templates", e);
                    // We hit a version of ES that doesn't support index templates v2 yet, so it's safe to ignore
                }

                if (has(ProductFeature.LEGACY_TEMPLATES)) {
                    Request getLegacyTemplatesRequest = new Request("GET", "_template");
                    Map<String, Object> legacyTemplates = XContentHelper.convertToMap(
                        JsonXContent.jsonXContent,
                        EntityUtils.toString(cleanupClient().performRequest(getLegacyTemplatesRequest).getEntity()),
                        false
                    );
                    for (String name : legacyTemplates.keySet()) {
                        if (isXPackTemplate(name)) {
                            continue;
                        }
                        try {
                            cleanupClient().performRequest(new Request("DELETE", "_template/" + name));
                        } catch (ResponseException e) {
                            logger.debug(() -> format("unable to remove index template %s", name), e);
                        }
                    }
                }
            } else {
                logger.debug("Clearing all templates");
                if (has(ProductFeature.LEGACY_TEMPLATES)) {
                    cleanupClient().performRequest(new Request("DELETE", "_template/*"));
                }
                try {
                    cleanupClient().performRequest(new Request("DELETE", "_index_template/*"));
                    cleanupClient().performRequest(new Request("DELETE", "_component_template/*"));
                } catch (ResponseException e) {
                    // We hit a version of ES that doesn't support index templates v2 yet, so it's safe to ignore
                }
            }
        }

        // wipe cluster settings
        if (preserveClusterSettings() == false) {
            wipeClusterSettings();
        }

        if (has(ProductFeature.ILM) && false == preserveILMPoliciesUponCompletion()) {
            deleteAllILMPolicies(preserveILMPolicyIds());
        }

        if (has(ProductFeature.CCR) && false == preserveAutoFollowPatternsUponCompletion()) {
            deleteAllAutoFollowPatterns();
        }

        deleteAllNodeShutdownMetadata();
    }

    private void waitForClusterUpdates() throws Exception {
        logger.info("Waiting for all cluster updates up to this moment to be processed");

        try {
            assertOK(cleanupClient().performRequest(new Request("GET", "_cluster/health?wait_for_events=languid")));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_REQUEST_TIMEOUT) {
                StringBuilder logMessage = new StringBuilder("Timed out waiting for cluster updates to be processed.");
                final var pendingTasks = getPendingClusterStateTasks();
                if (pendingTasks != null) {
                    logMessage.append('\n').append(pendingTasks);
                }
                final var hotThreads = getHotThreads();
                if (hotThreads != null) {
                    logMessage.append("\nHot threads: ").append(hotThreads);
                }
                logger.error(logMessage.toString());
            }
            throw e;
        }
    }

    private static String getPendingClusterStateTasks() {
        try {
            Response response = cleanupClient().performRequest(new Request("GET", "/_cluster/pending_tasks"));
            List<?> tasks = (List<?>) entityAsMap(response).get("tasks");
            if (tasks.isEmpty() == false) {
                StringBuilder message = new StringBuilder("There are still running tasks:");
                for (Object task : tasks) {
                    message.append('\n').append(task.toString());
                }
                return message.toString();
            }
        } catch (IOException e) {
            fail(e, "Failed to retrieve pending tasks in the cluster during cleanup");
        }
        return null;
    }

    private String getHotThreads() {
        try {
            Response response = adminClient().performRequest(
                new Request("GET", "/_nodes/hot_threads?ignore_idle_threads=false&threads=9999")
            );
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            logger.error("Failed to retrieve hot threads in the cluster during cleanup", e);
        }
        return null;
    }

    /**
     * This method checks whether ILM policies or templates get recreated after they have been deleted. If so, we are probably deleting
     * them unnecessarily, potentially causing test performance problems. This could happen for example if someone adds a new standard ILM
     * policy but forgets to put it in the exclusion list in this test.
     *
     * @throws IOException
     */
    private void checkForUnexpectedlyRecreatedObjects() throws IOException {
        if (has(ProductFeature.ILM) && false == preserveILMPoliciesUponCompletion()) {
            Set<String> unexpectedIlmPlicies = getAllUnexpectedIlmPolicies(preserveILMPolicyIds());
            assertTrue(
                "Expected no ILM policies after deletions, but found " + String.join(", ", unexpectedIlmPlicies),
                unexpectedIlmPlicies.isEmpty()
            );
        }
        Set<String> unexpectedTemplates = getAllUnexpectedTemplates();
        assertTrue(
            "Expected no templates after deletions, but found " + String.join(", ", unexpectedTemplates),
            unexpectedTemplates.isEmpty()
        );
    }

    private static Set<String> getAllUnexpectedIlmPolicies(Set<String> exclusions) throws IOException {
        Map<String, Object> policies;
        try {
            Response response = cleanupClient().performRequest(new Request("GET", "/_ilm/policy"));
            policies = entityAsMap(response);
        } catch (ResponseException e) {
            if (RestStatus.METHOD_NOT_ALLOWED.getStatus() == e.getResponse().getStatusLine().getStatusCode()
                || RestStatus.BAD_REQUEST.getStatus() == e.getResponse().getStatusLine().getStatusCode()) {
                // If bad request returned, ILM is not enabled.
                policies = new HashMap<>();
            } else {
                throw e;
            }
        }
        Set<String> unexpectedPolicies = policies.keySet()
            .stream()
            .filter(p -> exclusions.contains(p) == false)
            .collect(Collectors.toSet());
        return unexpectedPolicies;
    }

    private Set<String> getAllUnexpectedTemplates() throws IOException {
        Set<String> unexpectedTemplates = new HashSet<>();
        if (preserveDataStreamsUponCompletion() == false && preserveTemplatesUponCompletion() == false) {
            if (has(ProductFeature.XPACK)) {
                Request getTemplatesRequest = new Request("GET", "_index_template");
                Map<String, Object> composableIndexTemplates = XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    EntityUtils.toString(cleanupClient().performRequest(getTemplatesRequest).getEntity()),
                    false
                );
                unexpectedTemplates.addAll(
                    ((List<?>) composableIndexTemplates.get("index_templates")).stream()
                        .map(ct -> (String) ((Map<?, ?>) ct).get("name"))
                        .filter(name -> isXPackTemplate(name) == false)
                        .collect(Collectors.toSet())
                );
                Request compReq = new Request("GET", "_component_template");
                String componentTemplates = EntityUtils.toString(cleanupClient().performRequest(compReq).getEntity());
                Map<String, Object> cTemplates = XContentHelper.convertToMap(JsonXContent.jsonXContent, componentTemplates, false);
                ((List<?>) cTemplates.get("component_templates")).stream()
                    .map(ct -> (String) ((Map<?, ?>) ct).get("name"))
                    .filter(name -> isXPackTemplate(name) == false)
                    .forEach(unexpectedTemplates::add);

                if (has(ProductFeature.LEGACY_TEMPLATES)) {
                    Request getLegacyTemplatesRequest = new Request("GET", "_template");
                    Map<String, Object> legacyTemplates = XContentHelper.convertToMap(
                        JsonXContent.jsonXContent,
                        EntityUtils.toString(cleanupClient().performRequest(getLegacyTemplatesRequest).getEntity()),
                        false
                    );
                    unexpectedTemplates.addAll(
                        legacyTemplates.keySet().stream().filter(template -> isXPackTemplate(template) == false).collect(Collectors.toSet())
                    );
                }
            } else {
                // Do nothing
            }
        }
        return unexpectedTemplates;
    }

    /**
     * If any nodes are registered for shutdown, removes their metadata.
     */
    @SuppressWarnings("unchecked")
    protected void deleteAllNodeShutdownMetadata() throws IOException {
        if (has(ProductFeature.SHUTDOWN) == false) {
            return;
        }

        Request getShutdownStatus = new Request("GET", "_nodes/shutdown");
        Map<String, Object> statusResponse = responseAsMap(cleanupClient().performRequest(getShutdownStatus));

        Object nodesResponse = statusResponse.get("nodes");
        final List<String> nodeIds;
        if (nodesResponse instanceof List<?>) { // `nodes` is parsed as a List<> only if it's populated (not empty)
            List<Map<String, Object>> nodesArray = (List<Map<String, Object>>) nodesResponse;
            nodeIds = nodesArray.stream().map(nodeShutdownMetadata -> (String) nodeShutdownMetadata.get("node_id")).toList();
        } else {
            nodeIds = List.of();
        }

        for (String nodeId : nodeIds) {
            Request deleteRequest = new Request("DELETE", "_nodes/" + nodeId + "/shutdown");
            assertOK(cleanupClient().performRequest(deleteRequest));
        }
    }

    protected static void wipeAllIndices() throws IOException {
        wipeAllIndices(false);
    }

    protected static void wipeAllIndices(boolean preserveSecurityIndices) throws IOException {
        try {
            // remove all indices except some history indices which can pop up after deleting all data streams but shouldn't interfere
            final List<String> indexPatterns = new ArrayList<>(
                List.of("*", "-.ds-ilm-history-*", "-.ds-.slm-history-*", "-.ds-.watcher-history-*")
            );
            if (preserveSecurityIndices) {
                indexPatterns.add("-.security-*");
            }
            final Request deleteRequest = new Request("DELETE", Strings.collectionToCommaDelimitedString(indexPatterns));
            deleteRequest.addParameter("expand_wildcards", "open,closed,hidden");

            // If system index warning, ignore but log
            // See: https://github.com/elastic/elasticsearch/issues/117099
            // and: https://github.com/elastic/elasticsearch/issues/115809
            deleteRequest.setOptions(
                RequestOptions.DEFAULT.toBuilder().setWarningsHandler(ESRestTestCase::ignoreSystemIndexAccessWarnings)
            );

            final Response response = cleanupClient().performRequest(deleteRequest);
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

    protected static boolean ignoreSystemIndexAccessWarnings(List<String> warnings) {
        for (String warning : warnings) {
            if (warning.startsWith("this request accesses system indices:")) {
                SUITE_LOGGER.warn("Ignoring system index access warning during test cleanup: {}", warning);
            } else {
                return true;
            }
        }

        return false;
    }

    protected static void wipeDataStreams() throws IOException {
        try {
            if (hasXPack()) {
                cleanupClient().performRequest(new Request("DELETE", "_data_stream/*?expand_wildcards=all"));
            }
        } catch (ResponseException e) {
            // We hit a version of ES that doesn't understand expand_wildcards, try again without it
            try {
                if (hasXPack()) {
                    cleanupClient().performRequest(new Request("DELETE", "_data_stream/*"));
                }
            } catch (ResponseException ee) {
                // We hit a version of ES that doesn't serialize DeleteDataStreamAction.Request#wildcardExpressionsOriginallySpecified field
                // or that doesn't support data streams so it's safe to ignore
                int statusCode = ee.getResponse().getStatusLine().getStatusCode();
                if (statusCode == 400) {
                    // the test cluster likely does not include the data streams module so we can ignore this error code
                    // additionally there is an implementation gotcha that cause response code to be 400 or 405 dependent on if
                    // "_data_stream/*" matches a registered index pattern such as {a}/{b} but not for the HTTP verb.
                    // Prior to v9 POST {index}/{type} was registered as a compatible index pattern so the request would partially match
                    // and return a 405, but without that pattern registered at all the return value is a 400.
                    return;
                } else if (statusCode < 404 || statusCode > 405) {
                    throw ee;
                }
            }
        }
    }

    protected void wipeSearchableSnapshotsIndices() throws IOException {
        // retrieves all indices with a type of store equals to "snapshot"
        final Request request = new Request("GET", "_cluster/state/metadata");
        request.addParameter("filter_path", "metadata.indices.*.settings.index.store.snapshot");

        final Response response = cleanupClient().performRequest(request);
        @SuppressWarnings("unchecked")
        Map<String, ?> indices = (Map<String, ?>) XContentMapValues.extractValue("metadata.indices", entityAsMap(response));
        if (indices != null) {
            for (String index : indices.keySet()) {
                try {
                    assertAcked(
                        "Failed to delete searchable snapshot index [" + index + ']',
                        cleanupClient().performRequest(new Request("DELETE", index))
                    );
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
     * start empty.
     */
    protected void wipeSnapshots() throws IOException {
        for (Map.Entry<String, ?> repo : entityAsMap(cleanupClient().performRequest(new Request("GET", "/_snapshot/_all"))).entrySet()) {
            String repoName = repo.getKey();
            Map<?, ?> repoSpec = (Map<?, ?>) repo.getValue();
            String repoType = (String) repoSpec.get("type");
            if (false == preserveSnapshotsUponCompletion() && repoType.equals("fs")) {
                // All other repo types we really don't have a chance of being able to iterate properly, sadly.
                cleanupClient().performRequest(new Request("DELETE", "/_snapshot/" + repoName + "/*"));
            }
            if (preserveReposUponCompletion() == false) {
                deleteRepository(repoName);
            }
        }
    }

    protected void deleteRepository(String repoName) throws IOException {
        logger.debug("wiping snapshot repository [{}]", repoName);
        cleanupClient().performRequest(new Request("DELETE", "_snapshot/" + repoName));
    }

    /**
     * Remove any cluster settings.
     */
    private static void wipeClusterSettings() throws IOException {
        Map<?, ?> getResponse = entityAsMap(cleanupClient().performRequest(new Request("GET", "/_cluster/settings")));

        final var mustClear = new AtomicBoolean();
        final var request = newXContentRequest(HttpMethod.PUT, "/_cluster/settings", (clearCommand, params) -> {
            for (Map.Entry<?, ?> entry : getResponse.entrySet()) {
                String type = entry.getKey().toString();
                Map<?, ?> settings = (Map<?, ?>) entry.getValue();
                if (settings.isEmpty()) {
                    continue;
                }
                mustClear.set(true);
                clearCommand.startObject(type);
                for (Object key : settings.keySet()) {
                    clearCommand.field(key + ".*").nullValue();
                }
                clearCommand.endObject();
            }
            return clearCommand;
        });

        if (mustClear.get()) {
            request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> {
                if (warnings.isEmpty()) {
                    return false;
                } else if (warnings.size() > 1) {
                    return true;
                } else {
                    return warnings.get(0).contains("xpack.monitoring") == false;
                }
            }));
            cleanupClient().performRequest(request);
        }
    }

    private void wipeRollupJobs() throws IOException {
        final Response response;
        try {
            var request = new Request("GET", "/_rollup/job/_all");
            request.setOptions(ROLLUP_REQUESTS_OPTIONS);
            response = cleanupClient().performRequest(request);
        } catch (ResponseException e) {
            // If we don't see the rollup endpoint (possibly because of running against an older ES version) we just bail
            if (e.getResponse().getStatusLine().getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
                return;
            }
            throw e;
        }
        Map<String, Object> jobs = entityAsMap(response);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobConfigs = (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", jobs);

        if (jobConfigs == null) {
            return;
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            @SuppressWarnings("unchecked")
            String jobId = (String) ((Map<String, Object>) jobConfig.get("config")).get("id");
            Request request = new Request("POST", "/_rollup/job/" + jobId + "/_stop");
            request.setOptions(ROLLUP_REQUESTS_OPTIONS);
            setIgnoredErrorResponseCodes(request, RestStatus.NOT_FOUND);
            request.addParameter("wait_for_completion", "true");
            request.addParameter("timeout", "10s");
            logger.debug("stopping rollup job [{}]", jobId);
            cleanupClient().performRequest(request);
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            @SuppressWarnings("unchecked")
            String jobId = (String) ((Map<String, Object>) jobConfig.get("config")).get("id");
            Request request = new Request("DELETE", "/_rollup/job/" + jobId);
            request.setOptions(ROLLUP_REQUESTS_OPTIONS);
            setIgnoredErrorResponseCodes(request, RestStatus.NOT_FOUND); // 404s imply someone was racing us to delete this
            logger.debug("deleting rollup job [{}]", jobId);
            cleanupClient().performRequest(request);
        }
    }

    protected void refreshAllIndices() throws IOException {
        Request refreshRequest = new Request("POST", "/_refresh");
        refreshRequest.addParameter("expand_wildcards", "open,hidden");
        // Allow system index deprecation warnings
        refreshRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> {
            if (warnings.isEmpty()) {
                return false;
            } else if (warnings.size() > 1) {
                return true;
            } else {
                return warnings.get(0).startsWith("this request accesses system indices:") == false;
            }
        }));
        client().performRequest(refreshRequest);
    }

    protected static BroadcastResponse refresh(String index) throws IOException {
        return refresh(client(), index);
    }

    private static final ConstructingObjectParser<BroadcastResponse, Void> BROADCAST_RESPONSE_PARSER = new ConstructingObjectParser<>(
        "broadcast_response",
        true,
        arg -> {
            BaseBroadcastResponse response = (BaseBroadcastResponse) arg[0];
            return new BroadcastResponse(
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getFailedShards(),
                Arrays.asList(response.getShardFailures())
            );
        }
    );

    static {
        AbstractBroadcastResponseTestCase.declareBroadcastFields(BROADCAST_RESPONSE_PARSER);
    }

    protected static BroadcastResponse refresh(RestClient client, String index) throws IOException {
        Request refreshRequest = new Request("POST", "/" + index + "/_refresh");
        Response response = client.performRequest(refreshRequest);
        try (var parser = responseAsParser(response)) {
            return BROADCAST_RESPONSE_PARSER.apply(parser, null);
        }
    }

    private static void waitForPendingRollupTasks() throws Exception {
        waitForPendingTasks(cleanupClient(), taskName -> taskName.startsWith("xpack/rollup/job") == false);
    }

    private static void deleteAllILMPolicies(Set<String> exclusions) throws IOException {
        Map<String, Object> policies;

        try {
            Response response = cleanupClient().performRequest(new Request("GET", "/_ilm/policy"));
            policies = entityAsMap(response);
        } catch (ResponseException e) {
            if (RestStatus.METHOD_NOT_ALLOWED.getStatus() == e.getResponse().getStatusLine().getStatusCode()
                || RestStatus.BAD_REQUEST.getStatus() == e.getResponse().getStatusLine().getStatusCode()) {
                // If bad request returned, ILM is not enabled.
                return;
            }
            throw e;
        }

        if (policies == null || policies.isEmpty()) {
            return;
        }

        policies.keySet().stream().filter(p -> exclusions.contains(p) == false).forEach(policyName -> {
            try {
                cleanupClient().performRequest(new Request("DELETE", "/_ilm/policy/" + policyName));
            } catch (IOException e) {
                throw new RuntimeException("failed to delete policy: " + policyName, e);
            }
        });
    }

    private static void deleteAllSLMPolicies() throws IOException {
        Map<String, Object> policies;

        try {
            Response response = cleanupClient().performRequest(new Request("GET", "/_slm/policy"));
            policies = entityAsMap(response);
        } catch (ResponseException e) {
            if (RestStatus.METHOD_NOT_ALLOWED.getStatus() == e.getResponse().getStatusLine().getStatusCode()
                || RestStatus.BAD_REQUEST.getStatus() == e.getResponse().getStatusLine().getStatusCode()) {
                // If bad request returned, SLM is not enabled.
                return;
            }
            throw e;
        }

        if (policies == null || policies.isEmpty()) {
            return;
        }

        for (String policyName : policies.keySet()) {
            cleanupClient().performRequest(new Request("DELETE", "/_slm/policy/" + policyName));
        }
    }

    @SuppressWarnings("unchecked")
    private static void deleteAllAutoFollowPatterns() throws IOException {
        final List<Map<?, ?>> patterns;

        try {
            Response response = cleanupClient().performRequest(new Request("GET", "/_ccr/auto_follow"));
            patterns = (List<Map<?, ?>>) entityAsMap(response).get("patterns");
        } catch (ResponseException e) {
            if (RestStatus.METHOD_NOT_ALLOWED.getStatus() == e.getResponse().getStatusLine().getStatusCode()
                || RestStatus.BAD_REQUEST.getStatus() == e.getResponse().getStatusLine().getStatusCode()) {
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
            cleanupClient().performRequest(new Request("DELETE", "/_ccr/auto_follow/" + patternName));
        }
    }

    /**
     * Logs a message if there are still running tasks. The reasoning is that any tasks still running are state the is trying to bleed into
     * other tests.
     */
    private void logIfThereAreRunningTasks() throws IOException {
        Set<String> runningTasks = runningTasks(cleanupClient().performRequest(new Request("GET", "/_tasks")));
        // Ignore the task list API - it doesn't count against us
        runningTasks.remove(TransportListTasksAction.TYPE.name());
        runningTasks.remove(TransportListTasksAction.TYPE.name() + "[n]");
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
    private static void waitForClusterStateUpdatesToFinish() throws Exception {
        assertBusy(() -> {
            final var pendingTasks = getPendingClusterStateTasks();
            if (pendingTasks != null) {
                fail(pendingTasks);
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
        if (System.getProperty("tests.rest.cluster.username") != null) {
            if (System.getProperty("tests.rest.cluster.password") == null) {
                throw new IllegalStateException("The 'test.rest.cluster.password' system property must be set.");
            }
            String username = System.getProperty("tests.rest.cluster.username");
            String password = System.getProperty("tests.rest.cluster.password");
            String token = basicAuthHeaderValue(username, new SecureString(password.toCharArray()));
            builder.put(ThreadContext.PREFIX + ".Authorization", token);
        }
        if (System.getProperty("tests.rest.project.id") != null) {
            final var projectId = System.getProperty("tests.rest.project.id");
            builder.put(ThreadContext.PREFIX + ".X-Elastic-Project-Id", projectId);
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
     * Returns the REST client used for cleaning up the cluster.
     */
    protected Settings cleanupClientSettings() {
        if (multiProjectEnabled == false || shouldConfigureProjects() == false) {
            return restAdminSettings();
        }
        return addProjectIdToSettings(restAdminSettings());
    }

    private Settings addProjectIdToSettings(Settings settings) {
        if (multiProjectEnabled == false || shouldConfigureProjects() == false) {
            return settings;
        }
        return Settings.builder()
            .put(settings)
            .put(ThreadContext.PREFIX + "." + Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, activeProject)
            .build();
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

    /**
     * Override this to configure the client with additional settings.
     */
    protected void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        doConfigureClient(builder, settings);
    }

    protected static void doConfigureClient(RestClientBuilder builder, Settings settings) throws IOException {
        String truststorePath = settings.get(TRUSTSTORE_PATH);
        String certificateAuthorities = settings.get(CERTIFICATE_AUTHORITIES);
        String clientCertificatePath = settings.get(CLIENT_CERT_PATH);

        if (certificateAuthorities != null && truststorePath != null) {
            throw new IllegalStateException(
                "Cannot set both " + CERTIFICATE_AUTHORITIES + " and " + TRUSTSTORE_PATH + ". Please configure one of these."
            );

        }
        if (truststorePath != null) {
            if (inFipsJvm()) {
                throw new IllegalStateException(
                    "Keystore "
                        + truststorePath
                        + "cannot be used in FIPS 140 mode. Please configure "
                        + CERTIFICATE_AUTHORITIES
                        + " with a PEM encoded trusted CA/certificate instead"
                );
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
                Certificate caCert = PemUtils.readCertificates(List.of(caPath)).get(0);
                keyStore.setCertificateEntry(caCert.toString(), caCert);
                final SSLContextBuilder sslContextBuilder = SSLContexts.custom();
                if (clientCertificatePath != null) {
                    final Path certPath = PathUtils.get(clientCertificatePath);
                    final Path keyPath = PathUtils.get(Objects.requireNonNull(settings.get(CLIENT_KEY_PATH), "No key provided"));
                    final String password = settings.get(CLIENT_KEY_PASSWORD);
                    final char[] passwordChars = password == null ? null : password.toCharArray();
                    final PrivateKey key = PemUtils.readPrivateKey(keyPath, () -> passwordChars);
                    final Certificate[] clientCertChain = PemUtils.readCertificates(List.of(certPath)).toArray(Certificate[]::new);
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
        final String socketTimeoutString = Objects.requireNonNullElse(settings.get(CLIENT_SOCKET_TIMEOUT), "6000s");
        final TimeValue socketTimeout = TimeValue.parseTimeValue(socketTimeoutString, CLIENT_SOCKET_TIMEOUT);
        builder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<String> runningTasks(Response response) throws IOException {
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

    public static Response assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
        return response;
    }

    public static void assertOKAndConsume(Response response) {
        try {
            assertOK(response);
        } finally {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    public static ObjectPath assertOKAndCreateObjectPath(Response response) throws IOException {
        assertOK(response);
        return ObjectPath.createFromResponse(response);
    }

    /**
     * Assert that the index in question has the given number of documents present
     */
    public static void assertDocCount(RestClient client, String indexName, long docCount) throws IOException {
        Request countReq = new Request("GET", "/" + indexName + "/_count");
        ObjectPath resp = ObjectPath.createFromResponse(client.performRequest(countReq));
        assertEquals(
            "expected " + docCount + " documents but it was a different number",
            docCount,
            Long.parseLong(resp.evaluate("count").toString())
        );
    }

    public static void assertAcknowledged(Response response) throws IOException {
        try {
            assertOK(response);
            String jsonBody = EntityUtils.toString(response.getEntity());
            assertThat(jsonBody, containsString("\"acknowledged\":true"));
        } finally {
            // if assertOK throws an exception, still release resources
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    /**
     * Updates the cluster with the provided settings (as persistent settings)
     **/
    public static void updateClusterSettings(Settings settings) throws IOException {
        updateClusterSettings(client(), settings);
    }

    /**
     * Updates the cluster with the provided settings (as persistent settings)
     **/
    public static void updateClusterSettings(RestClient client, Settings settings) throws IOException {
        final var request = newXContentRequest(HttpMethod.PUT, "/_cluster/settings", (builder, params) -> {
            builder.startObject("persistent");
            settings.toXContent(builder, params);
            return builder.endObject();
        });
        assertOK(client.performRequest(request));
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
        ensureGreen(client(), index);
    }

    public final void ensureGreen(RestClient client, String index) throws IOException {
        ensureHealth(client, index, (request) -> {
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

    public static void ensureHealth(RestClient restClient, Consumer<Request> requestConsumer) throws IOException {
        ensureHealth(restClient, "", requestConsumer);
    }

    public static void ensureHealth(RestClient restClient, String index, Consumer<Request> requestConsumer) throws IOException {
        Request request = new Request("GET", "/_cluster/health" + (index.isBlank() ? "" : "/" + index));
        requestConsumer.accept(request);
        try {
            restClient.performRequest(request);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_REQUEST_TIMEOUT) {
                try {
                    final Response clusterStateResponse = restClient.performRequest(new Request("GET", "/_cluster/state?pretty"));
                    fail(
                        "timed out waiting for green state for index ["
                            + index
                            + "] "
                            + "cluster state ["
                            + EntityUtils.toString(clusterStateResponse.getEntity())
                            + "]"
                    );
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
        cleanupClient().performRequest(request);
    }

    protected static CreateIndexResponse createIndex(String name) throws IOException {
        return createIndex(name, null, null, null);
    }

    protected static CreateIndexResponse createIndex(String name, Settings settings) throws IOException {
        return createIndex(name, settings, null, null);
    }

    protected static CreateIndexResponse createIndex(RestClient client, String name, Settings settings) throws IOException {
        return createIndex(client, name, settings, null, null);
    }

    protected static CreateIndexResponse createIndex(RestClient client, String name, Settings settings, String mapping) throws IOException {
        return createIndex(client, name, settings, mapping, null);
    }

    protected static CreateIndexResponse createIndex(String name, Settings settings, String mapping) throws IOException {
        return createIndex(name, settings, mapping, null);
    }

    protected static CreateIndexResponse createIndex(String name, Settings settings, String mapping, String aliases) throws IOException {
        return createIndex(client(), name, settings, mapping, aliases);
    }

    public static CreateIndexResponse createIndex(RestClient client, String name, Settings settings, String mapping, String aliases)
        throws IOException {
        return createIndex(client::performRequest, name, settings, mapping, aliases);
    }

    protected static CreateIndexResponse createIndex(
        CheckedFunction<Request, Response, IOException> execute,
        String name,
        Settings settings,
        String mapping,
        String aliases
    ) throws IOException {
        final Request request = newXContentRequest(HttpMethod.PUT, "/" + name, (builder, params) -> {
            if (settings != null) {
                builder.startObject("settings");
                settings.toXContent(builder, params);
                builder.endObject();
            }

            if (mapping != null) {
                try (
                    var mappingParser = XContentType.JSON.xContent()
                        .createParser(XContentParserConfiguration.EMPTY, mapping.trim().startsWith("{") ? mapping : '{' + mapping + '}')
                ) {
                    builder.field("mappings");
                    builder.copyCurrentStructure(mappingParser);
                }
            }

            if (aliases != null) {
                try (
                    var aliasesParser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, '{' + aliases + '}')
                ) {
                    builder.field("aliases");
                    builder.copyCurrentStructure(aliasesParser);
                }
            }

            return builder;
        });

        if (settings != null && settings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) == false) {
            expectSoftDeletesWarning(request, name);
        }
        final Response response = execute.apply(request);
        try (var parser = responseAsParser(response)) {
            return TestResponseParsers.parseCreateIndexResponse(parser);
        }
    }

    protected static AcknowledgedResponse deleteIndex(String name) throws IOException {
        return deleteIndex(client(), name);
    }

    protected static AcknowledgedResponse deleteIndex(RestClient restClient, String name) throws IOException {
        Request request = new Request("DELETE", "/" + name);
        Response response = restClient.performRequest(request);
        try (var parser = responseAsParser(response)) {
            return TestResponseParsers.parseAcknowledgedResponse(parser);
        }
    }

    protected static void updateIndexSettings(String index, Settings.Builder settings) throws IOException {
        updateIndexSettings(index, settings.build());
    }

    private static void updateIndexSettings(String index, Settings settings) throws IOException {
        final var request = newXContentRequest(HttpMethod.PUT, "/" + index + "/_settings", settings);
        assertOK(client.performRequest(request));
    }

    protected static void expectSoftDeletesWarning(Request request, String indexName) throws IOException {
        final String expectedWarning =
            "Creating indices with soft-deletes disabled is deprecated and will be removed in future Elasticsearch versions. "
                + "Please do not specify value for setting [index.soft_deletes.enabled] of index ["
                + indexName
                + "].";

        final var softDeleteDisabledDeprecated = minimumIndexVersion().onOrAfter(IndexVersions.V_7_6_0);
        request.setOptions(expectVersionSpecificWarnings(v -> {
            if (softDeleteDisabledDeprecated) {
                v.current(expectedWarning);
            }
            v.compatible(expectedWarning);
        }));
    }

    protected static Map<String, Object> getIndexSettings(String index) throws IOException {
        return getIndexSettings(index, false);
    }

    protected static Map<String, Object> getIndexSettings(String index, boolean includeDefaults) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        if (includeDefaults) {
            request.addParameter("include_defaults", "true");
        }
        Response response = client().performRequest(request);
        try (InputStream is = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(
                XContentType.fromMediaType(response.getEntity().getContentType().getValue()).xContent(),
                is,
                true
            );
        }
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> getIndexSettingsAsMap(String index) throws IOException {
        Map<String, Object> indexSettings = getIndexSettings(index);
        return (Map<String, Object>) ((Map<String, Object>) indexSettings.get(index)).get("settings");
    }

    protected static Map<String, Object> getIndexMapping(String index) throws IOException {
        return entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_mapping")));
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getIndexMappingAsMap(String index) throws IOException {
        Map<String, Object> indexMapping = getIndexMapping(index);
        return (Map<String, Object>) ((Map<String, Object>) indexMapping.get(index)).get("mappings");
    }

    protected static boolean indexExists(String index) throws IOException {
        return indexExists(client(), index);
    }

    protected static boolean indexExists(RestClient client, String index) throws IOException {
        // We use the /_cluster/health/{index} API to ensure the index exists on the master node - which means all nodes see the index.
        Request request = new Request("GET", "/_cluster/health/" + index);
        request.addParameter("timeout", "0");
        request.addParameter("level", "indices");
        try {
            final var response = client.performRequest(request);
            @SuppressWarnings("unchecked")
            final var indices = (Map<String, Object>) entityAsMap(response).get("indices");
            return indices.containsKey(index);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_REQUEST_TIMEOUT) {
                return false;
            }
            throw e;
        }
    }

    protected static void awaitIndexExists(String index) throws IOException {
        awaitIndexExists(index, SAFE_AWAIT_TIMEOUT);
    }

    protected static void awaitIndexExists(String index, TimeValue timeout) throws IOException {
        // We use the /_cluster/health/{index} API to ensure the index exists on the master node - which means all nodes see the index.
        ensureHealth(client(), index, request -> request.addParameter("timeout", timeout.toString()));
    }

    protected static void awaitIndexDoesNotExist(String index, TimeValue timeout) throws Exception {
        assertBusy(() -> assertFalse(indexExists(index)), timeout.millis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Deprecation message emitted since 7.12.0 for the rest of the 7.x series. Can be removed in v10 since it is not
     * emitted in v8 or v9 and N-2 versions are now supported.
     * Note that this message is also permitted in certain YAML test cases, it can be removed there too.
     * See https://github.com/elastic/elasticsearch/issues/66419 and https://github.com/elastic/elasticsearch/pull/119594 for more details.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)
    private static final String WAIT_FOR_ACTIVE_SHARDS_DEFAULT_DEPRECATION_MESSAGE = "the default value for the ?wait_for_active_shards "
        + "parameter will change from '0' to 'index-setting' in version 8; specify '?wait_for_active_shards=index-setting' "
        + "to adopt the future default behaviour, or '?wait_for_active_shards=0' to preserve today's behaviour";

    protected static void closeIndex(String index) throws IOException {
        final Request closeRequest = new Request(HttpPost.METHOD_NAME, "/" + index + "/_close");
        closeRequest.setOptions(expectVersionSpecificWarnings(v -> v.compatible(WAIT_FOR_ACTIVE_SHARDS_DEFAULT_DEPRECATION_MESSAGE)));
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

    /**
     * Returns a list of the data stream's backing index names.
     */
    protected static List<String> getDataStreamBackingIndexNames(String dataStreamName) throws IOException {
        return getDataStreamBackingIndexNames(client(), dataStreamName);
    }

    /**
     * Returns a list of the data stream's backing index names.
     */
    @SuppressWarnings("unchecked")
    protected static List<String> getDataStreamBackingIndexNames(RestClient client, String dataStreamName) throws IOException {
        Map<String, Object> response = getAsMap(client, "/_data_stream/" + dataStreamName);
        List<?> dataStreams = (List<?>) response.get("data_streams");
        assertThat(dataStreams.size(), equalTo(1));
        Map<?, ?> dataStream = (Map<?, ?>) dataStreams.getFirst();
        assertThat(dataStream.get("name"), equalTo(dataStreamName));
        List<?> indices = (List<?>) dataStream.get("indices");
        return indices.stream().map(index -> ((Map<String, String>) index).get("index_name")).toList();
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
        return (Map<String, Object>) XContentMapValues.extractValue(index + ".aliases." + alias, getAliasResponse);
    }

    protected static Map<String, Object> getAsMap(final String endpoint) throws IOException {
        return getAsMap(client(), endpoint, false);
    }

    protected static Map<String, Object> getAsOrderedMap(final String endpoint) throws IOException {
        return getAsMap(client(), endpoint, true);
    }

    protected static Map<String, Object> getAsMap(RestClient client, final String endpoint) throws IOException {
        return getAsMap(client, endpoint, false);
    }

    private static Map<String, Object> getAsMap(RestClient client, final String endpoint, final boolean ordered) throws IOException {
        Response response = client.performRequest(new Request("GET", endpoint));
        return responseAsMap(response, ordered);
    }

    protected static Map<String, Object> responseAsMap(Response response) throws IOException {
        return responseAsMap(response, false);
    }

    protected static Map<String, Object> responseAsOrderedMap(Response response) throws IOException {
        return responseAsMap(response, true);
    }

    private static Map<String, Object> responseAsMap(Response response, boolean ordered) throws IOException {
        XContentType entityContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
        Map<String, Object> responseEntity = XContentHelper.convertToMap(
            entityContentType.xContent(),
            response.getEntity().getContent(),
            ordered
        );
        assertNotNull(responseEntity);
        return responseEntity;
    }

    public static XContentParser responseAsParser(Response response) throws IOException {
        return XContentHelper.createParser(
            XContentParserConfiguration.EMPTY,
            responseAsBytes(response),
            XContentType.fromMediaType(response.getEntity().getContentType().getValue())
        );
    }

    protected static BytesReference responseAsBytes(Response response) throws IOException {
        return new BytesArray(EntityUtils.toByteArray(response.getEntity()));
    }

    protected static void registerRepository(String repository, String type, boolean verify, Settings settings) throws IOException {
        registerRepository(client(), repository, type, verify, settings);
    }

    protected static void registerRepository(RestClient restClient, String repository, String type, boolean verify, Settings settings)
        throws IOException {

        final Request request = newXContentRequest(
            HttpMethod.PUT,
            "/_snapshot/" + repository,
            new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repository).type(type).settings(settings)
        );
        request.addParameter("verify", Boolean.toString(verify));

        final Response response = restClient.performRequest(request);
        assertAcked("Failed to create repository [" + repository + "] of type [" + type + "]: " + response, response);
    }

    protected static void createSnapshot(String repository, String snapshot, boolean waitForCompletion) throws IOException {
        createSnapshot(client(), repository, snapshot, waitForCompletion);
    }

    protected static void createSnapshot(RestClient restClient, String repository, String snapshot, boolean waitForCompletion)
        throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        final Response response = restClient.performRequest(request);
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

    protected static void deleteSnapshot(RestClient restClient, String repository, String snapshot, boolean ignoreMissing)
        throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        if (ignoreMissing) {
            setIgnoredErrorResponseCodes(request, RestStatus.NOT_FOUND);
        }
        final Response response = restClient.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), ignoreMissing ? anyOf(equalTo(200), equalTo(404)) : equalTo(200));
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
        if (name.startsWith(".fleet-")) {
            return true;
        }
        if (name.startsWith("behavioral_analytics-")) {
            return true;
        }
        if (name.startsWith("profiling-")) {
            return true;
        }
        if (name.startsWith("elastic-connectors")) {
            return true;
        }
        if (name.contains("@") && name.endsWith("@custom") == false) {
            // We have a naming convention that internal component templates contain `@`. See also index-templates.asciidoc.
            return true;
        }
        if (name.startsWith("apm@")
            || name.startsWith("apm-")
            || name.startsWith("traces-apm")
            || name.startsWith("metrics-apm")
            || name.startsWith("logs-apm")) {
            return true;
        }
        if (name.startsWith(".slm-history") || name.startsWith("ilm-history")) {
            return true;
        }
        switch (name) {
            case ".watches":
            case "security_audit_log":
            case ".async-search":
            case ".profiling-ilm-lock": // TODO: Remove after switch to K/V indices
            case "saml-service-provider":
            case "logs":
            case "logs-settings":
            case "logs-mappings":
            case "metrics":
            case "metrics-settings":
            case "metrics-tsdb-settings":
            case "metrics-mappings":
            case "synthetics":
            case "agentless":
            case "synthetics-settings":
            case "synthetics-mappings":
            case ".snapshot-blob-cache":
            case "logstash-index-template":
            case "security-index-template":
            case "data-streams-mappings":
            case "search-acl-filter":
            case ".kibana-reporting":
                return true;
            default:
                return false;
        }
    }

    protected static boolean isXPackIngestPipeline(String id) {
        if (id.matches("(logs|metrics|traces)-apm(\\.\\w+)?@default-pipeline")) {
            return true;
        }
        return switch (id) {
            case "logs-default-pipeline", "logs@default-pipeline", "logs@json-message", "logs@json-pipeline" -> true;
            case "apm@pipeline", "traces-apm@pipeline", "metrics-apm@pipeline" -> true;
            case "behavioral_analytics-events-final_pipeline", "ent-search-generic-ingestion", "search-default-ingestion" -> true;
            case "reindex-data-stream-pipeline" -> true;
            default -> false;
        };
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
        boolean mustHavePRRLs = minimumIndexVersion().onOrAfter(IndexVersions.V_7_6_0);
        assertBusy(() -> {
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            @SuppressWarnings("unchecked")
            Map<String, List<Map<String, ?>>> shards = (Map<String, List<Map<String, ?>>>) XContentMapValues.extractValue(
                "indices." + index + ".shards",
                stats
            );
            for (List<Map<String, ?>> shard : shards.values()) {
                for (Map<String, ?> copy : shard) {
                    Integer globalCheckpoint = (Integer) XContentMapValues.extractValue("seq_no.global_checkpoint", copy);
                    assertThat(XContentMapValues.extractValue("seq_no.max_seq_no", copy), equalTo(globalCheckpoint));
                    assertNotNull(globalCheckpoint);
                    @SuppressWarnings("unchecked")
                    List<Map<String, ?>> retentionLeases = (List<Map<String, ?>>) XContentMapValues.extractValue(
                        "retention_leases.leases",
                        copy
                    );
                    if (mustHavePRRLs == false && retentionLeases == null) {
                        continue;
                    }
                    assertNotNull(retentionLeases);
                    for (Map<String, ?> retentionLease : retentionLeases) {
                        if (((String) retentionLease.get("id")).startsWith("peer_recovery/")) {
                            assertThat(retentionLease.get("retaining_seq_no"), equalTo(globalCheckpoint + 1));
                        }
                    }
                    if (mustHavePRRLs) {
                        List<String> existingLeaseIds = retentionLeases.stream()
                            .map(lease -> (String) lease.get("id"))
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

    protected static Map<String, Set<String>> getClusterStateFeatures(RestClient adminClient) throws IOException {
        final Request request = new Request("GET", "_cluster/state");
        request.addParameter("filter_path", "nodes_features");
        if (multiProjectEnabled) {
            request.addParameter("multi_project", "true");
        }

        final Response response = adminClient.performRequest(request);

        var responseData = responseAsMap(response);
        if (responseData.get("nodes_features") instanceof List<?> nodesFeatures) {
            return nodesFeatures.stream()
                .map(Map.class::cast)
                .collect(Collectors.toUnmodifiableMap(nodeFeatureMap -> nodeFeatureMap.get("node_id").toString(), nodeFeatureMap -> {
                    @SuppressWarnings("unchecked")
                    var nodeFeatures = (List<String>) nodeFeatureMap.get("features");
                    return new HashSet<>(nodeFeatures);
                }));
        }
        return Map.of();
    }

    /**
     * Returns the minimum index version among all nodes of the cluster
     */
    protected static IndexVersion minimumIndexVersion() throws IOException {
        final Request request = new Request("GET", "_nodes");
        request.addParameter("filter_path", "nodes.*.version,nodes.*.max_index_version,nodes.*.index_version");

        final Response response = adminClient().performRequest(request);
        final Map<String, Object> nodes = ObjectPath.createFromResponse(response).evaluate("nodes");

        IndexVersion minVersion = null;
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            Map<?, ?> nodeData = (Map<?, ?>) node.getValue();
            Object versionStr = nodeData.get("index_version");
            if (versionStr == null) {
                versionStr = nodeData.get("max_index_version");
            }
            // fallback on version if index version is not there
            IndexVersion indexVersion = versionStr != null
                ? IndexVersion.fromId(Integer.parseInt(versionStr.toString()))
                : IndexVersion.fromId(
                    parseLegacyVersion((String) nodeData.get("version")).map(Version::id).orElse(IndexVersions.MINIMUM_COMPATIBLE.id())
                );
            if (minVersion == null || minVersion.after(indexVersion)) {
                minVersion = indexVersion;
            }
        }
        assertNotNull(minVersion);
        return minVersion;
    }

    /**
     * Returns the minimum transport version among all nodes of the cluster
     */
    protected static TransportVersion minimumTransportVersion() throws IOException {
        Response response = client.performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");

        TransportVersion minTransportVersion = null;
        for (String id : nodesAsMap.keySet()) {

            var transportVersion = getTransportVersionWithFallback(
                objectPath.evaluate("nodes." + id + ".version"),
                objectPath.evaluate("nodes." + id + ".transport_version"),
                () -> TransportVersions.MINIMUM_COMPATIBLE
            );
            if (minTransportVersion == null || minTransportVersion.after(transportVersion)) {
                minTransportVersion = transportVersion;
            }
        }

        assertNotNull(minTransportVersion);
        return minTransportVersion;
    }

    protected static TransportVersion getTransportVersionWithFallback(
        String versionField,
        Object transportVersionField,
        Supplier<TransportVersion> fallbackSupplier
    ) {
        if (transportVersionField instanceof Number transportVersionId) {
            return TransportVersion.fromId(transportVersionId.intValue());
        } else if (transportVersionField instanceof String transportVersionString) {
            return TransportVersion.fromString(transportVersionString);
        } else { // no transport_version field
            // The response might be from a node <8.8.0, but about a node >=8.8.0
            // In that case the transport_version field won't exist. Use version, but only for <8.8.0: after that versions diverge.
            var version = parseLegacyVersion(versionField);
            assert version.isPresent();
            if (version.get().before(VERSION_INTRODUCING_TRANSPORT_VERSIONS)) {
                return TransportVersion.fromId(version.get().id);
            }
        }
        return fallbackSupplier.get();
    }

    public static Optional<Version> parseLegacyVersion(String version) {
        var semanticVersionMatcher = SEMANTIC_VERSION_PATTERN.matcher(version);
        if (semanticVersionMatcher.matches()) {
            return Optional.of(Version.fromString(semanticVersionMatcher.group(1)));
        }
        return Optional.empty();
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
                XContentType xContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
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
        }, 10, TimeUnit.MINUTES);
    }

    // TODO: replace usages of this with warning_regex or allowed_warnings_regex
    static final Pattern CREATE_INDEX_MULTIPLE_MATCHING_TEMPLATES = Pattern.compile(
        "^index \\[(.+)\\] matches multiple legacy " + "templates \\[(.+)\\], composable templates will only match a single template$"
    );

    static final Pattern PUT_TEMPLATE_MULTIPLE_MATCHING_TEMPLATES = Pattern.compile(
        "^index template \\[(.+)\\] has index patterns "
            + "\\[(.+)\\] matching patterns from existing older templates \\[(.+)\\] with patterns \\((.+)\\); this "
            + "template \\[(.+)\\] will take precedence during new index creation$"
    );

    protected static void useIgnoreMultipleMatchingTemplatesWarningsHandler(Request request) throws IOException {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(warnings -> {
            if (warnings.size() > 0) {
                boolean matches = warnings.stream()
                    .anyMatch(
                        message -> CREATE_INDEX_MULTIPLE_MATCHING_TEMPLATES.matcher(message).matches()
                            || PUT_TEMPLATE_MULTIPLE_MATCHING_TEMPLATES.matcher(message).matches()
                    );
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

    protected FieldCapabilitiesResponse fieldCaps(
        List<String> indices,
        List<String> fields,
        QueryBuilder indexFilter,
        String fieldTypes,
        String fieldFilters
    ) throws IOException {
        return fieldCaps(client(), indices, fields, indexFilter, fieldTypes, fieldFilters);
    }

    protected FieldCapabilitiesResponse fieldCaps(
        RestClient restClient,
        List<String> indices,
        List<String> fields,
        QueryBuilder indexFilter,
        String fieldTypes,
        String fieldFilters
    ) throws IOException {
        Request request = new Request("POST", "/_field_caps");
        request.addParameter("index", String.join(",", indices));
        request.addParameter("fields", String.join(",", fields));
        if (fieldTypes != null) {
            request.addParameter("types", fieldTypes);
        }
        if (fieldFilters != null) {
            request.addParameter("filters", fieldFilters);
        }
        if (indexFilter != null) {
            addXContentBody(request, (body, params) -> body.field("index_filter", indexFilter));
        }
        Response response = restClient.performRequest(request);
        assertOK(response);
        try (XContentParser parser = responseAsParser(response)) {
            return FieldCapsUtils.parseFieldCapsResponse(parser);
        }
    }

    private static boolean isMlEnabled() {
        try {
            adminClient().performRequest(new Request("GET", "_ml/info"));
            return true;
        } catch (IOException e) {
            // do nothing, ML is disabled
            return false;
        }
    }

    public static void setIgnoredErrorResponseCodes(Request request, RestStatus... restStatuses) {
        request.addParameter(
            IGNORE_RESPONSE_CODES_PARAM,
            Arrays.stream(restStatuses).map(restStatus -> Integer.toString(restStatus.getStatus())).collect(Collectors.joining(","))
        );
    }

    private static XContentType randomSupportedContentType() {
        return randomFrom(XContentType.values());
    }

    public static void addXContentBody(Request request, ToXContent body) throws IOException {
        final var xContentType = randomSupportedContentType();
        final var bodyBytes = XContentHelper.toXContent(body, xContentType, EMPTY_PARAMS, randomBoolean());
        request.setEntity(
            new InputStreamEntity(
                bodyBytes.streamInput(),
                bodyBytes.length(),
                ContentType.create(xContentType.mediaTypeWithoutParameters())
            )
        );
    }

    public static Request newXContentRequest(HttpMethod method, String endpoint, ToXContent body) throws IOException {
        final var request = new Request(method.name(), endpoint);
        addXContentBody(request, body);
        return request;
    }

    protected static MapMatcher getProfileMatcher() {
        return matchesMap().entry("query", instanceOf(Map.class))
            .entry("planning", instanceOf(Map.class))
            .entry("drivers", instanceOf(List.class));
    }

    protected static MapMatcher getResultMatcher(boolean includeMetadata, boolean includePartial, boolean includeDocumentsFound) {
        MapMatcher mapMatcher = matchesMap();
        if (includeDocumentsFound) {
            // Older versions may not return documents_found and values_loaded.
            mapMatcher = mapMatcher.entry("documents_found", greaterThanOrEqualTo(0));
            mapMatcher = mapMatcher.entry("values_loaded", greaterThanOrEqualTo(0));
        }
        if (includeMetadata) {
            mapMatcher = mapMatcher.entry("took", greaterThanOrEqualTo(0));
        }
        // Older version may not have is_partial
        if (includePartial) {
            mapMatcher = mapMatcher.entry("is_partial", false);
        }
        return mapMatcher;
    }

    /**
     * Create empty result matcher from result, taking into account all metadata items.
     */
    protected static MapMatcher getResultMatcher(Map<String, Object> result) {
        return getResultMatcher(result.containsKey("took"), result.containsKey("is_partial"), result.containsKey("documents_found"));
    }

    /**
     * Match result columns and values, with default matchers for metadata.
     */
    protected static void assertResultMap(Map<String, Object> result, Matcher<?> columnMatcher, Matcher<?> valuesMatcher) {
        assertMap(result, getResultMatcher(result).entry("columns", columnMatcher).entry("values", valuesMatcher));
    }

    protected static void assertResultMap(Map<String, Object> result, Object columnMatcher, Object valuesMatcher) {
        assertMap(result, getResultMatcher(result).entry("columns", columnMatcher).entry("values", valuesMatcher));
    }

    /**
     * Match result columns and values, with default matchers for metadata.
     */
    protected static void assertResultMap(
        Map<String, Object> result,
        MapMatcher mapMatcher,
        Matcher<?> columnMatcher,
        Matcher<?> valuesMatcher
    ) {
        assertMap(result, mapMatcher.entry("columns", columnMatcher).entry("values", valuesMatcher));
    }

    /**
     * Whether the test framework should configure an active projects and some extra projects. This is true by default (when multi-project
     * is enabled). Subclasses can override this method to avoid configuring projects - e.g. when they configure projects themselves.
     */
    protected boolean shouldConfigureProjects() {
        assert multiProjectEnabled;
        return true;
    }

    private void configureProjects() throws IOException {
        if (projectsConfigured || multiProjectEnabled == false || shouldConfigureProjects() == false) {
            return;
        }
        projectsConfigured = true;
        createProject(activeProject);
        for (var project : extraProjects) {
            createProject(project);
        }

        // The admin client does not set a project id, and can see all projects
        assertProjectIds(
            adminClient(),
            CollectionUtils.concatLists(List.of(Metadata.DEFAULT_PROJECT_ID.id(), activeProject), extraProjects)
        );
        // The test client can only see the project it targets
        assertProjectIds(client(), List.of(activeProject));
    }

    @After
    public final void assertEmptyProjects() throws Exception {
        if (projectsConfigured == false) {
            return;
        }
        assertEmptyProject(Metadata.DEFAULT_PROJECT_ID.id());
        for (var project : extraProjects) {
            assertEmptyProject(project);
        }
    }

    /**
     * If multi-project is enabled, returns the active project ID followed by a slash, which is used to prefix various keys in REST
     * responses. Otherwise, returns the empty string.
     */
    protected String activeProjectPrefix() {
        return multiProjectEnabled ? (activeProject + "/") : "";
    }

    protected void createProject(String project) throws IOException {
        assert multiProjectEnabled;
        final Request request = new Request("PUT", "/_project/" + project);
        try {
            final Response response = adminClient().performRequest(request);
            logger.info("Created project {} : {}", project, response.getStatusLine());
        } catch (ResponseException e) {
            logger.error("Failed to create project: {}", project);
            throw e;
        }
    }

    protected void assertProjectIds(RestClient client, List<String> expectedProjects) throws IOException {
        assert multiProjectEnabled;
        final Collection<String> actualProjects = getProjectIds(client);
        assertThat(
            "Cluster returned project ids: " + actualProjects,
            actualProjects,
            containsInAnyOrder(expectedProjects.toArray(String[]::new))
        );
    }

    protected Collection<String> getProjectIds(RestClient client) throws IOException {
        assert multiProjectEnabled;
        final Request request = new Request("GET", "/_cluster/state/routing_table?multi_project=true");
        try {
            final ObjectPath response = ObjectPath.createFromResponse(client.performRequest(request));
            final List<Map<String, Object>> projectRouting = response.evaluate("routing_table.projects");
            return projectRouting.stream().map(obj -> (String) obj.get("id")).toList();
        } catch (ResponseException e) {
            logger.error("Failed to retrieve cluster state");
            throw e;
        }
    }

    protected void cleanUpProjects() throws IOException {
        assert multiProjectEnabled;
        final var projectIds = getProjectIds(adminClient());
        for (String projectId : projectIds) {
            if (projectId.equals(ProjectId.DEFAULT.id())) {
                continue;
            }
            deleteProject(projectId);
        }
    }

    private void deleteProject(String project) throws IOException {
        assert multiProjectEnabled;
        final Request request = new Request("DELETE", "/_project/" + project);
        cleanupClient().performRequest(request);
    }

    protected void assertEmptyProject(String projectId) throws IOException {
        assert multiProjectEnabled;
        final Request request = new Request("GET", "_cluster/state/metadata,routing_table,customs");
        request.setOptions(request.getOptions().toBuilder().addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId).build());

        var response = responseAsMap(adminClient().performRequest(request));
        ObjectPath state = new ObjectPath(response);

        final var indexNames = ((Map<?, ?>) state.evaluate("metadata.indices")).keySet();
        final var routingTableEntries = ((Map<?, ?>) state.evaluate("routing_table.indices")).keySet();
        if (indexNames.isEmpty() == false || routingTableEntries.isEmpty() == false) {
            // Only the default project is allowed to have the security index after tests complete.
            // The security index could show up in the indices, routing table, or both.
            // If that happens, we need to check that it hasn't been modified by any leaking API calls.
            if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id())
                && (indexNames.isEmpty() || (indexNames.size() == 1 && indexNames.contains(".security-7")))
                && (routingTableEntries.isEmpty() || (routingTableEntries.size() == 1 && routingTableEntries.contains(".security-7")))) {
                checkSecurityIndex();
            } else {
                // If there are any other indices or if this is for a non-default project, we fail the test.
                assertThat("Project [" + projectId + "] should not have indices", indexNames, empty());
                assertThat("Project [" + projectId + "] should not have routing entries", routingTableEntries, empty());
            }
        }
        assertThat(
            "Project [" + projectId + "] should not have graveyard entries",
            state.evaluate("metadata.index-graveyard.tombstones"),
            empty()
        );

        final Map<String, ?> legacyTemplates = state.evaluate("metadata.templates");
        if (legacyTemplates != null) {
            var templateNames = legacyTemplates.keySet().stream().filter(name -> isXPackTemplate(name) == false).toList();
            assertThat("Project [" + projectId + "] should not have legacy templates", templateNames, empty());
        }

        final Map<String, Object> indexTemplates = state.evaluate("metadata.index_template.index_template");
        if (indexTemplates != null) {
            var templateNames = indexTemplates.keySet().stream().filter(name -> isXPackTemplate(name) == false).toList();
            assertThat("Project [" + projectId + "] should not have index templates", templateNames, empty());
        }

        final Map<String, Object> componentTemplates = state.evaluate("metadata.component_template.component_template");
        if (componentTemplates != null) {
            var templateNames = componentTemplates.keySet().stream().filter(name -> isXPackTemplate(name) == false).toList();
            assertThat("Project [" + projectId + "] should not have component templates", templateNames, empty());
        }

        final List<Map<String, ?>> pipelines = state.evaluate("metadata.ingest.pipeline");
        if (pipelines != null) {
            var pipelineNames = pipelines.stream()
                .map(pipeline -> String.valueOf(pipeline.get("id")))
                .filter(id -> isXPackIngestPipeline(id) == false)
                .toList();
            assertThat("Project [" + projectId + "] should not have ingest pipelines", pipelineNames, empty());
        }

        if (has(ProductFeature.ILM)) {
            final Map<String, Object> ilmPolicies = state.evaluate("metadata.index_lifecycle.policies");
            if (ilmPolicies != null) {
                var policyNames = new HashSet<>(ilmPolicies.keySet());
                policyNames.removeAll(preserveILMPolicyIds());
                assertThat("Project [" + projectId + "] should not have ILM Policies", policyNames, empty());
            }
        }
    }

    private void checkSecurityIndex() throws IOException {
        assert multiProjectEnabled;
        final Request request = new Request("GET", "/_security/_query/role");
        request.setJsonEntity("""
            {
              "query": {
                "bool": {
                  "must_not": {
                    "term": {
                      "metadata._reserved": true
                    }
                  }
                }
              }
            }""");
        request.setOptions(
            request.getOptions().toBuilder().addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, Metadata.DEFAULT_PROJECT_ID.id()).build()
        );
        final var response = responseAsMap(adminClient().performRequest(request));
        assertThat("Security index should not contain any non-reserved roles", (Collection<?>) response.get("roles"), empty());
    }

    public static final String FIPS_KEYSTORE_PASSWORD = "keystore-password";

    /**
     * @return a REST {@link Request} which will reload the keystore in the test cluster.
     */
    protected final Request createReloadSecureSettingsRequest() {
        try {
            return newXContentRequest(
                HttpMethod.POST,
                "/_nodes/reload_secure_settings",
                (b, p) -> inFipsJvm() ? b.field("secure_settings_password", FIPS_KEYSTORE_PASSWORD) : b
            );
        } catch (IOException e) {
            throw new AssertionError("impossible", e);
        }
    }
}
