/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.test.SecuritySettingsSourceField;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractUpgradeTestCase extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "test_user",
        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD)
    );

    protected static final String UPGRADE_FROM_VERSION = System.getProperty("tests.upgrade_from_version");
    protected static final boolean FIRST_MIXED_ROUND = Booleans.parseBoolean(System.getProperty("tests.first_round", "false"));
    protected static final boolean SKIP_ML_TESTS = Booleans.parseBoolean(System.getProperty("tests.ml.skip", "false"));

    protected static boolean isOriginalCluster(String clusterVersion) {
        return UPGRADE_FROM_VERSION.equals(clusterVersion);
    }

    protected RestClient oldVersionClient = null;
    protected RestClient newVersionClient = null;

    /**
     * Upgrade tests by design are also executed with the same version. We might want to skip some checks if that's the case, see
     * for example gh#39102.
     * @return true if the cluster version is the current version.
     */
    protected static boolean isOriginalClusterCurrent() {
        return UPGRADE_FROM_VERSION.equals(Build.current().version());
    }

    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSearchableSnapshotsIndicesUponCompletion() {
        return true;
    }

    enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType parse(String value) {
            return switch (value) {
                case "old_cluster" -> OLD;
                case "mixed_cluster" -> MIXED;
                case "upgraded_cluster" -> UPGRADED;
                default -> throw new AssertionError("unknown cluster type: " + value);
            };
        }
    }

    protected static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.suite"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)

            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")

            .build();
    }

    protected Collection<String> templatesToWaitFor() {
        return Collections.emptyList();
    }

    @Before
    public void setupForTests() throws Exception {
        final Collection<String> expectedTemplates = templatesToWaitFor();

        if (expectedTemplates.isEmpty()) {
            return;
        }
        assertBusy(() -> {
            final Request catRequest = new Request("GET", "_cat/templates?h=n&s=n");
            final Response catResponse = adminClient().performRequest(catRequest);

            final List<String> templates = Streams.readAllLines(catResponse.getEntity().getContent());

            final List<String> missingTemplates = expectedTemplates.stream()
                .filter(each -> templates.contains(each) == false)
                .collect(Collectors.toList());

            // While it's possible to use a Hamcrest matcher for this, the failure is much less legible.
            if (missingTemplates.isEmpty() == false) {
                fail("Some expected templates are missing: " + missingTemplates + ". The templates that exist are: " + templates + "");
            }
        });
    }

    protected static void waitForSecurityMigrationCompletion(RestClient adminClient, int version) throws Exception {
        final Request request = new Request("GET", "_cluster/state/metadata/.security-7");
        assertBusy(() -> {
            Map<String, Object> indices = new XContentTestUtils.JsonMapView(entityAsMap(adminClient.performRequest(request))).get(
                "metadata.indices"
            );
            assertNotNull(indices);
            assertTrue(indices.containsKey(".security-7"));
            // JsonMapView doesn't support . prefixed indices (splits on .)
            @SuppressWarnings("unchecked")
            String responseVersion = new XContentTestUtils.JsonMapView((Map<String, Object>) indices.get(".security-7")).get(
                "migration_version.version"
            );
            assertNotNull(responseVersion);
            assertTrue(Integer.parseInt(responseVersion) >= version);
        });
    }

    protected void closeClientsByVersion() throws IOException {
        if (oldVersionClient != null) {
            oldVersionClient.close();
            oldVersionClient = null;
        }
        if (newVersionClient != null) {
            newVersionClient.close();
            newVersionClient = null;
        }
    }

    @SuppressWarnings("unchecked")
    protected Map<String, String> getRestEndpointByIdNodeId() throws IOException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        assertOK(response);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        return nodesAsMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            Map<String, Object> nodeDetails = (Map<String, Object>) e.getValue();
            Map<String, Object> httpInfo = (Map<String, Object>) nodeDetails.get("http");
            return (String) httpInfo.get("publish_address");
        }));
    }

    protected void createClientsByCapability(Predicate<TestNodeInfo> capabilityChecker) throws IOException {
        var testNodesByCapability = collectNodeInfos(adminClient()).stream().collect(Collectors.partitioningBy(capabilityChecker));
        if (testNodesByCapability.size() == 2) {
            oldVersionClient = buildClient(
                restClientSettings(),
                new HttpHost[] { HttpHost.create(testNodesByCapability.get(false).getFirst().restEndpoint) }
            );
            newVersionClient = buildClient(
                restClientSettings(),
                new HttpHost[] { HttpHost.create(testNodesByCapability.get(true).getFirst().restEndpoint) }
            );
            assertThat(oldVersionClient, notNullValue());
            assertThat(newVersionClient, notNullValue());
        } else {
            fail("expected 2 versions during rolling upgrade but got: " + testNodesByCapability.size());
        }
    }

    protected Set<TestNodeInfo> collectNodeInfos(RestClient adminClient) throws IOException {
        final Request request = new Request("GET", "_cluster/state");
        request.addParameter("filter_path", "nodes_features");

        final Response response = adminClient.performRequest(request);

        final Map<String, Set<String>> nodeFeatures;
        var responseData = responseAsMap(response);
        if (responseData.get("nodes_features") instanceof List<?> nodesFeatures) {
            nodeFeatures = nodesFeatures.stream()
                .map(Map.class::cast)
                .collect(Collectors.toUnmodifiableMap(nodeFeatureMap -> nodeFeatureMap.get("node_id").toString(), nodeFeatureMap -> {
                    @SuppressWarnings("unchecked")
                    var features = (List<String>) nodeFeatureMap.get("features");
                    return new HashSet<>(features);
                }));
        } else {
            nodeFeatures = Map.of();
        }
        var restEndpointByNodeId = getRestEndpointByIdNodeId();

        return nodeInfoById().entrySet().stream().map(entry -> {
            var version = (String) extractValue((Map<?, ?>) entry.getValue(), "version");
            assertNotNull(version);
            var transportVersion = (Integer) extractValue((Map<?, ?>) entry.getValue(), "transport_version");
            assertNotNull(transportVersion);
            return new TestNodeInfo(
                entry.getKey(),
                version,
                TransportVersion.fromId(transportVersion),
                nodeFeatures.getOrDefault(entry.getKey(), Set.of()),
                restEndpointByNodeId.get(entry.getKey())
            );
        }).collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nodeInfoById() throws IOException {
        final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, "_nodes/_all"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        final Map<String, Object> nodes = (Map<String, Object>) extractValue(responseAsMap(response), "nodes");
        assertNotNull("Nodes info is null", nodes);
        return nodes;
    }

    protected record TestNodeInfo(
        String nodeId,
        String version,
        TransportVersion transportVersion,
        Set<String> features,
        String restEndpoint
    ) {
        public boolean isOriginalVersionCluster() {
            return AbstractUpgradeTestCase.isOriginalCluster(this.version());
        }

        public boolean isUpgradedVersionCluster() {
            return false == isOriginalVersionCluster();
        }

        public boolean supportsFeature(String feature) {
            return features().contains(feature);
        }
    }

}
