/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

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

/**
 * Base class for x-pack rolling upgrade tests running against the parameterized rolling upgrade
 * cluster harness ({@link ParameterizedRollingUpgradeTestCase}). It replaces the previous
 * {@code AbstractUpgradeTestCase}, which relied on Gradle chaining separate old/mixed/upgraded
 * cluster test tasks and reading the cluster state from system properties.
 *
 * <p>TODO: {@code UpgradeClusterClientYamlTestSuiteIT} was dropped during the migration to this
 * class. It only ran YAML REST tests against the fully upgraded cluster and would require
 * significant work to adapt {@link org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase} to
 * the {@link ParameterizedRollingUpgradeTestCase} cluster lifecycle. See the migration task
 * description for details.
 */
public abstract class AbstractXPackRollingUpgradeTestCase extends ParameterizedRollingUpgradeTestCase {

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = buildCluster();

    private static ElasticsearchCluster buildCluster() {
        return XPackRollingUpgradeClusterConfig.buildCluster(getOldClusterVersion(), isOldClusterDetachedVersion(), repoDirectory);
    }

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    protected RestClient oldVersionClient = null;
    protected RestClient newVersionClient = null;

    protected static boolean isOriginalCluster(String clusterVersion) {
        return getOldClusterVersion().equals(clusterVersion);
    }

    /**
     * Upgrade tests by design are also executed with the same version. We might want to skip some checks if that's the case, see
     * for example gh#39102.
     * @return true if the cluster version is the current version.
     */
    protected static boolean isOriginalClusterCurrent() {
        return getOldClusterVersion().equals(Build.current().version());
    }

    protected AbstractXPackRollingUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(super.restClientSettings())
            .put(
                ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password".toCharArray()))
            )
            .build();
    }

    /**
     * ML tests may need to be skipped on hosts where the old cluster version and the installed glibc are incompatible,
     * see {@code BwcVersions#isMlCompatible} in the build logic. The build wires this into the {@code tests.ml.skip}
     * system property per BWC version.
     */
    protected static boolean skipMlTests() {
        return Booleans.parseBoolean(System.getProperty("tests.ml.skip", "false"));
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
        Map<Boolean, List<TestNodeInfo>> testNodesByCapability = collectNodeInfos(adminClient()).stream()
            .collect(Collectors.partitioningBy(capabilityChecker));
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
        Map<String, Object> responseData = responseAsMap(response);
        if (responseData.get("nodes_features") instanceof List<?> nodesFeatures) {
            nodeFeatures = nodesFeatures.stream()
                .map(Map.class::cast)
                .collect(Collectors.toUnmodifiableMap(nodeFeatureMap -> nodeFeatureMap.get("node_id").toString(), nodeFeatureMap -> {
                    @SuppressWarnings("unchecked")
                    List<String> features = (List<String>) nodeFeatureMap.get("features");
                    return new HashSet<>(features);
                }));
        } else {
            nodeFeatures = Map.of();
        }
        Map<String, String> restEndpointByNodeId = getRestEndpointByIdNodeId();

        return nodeInfoById().entrySet().stream().map(entry -> {
            String version = (String) extractValue((Map<?, ?>) entry.getValue(), "version");
            assertNotNull(version);
            Integer transportVersion = (Integer) extractValue((Map<?, ?>) entry.getValue(), "transport_version");
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
            return getOldClusterVersion().equals(this.version());
        }

        public boolean isUpgradedVersionCluster() {
            return false == isOriginalVersionCluster();
        }

        public boolean supportsFeature(String feature) {
            return features().contains(feature);
        }
    }
}
