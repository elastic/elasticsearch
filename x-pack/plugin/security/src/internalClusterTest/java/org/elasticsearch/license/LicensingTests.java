/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.apache.http.Header;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsIndices;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.license.LicenseService.LICENSE_EXPIRATION_WARNING_PERIOD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class LicensingTests extends SecurityIntegTestCase {

    private static final SecureString HASH_PASSWD = new SecureString(Hasher.BCRYPT4.hash(new SecureString("passwd".toCharArray())));

    private static final String ROLES =
            SecuritySettingsSource.TEST_ROLE + ":\n" +
                    "  cluster: [ all ]\n" +
                    "  indices:\n" +
                    "    - names: '*'\n" +
                    "      privileges: [manage]\n" +
                    "    - names: '/.*/'\n" +
                    "      privileges: [write]\n" +
                    "    - names: 'test'\n" +
                    "      privileges: [read]\n" +
                    "    - names: 'test1'\n" +
                    "      privileges: [read]\n" +
                    "\n" +
                    "role_a:\n" +
                    "  indices:\n" +
                    "    - names: 'a'\n" +
                    "      privileges: [all]\n" +
                    "    - names: 'test-dls'\n" +
                    "      privileges: [read]\n" +
                    "      query: '{\"term\":{\"field\":\"value\"} }'\n" +
                    "\n" +
                    "role_b:\n" +
                    "  indices:\n" +
                    "    - names: 'b'\n" +
                    "      privileges: [all]\n";

    private static final String USERS_ROLES =
            SecuritySettingsSource.CONFIG_STANDARD_USER_ROLES +
                    "role_a:user_a,user_b\n" +
                    "role_b:user_b\n";

    @Override
    protected String configRoles() {
        return ROLES;
    }

    @Override
    protected String configUsers() {
        return SecuritySettingsSource.CONFIG_STANDARD_USER +
            "user_a:" + HASH_PASSWD + "\n" +
            "user_b:" + HASH_PASSWD + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return USERS_ROLES;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), Netty4Plugin.class); // for http
    }

    @Override
    protected int maxNumberOfNodes() {
        return super.maxNumberOfNodes() + 1;
    }

    @Before
    public void resetLicensing() throws Exception {
        enableLicensing(OperationMode.BASIC);
    }

    @After
    public void cleanupSecurityIndex() {
        deleteSecurityIndex();
    }

    public void testEnableDisableBehaviour() throws Exception {
        IndexResponse indexResponse = index("test", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());


        indexResponse = index("test1", jsonBuilder()
                .startObject()
                .field("name", "value1")
                .endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        refresh();
        final Client client = internalCluster().client();

        disableLicensing();

        assertElasticsearchSecurityException(() -> client.admin().indices().prepareStats().get());
        assertElasticsearchSecurityException(() -> client.admin().cluster().prepareClusterStats().get());
        assertElasticsearchSecurityException(() -> client.admin().cluster().prepareHealth().get());
        assertElasticsearchSecurityException(() -> client.admin().cluster().prepareNodesStats().get());

        enableLicensing(randomFrom(License.OperationMode.values()));

        IndicesStatsResponse indicesStatsResponse = client.admin().indices().prepareStats().get();
        assertNoFailures(indicesStatsResponse);

        ClusterStatsResponse clusterStatsNodeResponse = client.admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsNodeResponse, notNullValue());
        ClusterStatsIndices indices = clusterStatsNodeResponse.getIndicesStats();
        assertThat(indices, notNullValue());
        assertThat(indices.getIndexCount(), greaterThanOrEqualTo(2));

        ClusterHealthResponse clusterIndexHealth = client.admin().cluster().prepareHealth().get();
        assertThat(clusterIndexHealth, notNullValue());

        NodesStatsResponse nodeStats = client.admin().cluster().prepareNodesStats().get();
        assertThat(nodeStats, notNullValue());
    }

    public void testNodeJoinWithoutSecurityExplicitlyEnabled() throws Exception {
        License.OperationMode mode = randomFrom(License.OperationMode.GOLD, License.OperationMode.PLATINUM,
            License.OperationMode.ENTERPRISE, License.OperationMode.STANDARD);
        enableLicensing(mode);

        final List<String> seedHosts = internalCluster().masterClient().admin().cluster().nodesInfo(new NodesInfoRequest()).get()
            .getNodes().stream().map(n -> n.getInfo(TransportInfo.class).getAddress().publishAddress().toString()).distinct()
            .collect(Collectors.toList());

        Path home = createTempDir();
        Path conf = home.resolve("config");
        Files.createDirectories(conf);
        Settings.Builder nodeSettings = Settings.builder()
            .put(nodeSettings(maxNumberOfNodes() - 1, Settings.EMPTY).filter(s -> "xpack.security.enabled".equals(s) == false))
            .put("node.name", "my-test-node")
            .put("network.host", "localhost")
            .put("cluster.name", internalCluster().getClusterName())
            .put("path.home", home)
            .putList(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey())
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey(), seedHosts);

        Collection<Class<? extends Plugin>> mockPlugins = Arrays.asList(LocalStateSecurity.class, MockHttpTransport.TestPlugin.class);
        try (Node node = new MockNode(nodeSettings.build(), mockPlugins)) {
            node.start();
            ensureStableCluster(cluster().size() + 1);
        }
    }

    public void testNoWarningHeaderWhenAuthenticationFailed() throws Exception {
        Request request = new Request("GET", "/_security/user");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
            new SecureString(SecuritySettingsSourceField.TEST_INVALID_PASSWORD.toCharArray())));
        request.setOptions(options);
        License.OperationMode mode = randomFrom(License.OperationMode.GOLD, License.OperationMode.PLATINUM,
            License.OperationMode.ENTERPRISE, License.OperationMode.STANDARD);
        long now = System.currentTimeMillis();
        long newExpirationDate = now + LICENSE_EXPIRATION_WARNING_PERIOD.getMillis() - 1;
        setLicensingExpirationDate(mode, "warning: license will expire soon");
        Header[] headers = null;
        try {
            getRestClient().performRequest(request);
        } catch (ResponseException e) {
            headers = e.getResponse().getHeaders();
            List<String> afterWarningHeaders= getWarningHeaders(e.getResponse().getHeaders());
            assertThat(afterWarningHeaders, Matchers.hasSize(0));
        }
        assertThat(headers != null && headers.length == 3, is(true));
    }

    private static void assertElasticsearchSecurityException(ThrowingRunnable runnable) {
        ElasticsearchSecurityException ee = expectThrows(ElasticsearchSecurityException.class, runnable);
        assertThat(ee.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackField.SECURITY));
        assertThat(ee.status(), is(RestStatus.FORBIDDEN));
    }

    private void disableLicensing() throws Exception {
        // This method first makes sure licensing is enabled everywhere so that we can execute
        // monitoring actions to ensure we have a stable cluster and only then do we disable.
        // This is done in an assertBusy since there is a chance that the enabling of the license
        // is overwritten by some other cluster activity and the node throws an exception while we
        // wait for things to stabilize!
        assertBusy(() -> {
            enableLicensing(OperationMode.BASIC);

            ensureGreen();
            ensureClusterSizeConsistency();
            ensureClusterStateConsistency();

            // apply the disabling of the license once the cluster is stable
            for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
                licenseState.update(OperationMode.BASIC, false, null);
            }
        }, 30L, TimeUnit.SECONDS);
    }

    private void enableLicensing(License.OperationMode operationMode) throws Exception {
        // do this in an await busy since there is a chance that the enabling of the license is
        // overwritten by some other cluster activity and the node throws an exception while we
        // wait for things to stabilize!
        assertBusy(() -> {
            // first update the license so we can execute monitoring actions
            for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
                licenseState.update(operationMode, true, null);
            }

            ensureGreen();
            ensureClusterSizeConsistency();
            ensureClusterStateConsistency();

            // re-apply the update in case any node received an updated cluster state that triggered the license state
            // to change
            for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
                licenseState.update(operationMode, true, null);
            }
        }, 30L, TimeUnit.SECONDS);
    }

    private void setLicensingExpirationDate(License.OperationMode operationMode, String expiryWarning) throws Exception {
        assertBusy(() -> {
            for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
                licenseState.update(operationMode, true, expiryWarning);
            }

            ensureGreen();
            ensureClusterSizeConsistency();
            ensureClusterStateConsistency();
        }, 30L, TimeUnit.SECONDS);
    }

    private List<String> getWarningHeaders(Header[] headers) {
        List<String> warnings = new ArrayList<>();

        for (Header header : headers) {
            if (header.getName().equals("Warning")) {
                warnings.add(header.getValue());
            }
        }

        return warnings;
    }
}
