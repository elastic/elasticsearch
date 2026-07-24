/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.ccq.Clusters.REMOTE_CLUSTER_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Exercises the federation kill switch's remote-dataset gate in {@code EsqlResolveFieldsAction} at its call site,
 * which the pure-unit {@code FederationTests} cannot: that helper test drives {@code Federation.ensureEnabled(boolean)}
 * directly, so deleting the {@code && Federation.isAvailable()} guard from the resolve action would not make it fail.
 *
 * <p>The gate only runs on a remote cluster (the {@code resolveDatasets} option is set only on the request the
 * originating cluster sends to a remote), and to go red on removal the disabled remote must still hold a dataset in
 * its cluster state. A dataset can only be created while federation is enabled (the REST route is gone once the switch
 * is engaged), so this test creates the dataset on the remote while enabled, then bounces the remote with the switch
 * off. Because a remote restart lands on new ports, the remote connection is configured dynamically through the
 * cluster settings API (see {@link Clusters#localClusterForDynamicRemote}) so the seed can be re-pointed after the
 * bounce.
 *
 * <p>With the switch engaged the remote reports no datasets, so {@code FROM <remote>:<dataset>} falls through to normal
 * remote index resolution and fails as a plain missing index, exactly like a nonexistent name. If the gate were
 * removed the still-present dataset would instead surface a {@code RemoteDatasetNotSupportedException} naming it, so
 * the phase-2 assertions go red exactly when the wiring is lost.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationRemoteDatasetGateRestIT extends ESRestTestCase {

    private static final Path DATA_PATH = org.elasticsearch.xpack.esql.CsvTestUtils.createCsvDataDirectory();

    private static final AtomicReference<String> remoteRegisterFederationFeature = new AtomicReference<>("true");

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster(DATA_PATH, emptyMap(), false, remoteRegisterFederationFeature::get);
    static ElasticsearchCluster localCluster = Clusters.localClusterForDynamicRemote(DATA_PATH);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // The remote is restarted mid-test and is ephemeral for this class; skip the shared-cluster wipe, which cannot
        // reach the remote's now-unregistered federation routes to remove the created dataset anyway.
        return true;
    }

    public void testDisabledRemoteResolvesDatasetAsMissingIndex() throws Exception {
        assumeTrue("datasources are only available in snapshot builds", Build.current().isSnapshot());
        // The remote-dataset gate needs the federation kill switch on both nodes: the coordinator sends the
        // resolveDatasets option to the remote, and the remote honors the switch by dropping its datasets. Older nodes
        // predate the feature and do not report the capability, so a mixed cluster correctly skips this scenario.
        List<String> killSwitchCapability = List.of(EsqlCapabilities.Cap.REGISTER_FEDERATION_FEATURE.capabilityName());
        try (RestClient capabilityClient = remoteClusterClient()) {
            assumeTrue(
                "federation kill switch requires the feature on both clusters",
                clusterHasCapability("POST", "/_query", List.of(), killSwitchCapability).orElse(false)
                    && clusterHasCapability(capabilityClient, "POST", "/_query", List.of(), killSwitchCapability).orElse(false)
            );
        }

        final String dataSource = "gate_ds";
        final String dataset = "gate_dataset";
        final String qualified = REMOTE_CLUSTER_NAME + ":" + dataset;
        final String query = "FROM " + qualified;

        // A valid local CSV the dataset points at. The dataset is never actually read (the query fails during
        // resolution), but a real file under the allowlisted path keeps registration from tripping on the resource.
        Path csv = DATA_PATH.resolve("gate.csv");
        Files.writeString(csv, "id\n1\n2\n");
        String resource = csv.toUri().toString();

        configureRemoteConnection(remoteCluster.getTransportEndpoint(0));

        // Phase 1: remote federation enabled. Create the data source + dataset in the remote's cluster state.
        try (RestClient remoteClient = remoteClusterClient()) {
            DatasetRegistry.putDataSource(remoteClient, dataSource, "local", Map.of());
            DatasetRegistry.putDataset(remoteClient, dataset, dataSource, resource, Map.of());
            // Guard against a false green: the dataset must really be in the remote's cluster state, otherwise phase 2
            // would fall through to "Unknown index" even with the gate removed.
            assertThat(datasetNames(remoteClient), hasItem(dataset));
        }

        // While the remote is enabled, FROM <remote>:<dataset> is rejected because datasets are non-remotable: the
        // remote reports it via RemoteDatasetNotSupportedException, naming the dataset. This is the positive control.
        ResponseException enabledError = expectThrows(ResponseException.class, () -> runQuery(query));
        String enabledBody = EntityUtils.toString(enabledError.getResponse().getEntity());
        assertThat(enabledBody, containsString("remote datasets are not supported"));
        assertThat(enabledBody, containsString(dataset));

        // Suppress federation on the remote and bounce it so it re-reads the property, then re-point the seed at the
        // remote's new transport port.
        remoteRegisterFederationFeature.set("false");
        remoteCluster.restart(false);
        configureRemoteConnection(remoteCluster.getTransportEndpoint(0));

        // Phase 2: remote federation disabled. The remote reports no datasets (the gate), so the qualified name falls
        // through to normal remote index resolution and fails as a plain missing index, the same error a nonexistent
        // name gives. Removing the gate would instead surface RemoteDatasetNotSupportedException for the still-present
        // dataset, so both assertions below go red exactly when the wiring is lost.
        ResponseException disabledError = expectThrows(ResponseException.class, () -> runQuery(query));
        String disabledBody = EntityUtils.toString(disabledError.getResponse().getEntity());
        assertThat(disabledBody, containsString("Unknown index [" + qualified + "]"));
        assertThat(disabledBody, not(containsString("remote datasets are not supported")));
    }

    private void runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\":\"" + query + "\"}");
        client().performRequest(request);
    }

    /**
     * Points the {@code remote_cluster} alias at {@code transportEndpoint} through the cluster settings API (sniff
     * mode, {@code skip_unavailable=false} so an unresolved remote name surfaces as a hard error rather than a skipped
     * cluster), then waits until the connection is established. Re-invoked after the remote restart to follow its new
     * port.
     */
    private void configureRemoteConnection(String transportEndpoint) throws Exception {
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity(Strings.format("""
            {
              "persistent": {
                "cluster.remote.%s.mode": "sniff",
                "cluster.remote.%s.seeds": ["%s"],
                "cluster.remote.%s.skip_unavailable": false
              }
            }
            """, REMOTE_CLUSTER_NAME, REMOTE_CLUSTER_NAME, transportEndpoint, REMOTE_CLUSTER_NAME));
        client().performRequest(settings);

        assertBusy(() -> {
            Map<String, Object> info = entityAsMap(client().performRequest(new Request("GET", "/_remote/info")));
            assertThat(info, hasKey(REMOTE_CLUSTER_NAME));
            @SuppressWarnings("unchecked")
            Map<String, Object> remote = (Map<String, Object>) info.get(REMOTE_CLUSTER_NAME);
            assertThat(remote.get("connected"), equalTo(true));
        }, 60, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private static List<String> datasetNames(RestClient client) throws IOException {
        Map<String, Object> body = entityAsMap(client.performRequest(new Request("GET", "/_query/dataset")));
        return ((List<Map<String, Object>>) body.get("datasets")).stream().map(h -> (String) h.get("name")).toList();
    }

    private RestClient remoteClusterClient() throws IOException {
        HttpHost[] remoteHosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
        return buildClient(restClientSettings(), remoteHosts);
    }
}
