/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Live multi-node demonstration that the data-source encryption pipeline survives cluster-state
 * propagation: a node that joins after the dataset is registered receives the encrypted dataset purely
 * via the cluster-state full-state-on-join path, and when asked to serve a query against it, its
 * {@code StorageProviderRegistry} decrypts the secret at provider construction time.
 *
 * <p>Strategy: start single-node (master + data), register the data source + dataset there (encrypts at
 * rest; no multi-node publication so no incremental-publication assertion), dynamically add a data-only
 * node, then route the ES|QL query through the joined node's client so that node serves as the
 * coordinator for the read. With {@code coordinator_only} distribution the read executes on the joined
 * node, exercising its registry-side decrypt deterministically. A capturing storage-provider factory
 * records the plaintext value it observed per node and the test asserts the joined node saw it.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FileSourceSecretDecryptionAcrossNodesIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String SECRET_KEY = "secret_token";
    private static final String SECRET_VALUE = "S3CR3T_across_nodes";
    private static final String SCHEME = "teststorexn";

    /** node-name -> captured secret value at provider construction. */
    static final Map<String, String> capturedByNode = new ConcurrentHashMap<>();
    /**
     * Tells the gated storage provider what plaintext credential it should see at read time. Set per test
     * before the query runs; the gate compares the value the registry handed the factory against this
     * exact String and rejects any mismatch.
     */
    static volatile String expectedCredentialOverride;

    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    public static final class CapturingDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Set<String> supportedSchemes() {
            return Set.of(SCHEME);
        }

        @Override
        public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
            return Map.of(SCHEME, new CapturingStorageProviderFactory());
        }
    }

    private static final class CapturingStorageProviderFactory implements StorageProviderFactory {
        @Override
        public StorageProvider create(Settings settings) {
            // No-config path: any byte read on the returned provider throws via its credential gate.
            return new CredentialGatedLocalStorageProvider(SCHEME, null, null);
        }

        @Override
        public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
            Object secret = config == null ? null : config.get(SECRET_KEY);
            String node = settings.get("node.name", "unknown");
            if (secret instanceof String s) {
                capturedByNode.put(node, s);
            } else if (secret instanceof EncryptedData) {
                // The whole point of the PR is the registry decrypts at the chokepoint; a data node ever
                // seeing the carrier means that chokepoint was bypassed.
                throw new IllegalStateException("node [" + node + "] received an UNDECRYPTED secret carrier: " + secret);
            }
            return new Configured<>(
                new CredentialGatedLocalStorageProvider(SCHEME, secret, expectedCredentialOverride),
                secret == null ? Set.of() : Set.of(SECRET_KEY)
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(TestEncryptionServicePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        // coordinator_only is deterministic: whichever node receives the query runs the read locally.
        // Combined with sending the query via the joined node's client below, this forces the read to
        // execute on that node, exercising its registry-side decrypt.
        return new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "coordinator_only").build());
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    @Before
    public void resetCapture() {
        capturedByNode.clear();
        expectedCredentialOverride = null;
    }

    // No @After cleanup: TEST scope rebuilds the cluster per method, and deleting the dataset on the now
    // multi-node cluster would trip a pre-existing main assertion in the publication path (the same bug
    // that blocks a static numDataNodes >= 2 form of this test).

    public void testJoinedDataNodeDecryptsTheSecret() throws Exception {
        // Step 1 — record the original cluster bootstrap node (master + data).
        String bootstrapName = internalCluster().getNodeNames()[0];

        // Tell the storage provider exactly what plaintext credential it should see at read time. Reads
        // succeed only when decryption produces this exact value.
        expectedCredentialOverride = SECRET_VALUE;

        Path fixture = createTempFile("secret-fixture-", ".csv");
        Files.writeString(fixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob") + "\n");

        // Step 2 — register the data source (encrypts) and the dataset while single-node, so dataset
        // publication does not trip the incremental-publication assertion that fires on multi-node.
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "secret_src",
                    "test",
                    null,
                    new HashMap<>(Map.of(SECRET_KEY, SECRET_VALUE))
                )
            )
        );
        String uri = SCHEME + "://" + fixture.toAbsolutePath();
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, "secret_ds", "secret_src", uri, null, new HashMap<>(Map.of("format", "csv")))
            )
        );

        // Step 3 — add a data-only node; it bootstraps with the full cluster state in one shot, so it
        // receives the (already-encrypted-on-master) dataset + data source without going through the
        // incremental-publication path that fires the pre-existing assertion on add/remove.
        String joinedNodeName = internalCluster().startDataOnlyNode();
        ensureGreen();

        // Step 4 — route the query through the joined node's client so that node is the coordinator for
        // this query; with coordinator_only distribution the read runs there. End-to-end: the joined node,
        // which got the encrypted dataset purely via cluster state, must run the query to completion. The
        // credential-gated provider would throw on read if decryption did not produce plaintext, so a
        // successful response with the fixture rows is itself proof of end-to-end decryption + read.
        try (
            EsqlQueryResponse response = client(joinedNodeName).execute(
                EsqlQueryAction.INSTANCE,
                syncEsqlQueryRequest("FROM secret_ds | KEEP emp_no, first_name | SORT emp_no | LIMIT 5")
            ).get(30, TimeUnit.SECONDS)
        ) {
            assertThat("expected emp_no + first_name columns from the fixture", response.columns(), hasSize(2));
            assertThat(response.columns().get(0).name(), equalTo("emp_no"));
            assertThat(response.columns().get(1).name(), equalTo("first_name"));
            List<List<Object>> rows = getValuesList(response);
            assertThat("expected the fixture's two rows back", rows, hasSize(2));
            assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(1));
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
            assertThat(((Number) rows.get(1).get(0)).intValue(), equalTo(2));
            assertThat(rows.get(1).get(1).toString(), equalTo("Bob"));
        }

        logger.info("bootstrap=[{}] joined=[{}] capturedByNode={}", bootstrapName, joinedNodeName, capturedByNode);

        assertTrue(
            "expected the joined node [" + joinedNodeName + "] to observe the secret; observed nodes=" + capturedByNode.keySet(),
            capturedByNode.containsKey(joinedNodeName)
        );
        assertEquals(
            "joined node [" + joinedNodeName + "] must receive the decrypted plaintext secret",
            SECRET_VALUE,
            capturedByNode.get(joinedNodeName)
        );
    }
}
