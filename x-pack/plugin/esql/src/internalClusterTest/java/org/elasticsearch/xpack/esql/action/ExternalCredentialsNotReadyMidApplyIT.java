/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.encryption.EncryptionResetRequest;
import org.elasticsearch.xpack.encryption.TransportEncryptionResetAction;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * The real thing, end to end: a worker that is still applying cluster state misses the key the coordinator used, declines, and the
 * coordinator moves its splits to another worker. No stubs. The key cache is the real {@code EncryptionPlugin} one, the miss is a
 * genuine {@code EncryptionKeyNotYetAvailableException} from a node whose applier thread is parked, and the query still returns every row.
 *
 * <p>Shape, after {@code DataSourcePutStaleCurrentIT}: a dedicated master is the only voter, so parking a data-only node cannot steal
 * quorum, and {@code cluster.publish.timeout} is short because the master applies a state only once its publication ends, which the
 * parked node delays. The sequence is: park one worker with {@link BlockClusterStateProcessing}; force a key change through the public
 * reset action (the coordinator reinstalls a fresh key); create the data source after that, so its secret is encrypted under the new
 * key that the parked worker has never seen; run a round-robin query from another data node. The parked worker is asked, cannot decrypt,
 * declines before reading anything, and the coordinator's reroute log line shows where the splits went.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ExternalCredentialsNotReadyMidApplyIT extends AbstractExternalDataSourceIT {

    private static final String PASSWORD_ID = "v1";
    private static final String PASSWORD = "encryption-test-password";
    /** Short, so requests do not wait out the parked node's missing ack; the master still applies after the publish timeout. */
    private static final TimeValue SHORT_ACK = TimeValue.timeValueSeconds(1);
    private static final String REROUTE_LOGGER = "org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // The real plugin replaces the base class's identity stub (a node binds EncryptionService once). It goes first so each node's
        // EsqlPlugin reads this node's service from the static registry during construction.
        plugins.remove(TestEncryptionServicePlugin.class);
        plugins.add(0, EncryptionPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString("cluster.state.encryption.active_password_id", PASSWORD_ID);
        secure.setString("cluster.state.encryption.password." + PASSWORD_ID, PASSWORD);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(2))
            .setSecureSettings(secure)
            .build();
    }

    @TestLogging(value = REROUTE_LOGGER + ":DEBUG", reason = "the reroute is only announced at DEBUG; the test asserts it happened")
    public void testParkedWorkerMissesTheNewKeyAndItsSplitsMoveToAnotherWorker() throws Exception {
        internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        ensureStableCluster(4);
        String master = internalCluster().getMasterName();
        String parked = dataNodes.get(0);
        String coordinator = dataNodes.get(1);

        // The first key is installed asynchronously after election; every node must hold it before anything is parked.
        assertBusy(() -> assertThat(activeKeyIdOn(master), notNullValue()), 30, TimeUnit.SECONDS);
        String firstKey = activeKeyIdOn(master);
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertThat("first key on " + node, activeKeyIdOn(node), equalTo(firstKey));
            }
        }, 30, TimeUnit.SECONDS);

        int fileCount = 6;
        Path dir = createTempDir().resolve("mid-apply");
        Files.createDirectories(dir);
        for (int i = 0; i < fileCount; i++) {
            int value = i;
            writeParquet(
                dir.resolve("f" + i + ".parquet"),
                "message test { required int32 value; }",
                1,
                1024,
                (g, r) -> g.add("value", value)
            );
        }

        BlockClusterStateProcessing hold = new BlockClusterStateProcessing(parked, random());
        internalCluster().setDisruptionScheme(hold);
        hold.startDisrupting();
        try {
            // Force a key change. Reset removes the key; the coordinator on the master installs a fresh one on the next state. Both
            // publications end at the publish timeout because the parked node cannot ack, and only then does the master apply them.
            AcknowledgedResponse reset = client(master).execute(
                TransportEncryptionResetAction.TYPE,
                new EncryptionResetRequest(TIMEOUT, SHORT_ACK, true)
            ).actionGet(TIMEOUT);
            assertThat("the parked node cannot ack", reset.isAcknowledged(), equalTo(false));
            assertBusy(() -> {
                assertThat("master has an active key again", activeKeyIdOn(master), notNullValue());
                assertThat("master has a new active key", activeKeyIdOn(master), not(equalTo(firstKey)));
                assertThat("coordinator applied the new key", activeKeyIdOn(coordinator), equalTo(activeKeyIdOn(master)));
            }, 30, TimeUnit.SECONDS);
            String newKey = activeKeyIdOn(master);
            assertThat("the parked worker still only has the first key", activeKeyIdOn(parked), equalTo(firstKey));

            // Created now, on the master, so the secret is encrypted under the new key the parked worker has never seen.
            client(master).execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(
                    TIMEOUT,
                    SHORT_ACK,
                    "secret_src",
                    "test",
                    null,
                    new HashMap<>(Map.of("secret_token", "S3CR3T-" + randomAlphaOfLength(6)))
                )
            ).actionGet(TIMEOUT);
            client(master).execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    SHORT_ACK,
                    "mid_apply_ds",
                    "secret_src",
                    StoragePath.fileUri(dir) + "/*.parquet",
                    null,
                    new HashMap<>()
                )
            ).actionGet(TIMEOUT);

            QueryPragmas pragmas = new QueryPragmas(
                Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()
            );
            // The dataset reaches the coordinator on its own commit, independently of the parked node; poll for it.
            assertBusy(() -> {
                MockLog.assertThatLogger(() -> {
                    EsqlQueryRequest request = syncEsqlQueryRequest("FROM mid_apply_ds | STATS c = COUNT(*), s = SUM(value)").pragmas(
                        pragmas
                    );
                    request.acceptedPragmaRisks(true);
                    request.profile(true);
                    try (EsqlQueryResponse response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT)) {
                        List<List<Object>> rows = getValuesList(response);
                        assertThat(rows, hasSize(1));
                        assertThat("every file is read exactly once", rows.get(0).get(0), equalTo((long) fileCount));
                        assertThat(rows.get(0).get(1), equalTo((long) fileCount * (fileCount - 1) / 2));
                        Set<String> scanNodes = externalScanNodeNames(response);
                        assertThat("the parked worker ran no scan", scanNodes, not(hasItem(parked)));
                    }
                },
                    REROUTE_LOGGER,
                    new MockLog.SeenEventExpectation(
                        "splits rerouted away from the parked worker",
                        REROUTE_LOGGER,
                        Level.DEBUG,
                        "external worker [" + parked + "] cannot decrypt the data source credentials yet; sending its [*] splits to [*]"
                    )
                );
            }, 30, TimeUnit.SECONDS);
            assertThat("the parked worker is still behind", activeKeyIdOn(parked), equalTo(firstKey));
            assertThat(newKey, not(equalTo(firstKey)));
        } finally {
            hold.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
        // Released, the worker catches up and holds the new key like everyone else.
        assertBusy(() -> assertThat(activeKeyIdOn(parked), equalTo(activeKeyIdOn(master))), 30, TimeUnit.SECONDS);
    }

    /** Type string of the project-encryption-key custom; the class itself is package-private to the encryption plugin. */
    private static final String PEK_CUSTOM_TYPE = "primary_encryption_key";

    /**
     * The active key id in this node's applied cluster state, the same way {@code KeyRotationIT} waits on it, read here through the
     * custom's API rendering (what {@code GET _cluster/state} shows) because the metadata class is not visible from this package.
     * {@code null} while no key is installed: before the first install, or between a reset and the reinstall.
     */
    private static String activeKeyIdOn(String node) throws IOException {
        Metadata.ProjectCustom pek = internalCluster().clusterService(node).state().metadata().getSingleProjectCustom(PEK_CUSTOM_TYPE);
        if (pek == null) {
            return null;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            ChunkedToXContent.wrapAsToXContent(pek)
                .toXContent(builder, new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API)));
            builder.endObject();
            Object id = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2().get("active_key_id");
            return id == null || id.toString().isEmpty() ? null : id.toString();
        }
    }
}
