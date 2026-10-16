/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.encryption.EncryptionResetRequest;
import org.elasticsearch.xpack.encryption.TransportEncryptionResetAction;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Reproduces the serverless scale-up failure end to end: an ES|QL query over an external data source fails with
 * {@code encryption_key_not_yet_available_exception} because the coordinator sent splits to a worker that has not yet applied the
 * cluster state carrying the key. No stubs: the real {@code EncryptionPlugin} key cache, a worker whose cluster-applier thread is
 * parked, and a real query.
 *
 * <p>Shape, after {@code DataSourcePutStaleCurrentIT}: a dedicated master is the only voter, so parking a data-only node cannot steal
 * quorum, and {@code cluster.publish.timeout} is short because the master applies a state only once its publication ends, which the
 * parked node delays. Sequence: every node holds the first key; one worker's applier is parked with {@link BlockClusterStateProcessing};
 * a key change is forced through the public reset action, after which the coordinator installs a fresh key; the data source is created
 * after that, so its secret is encrypted under the new key the parked worker has never seen; a round-robin query runs from another
 * data node. The coordinator picks the parked worker (it is a data-capable, non-index node in the coordinator's state), the worker
 * cannot decrypt the credentials, and the query fails with the production exception naming the new key. Once the worker is released
 * and catches up, the same query returns every row, which pins the lag as the only cause.
 *
 * <p>Waits are on the active key id in each node's applied cluster state, as {@code KeyRotationIT} does; the metadata class is
 * package-private to the encryption plugin, so it is read through the custom's API rendering (what {@code GET _cluster/state} shows).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ExternalSplitOnLaggingWorkerIT extends AbstractExternalDataSourceIT {

    private static final String PASSWORD_ID = "v1";
    private static final String PASSWORD = "encryption-test-password";
    /** Short, so requests do not wait out the parked node's missing ack; the master still applies after the publish timeout. */
    private static final TimeValue SHORT_ACK = TimeValue.timeValueSeconds(1);
    private static final String KEY_SERVICE_LOGGER = "org.elasticsearch.xpack.encryption.ProjectEncryptionKeyService";
    private static final String KEY_NOT_YET_AVAILABLE = "encryption_key_not_yet_available_exception";

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

    public void testQueryFailsWhenASplitLandsOnAWorkerThatHasNotAppliedTheKeyYet() throws Exception {
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
        Path dir = createTempDir().resolve("lagging-worker");
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
        QueryPragmas roundRobin = new QueryPragmas(
            Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()
        );

        BlockClusterStateProcessing hold = new BlockClusterStateProcessing(parked, random());
        internalCluster().setDisruptionScheme(hold);
        hold.startDisrupting();
        final String newKey;
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
            newKey = activeKeyIdOn(master);
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
                    "lagging_ds",
                    "secret_src",
                    StoragePath.fileUri(dir) + "/*.parquet",
                    null,
                    new HashMap<>()
                )
            ).actionGet(TIMEOUT);

            // The dataset reaches the coordinator on its own commit, independently of the parked node, so poll until the query gets
            // far enough to fail for the expected reason. The worker's own key service reports the miss: the new key is not in the
            // cluster state it has applied.
            MockLog.assertThatLogger(() -> {
                try {
                    assertBusy(() -> {
                        Exception failure = expectThrows(Exception.class, () -> {
                            try (EsqlQueryResponse response = runQuery(coordinator, roundRobin)) {
                                fail("expected the query to fail, got " + getValuesList(response));
                            }
                        });
                        if (keyNotYetAvailable(failure) == false) {
                            throw new AssertionError("query failed, but not with the key miss yet: " + failure, failure);
                        }
                        assertThat(ExceptionsHelper.status(failure), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                        assertThat(failure.getMessage(), containsString("project encryption key [" + newKey + "] is not yet available"));
                    }, 30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            },
                KEY_SERVICE_LOGGER,
                new MockLog.SeenEventExpectation(
                    "the parked worker's key service reports the miss",
                    KEY_SERVICE_LOGGER,
                    Level.WARN,
                    "project encryption key [" + newKey + "] is not in cluster state (active key is [" + firstKey + "])"
                )
            );
            assertThat("the parked worker is still behind", activeKeyIdOn(parked), equalTo(firstKey));
        } finally {
            hold.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }

        // Released, the worker catches up, and the very same query returns every row: the lag was the only cause.
        assertBusy(() -> assertThat(activeKeyIdOn(parked), equalTo(newKey)), 30, TimeUnit.SECONDS);
        try (EsqlQueryResponse response = runQuery(coordinator, roundRobin)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo((long) fileCount));
            assertThat(rows.get(0).get(1), equalTo((long) fileCount * (fileCount - 1) / 2));
        }
    }

    private static EsqlQueryResponse runQuery(String coordinator, QueryPragmas pragmas) {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM lagging_ds | STATS c = COUNT(*), s = SUM(value)").pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        return client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT);
    }

    /**
     * Whether the failure, or anything in its cause chain, is the key miss. The SPI type is not a registered exception, so after the
     * node boundary it arrives as a {@link NotSerializableExceptionWrapper} carrying the original name, exactly as in production.
     */
    private static boolean keyNotYetAvailable(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = failure; t != null && seen.add(t); t = t.getCause()) {
            if (t instanceof EncryptionKeyNotYetAvailableException) {
                return true;
            }
            if (t instanceof NotSerializableExceptionWrapper wrapper && KEY_NOT_YET_AVAILABLE.equals(wrapper.getExceptionName())) {
                return true;
            }
        }
        return false;
    }

    /** Type string of the project-encryption-key custom; the class itself is package-private to the encryption plugin. */
    private static final String PEK_CUSTOM_TYPE = "primary_encryption_key";

    /**
     * The active key id in this node's applied cluster state; {@code null} while no key is installed (before the first install, or
     * between a reset and the reinstall), so callers can poll with assertBusy.
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
