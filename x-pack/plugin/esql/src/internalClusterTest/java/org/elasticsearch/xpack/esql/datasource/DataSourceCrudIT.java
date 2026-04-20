/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.esql.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.dataset.GetDatasetAction;
import org.elasticsearch.xpack.esql.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Integration coverage for the ES|QL data-source + dataset CRUD API: full lifecycle, secret round-trip,
 *  gateway persistence, concurrent-writer races, dispatch-vs-task-execute windows, index/dataset name
 *  collision, and validator rejection. Single-node; the feature flag is enabled via a
 *  {@code systemProperty} on the {@code internalClusterTest} Gradle task. */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false, minNumDataNodes = 1)
public class DataSourceCrudIT extends ESIntegTestCase {

    private static final TimeValue TEST_TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateDataSource.class);
    }

    // Scenario 1 — full lifecycle

    public void testFullLifecycle() throws Exception {
        final String dsName = "prod_test";
        final String datasetName = "access_logs";

        // PUT data source
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));

        // PUT dataset pointing at the data source
        assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(datasetName, dsName, "test://logs/*.parquet", Map.of())));

        // GET data source
        GetDataSourceAction.Response dsResp = client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(dsName)).get();
        assertThat(dsResp.getDataSources(), hasSize(1));
        DataSource ds = dsResp.getDataSources().iterator().next();
        assertThat(ds.name(), equalTo(dsName));
        assertThat(ds.type(), equalTo("test"));
        assertThat(ds.settings(), not(nullValue()));
        assertThat(ds.settings().get("region").nonSecretValue(), equalTo("us-east-1"));

        // GET dataset
        GetDatasetAction.Response dsetResp = client().execute(GetDatasetAction.INSTANCE, getDatasetRequest(datasetName)).get();
        assertThat(dsetResp.getDatasets(), hasSize(1));
        Dataset dataset = dsetResp.getDatasets().iterator().next();
        assertThat(dataset.name(), equalTo(datasetName));
        assertThat(dataset.dataSource().getName(), equalTo(dsName));
        assertThat(dataset.resource(), equalTo("test://logs/*.parquet"));

        // DELETE dataset first (referential integrity requires dataset removed before data source)
        assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(datasetName)));

        // DELETE data source
        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));

        // Confirm both gone — GET should 404 / return empty
        expectDataSourceMissing(dsName);
        expectDatasetMissing(datasetName);
    }

    // Scenario 1b — secret classification round-trips through the wire + cluster state

    public void testSecretClassificationRoundTrip() throws Exception {
        final String dsName = "secret_rt";
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                putDataSourceRequest(dsName, Map.of("region", "us-east-1", "secret_access_key", "AKIAXYZ"))
            )
        );

        GetDataSourceAction.Response resp = client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(dsName)).get();
        assertThat(resp.getDataSources(), hasSize(1));
        DataSource ds = resp.getDataSources().iterator().next();

        DataSourceSetting region = ds.settings().get("region");
        DataSourceSetting secret = ds.settings().get("secret_access_key");

        assertThat("plain setting not marked secret", region.secret(), equalTo(false));
        assertThat("plain setting value accessible", region.nonSecretValue(), equalTo("us-east-1"));

        assertThat("secret-prefixed setting marked secret", secret.secret(), equalTo(true));
        assertThat("secret value must be accessible via secretValue()", secret.secretValue().toString(), equalTo("AKIAXYZ"));

        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    // Scenario 2 — gateway persistence across full restart

    public void testGatewayPersistence() throws Exception {
        final String dsName = "persists_across_restart";
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-west-2"))));

        // Full-cluster restart. GATEWAY-only context ({@code org.elasticsearch.cluster.metadata.DataSourceMetadata.context() =
        // EnumSet.of(GATEWAY)})
        // means the metadata is persisted to disk via the gateway and survives restart.
        internalCluster().fullRestart();
        ensureYellow();

        GetDataSourceAction.Response resp = client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(dsName)).get();
        assertThat("data source should persist across full restart", resp.getDataSources(), hasSize(1));
        assertThat(resp.getDataSources().iterator().next().settings().get("region").nonSecretValue(), equalTo("us-west-2"));

        // Cleanup so subsequent tests start from a clean slate in a SUITE-scoped cluster.
        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    // Scenario 4 — concurrent PUT same data source, two clients

    public void testConcurrentPutSameDataSource() throws Exception {
        final String dsName = "concurrent_same";
        // Two PUTs with different settings. MasterService's single-threaded task executor serializes
        // the submitted tasks, so exactly one winning value lands in cluster state. We assert the
        // state is consistent (one of the two values, not mixed or absent), not which value wins.
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            try {
                startGate.await();
                client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "value_A"))).get();
            } catch (Exception e) {
                logger.error("thread 1 put failed", e);
            } finally {
                doneGate.countDown();
            }
        }, "put-a");

        Thread t2 = new Thread(() -> {
            try {
                startGate.await();
                client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "value_B"))).get();
            } catch (Exception e) {
                logger.error("thread 2 put failed", e);
            } finally {
                doneGate.countDown();
            }
        }, "put-b");

        t1.start();
        t2.start();
        startGate.countDown();
        assertTrue("both PUTs must complete", doneGate.await(30, TimeUnit.SECONDS));

        GetDataSourceAction.Response resp = client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(dsName)).get();
        assertThat(resp.getDataSources(), hasSize(1));
        Object storedRegion = resp.getDataSources().iterator().next().settings().get("region").nonSecretValue();
        assertThat(
            "stored value must be exactly one of the two concurrent writes",
            storedRegion,
            anyOf(equalTo("value_A"), equalTo("value_B"))
        );

        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    // Scenario 5 — DELETE data source racing dataset PUT

    public void testDeleteDataSourceRacingDatasetPut() throws Exception {
        final String dsName = "racing_parent";
        final String datasetName = "racing_child";

        // Seed: data source exists
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));

        // Race two operations from two threads
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(2);
        @SuppressWarnings("unchecked")
        final ActionFuture<AcknowledgedResponse>[] putFuture = new ActionFuture[1];
        @SuppressWarnings("unchecked")
        final ActionFuture<AcknowledgedResponse>[] deleteFuture = new ActionFuture[1];

        Thread puter = new Thread(() -> {
            try {
                startGate.await();
                putFuture[0] = client().execute(
                    PutDatasetAction.INSTANCE,
                    putDatasetRequest(datasetName, dsName, "test://logs/", Map.of())
                );
            } catch (Exception e) {
                logger.error("put thread failed", e);
            } finally {
                doneGate.countDown();
            }
        }, "put-dataset");

        Thread deleter = new Thread(() -> {
            try {
                startGate.await();
                deleteFuture[0] = client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName));
            } catch (Exception e) {
                logger.error("delete thread failed", e);
            } finally {
                doneGate.countDown();
            }
        }, "delete-ds");

        puter.start();
        deleter.start();
        startGate.countDown();
        assertTrue("both operations must return", doneGate.await(30, TimeUnit.SECONDS));

        // Exactly one of two valid end states:
        // a) DELETE wins → PUT dataset gets ResourceNotFoundException at ~DatasetService.java:142
        // b) PUT wins → DELETE gets ElasticsearchStatusException(CONFLICT) at ~DataSourceService.java:184
        boolean putOk = isActionSuccess(putFuture[0]);
        boolean deleteOk = isActionSuccess(deleteFuture[0]);

        if (deleteOk && putOk == false) {
            // (a) Valid: DELETE removed parent, PUT task's re-check threw.
            Throwable err = rootCauseOf(putFuture[0]);
            assertThat(err, instanceOf(ResourceNotFoundException.class));
            assertThat(err.getMessage(), containsString(dsName));
            // parent gone → dataset doesn't exist either
            expectDataSourceMissing(dsName);
            expectDatasetMissing(datasetName);
        } else if (putOk && deleteOk == false) {
            // (b) Valid: dataset landed first, DELETE saw dependent and threw.
            Throwable err = rootCauseOf(deleteFuture[0]);
            assertThat(err, instanceOf(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) err).status(), equalTo(RestStatus.CONFLICT));
            // state consistent: both still exist
            assertThat(client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(dsName)).get().getDataSources(), hasSize(1));
            assertThat(client().execute(GetDatasetAction.INSTANCE, getDatasetRequest(datasetName)).get().getDatasets(), hasSize(1));
            // clean up for subsequent tests
            assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(datasetName)));
            assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
        } else {
            fail("Expected exactly one of PUT or DELETE to succeed; got put=" + putOk + " delete=" + deleteOk);
        }
    }

    // Scenario 6 — dispatch-vs-task-execute race for dataset PUT vs parent data source DELETE

    public void testDispatchVsTaskExecuteRace() throws Exception {
        final String dsName = "dispatch_race_parent";
        final String datasetName = "dispatch_race_child";
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));

        // Block the master's cluster-state task queue with a no-op task that waits on a CyclicBarrier.
        // While the master is blocked, submit the dataset PUT (pre-task dispatch validation reads
        // ClusterService.state() which still shows the parent) and the parent DELETE. Both get queued
        // behind the blocker. Release the barrier; master drains in submission order — we control the
        // order so DELETE runs first, then PUT's task re-checks and throws.
        ClusterService masterCs = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        CyclicBarrier barrier = new CyclicBarrier(2);

        masterCs.submitUnbatchedStateUpdateTask("test-block", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                safeAwait(barrier);
                safeAwait(barrier);
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("blocking task failed", e);
            }
        });
        safeAwait(barrier); // master is now blocked inside the no-op task

        // DELETE parent first, then dataset PUT — order of task submission = order of execution on master.
        ActionFuture<AcknowledgedResponse> deleteFuture = client().execute(
            DeleteDataSourceAction.INSTANCE,
            deleteDataSourceRequest(dsName)
        );
        ActionFuture<AcknowledgedResponse> putFuture = client().execute(
            PutDatasetAction.INSTANCE,
            putDatasetRequest(datasetName, dsName, "test://logs/", Map.of())
        );

        // Give the PUT's pre-task validation (which runs on the transport thread before the CAS task
        // is submitted) a moment to complete against the still-present parent.
        Thread.sleep(100);

        // Release barrier — master now runs DELETE task (succeeds), then PUT's task (re-check sees
        // parent is gone and throws ResourceNotFoundException).
        safeAwait(barrier);

        assertAcked(deleteFuture.get(30, TimeUnit.SECONDS));

        ExecutionException putErr = expectThrows(ExecutionException.class, () -> putFuture.get(30, TimeUnit.SECONDS));
        Throwable rootCause = rootCauseOf(putFuture);
        assertThat(
            "dataset PUT's task-level re-check should fail with ResourceNotFoundException",
            rootCause,
            instanceOf(ResourceNotFoundException.class)
        );
        assertThat(rootCause.getMessage(), containsString(dsName));

        // Final state: parent gone, dataset never landed.
        expectDataSourceMissing(dsName);
        expectDatasetMissing(datasetName);
        assertThat(putErr, notNullValue());
    }

    // Scenario 7 — index creation collides with existing dataset

    public void testIndexCreationCollidesWithDataset() throws Exception {
        final String dsName = "collision_parent";
        final String collidingName = "my_collision";
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));
        assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(collidingName, dsName, "test://logs/", Map.of())));

        // Attempt to create a native ES index with the same name — MetadataCreateIndexService.validateIndexName
        // at line ~314 calls ProjectMetadata.hasDataset() and rejects with InvalidIndexNameException.
        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(TransportCreateIndexAction.TYPE, new CreateIndexRequest(collidingName)).get(30, TimeUnit.SECONDS)
        );
        Throwable rootCause = err.getCause();
        assertThat(rootCause, instanceOf(InvalidIndexNameException.class));
        assertThat(rootCause.getMessage(), containsString("already exists as an ESQL dataset"));

        assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(collidingName)));
        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    // Scenario 8 — validator-level rejection surfaces cleanly through REST + transport + service

    public void testValidatorRejectionSurfacesCleanly() throws Exception {
        final String dsName = "rejected_ds";
        final String datasetName = "rejected_dataset";
        final String parentDsName = "good_parent";

        // Data-source side: validator throws on PUT
        ExecutionException dsErr = expectThrows(
            ExecutionException.class,
            () -> client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of(TestValidator.REJECT_SENTINEL, true)))
                .get()
        );
        assertThat(dsErr.getCause(), instanceOf(ValidationException.class));
        assertThat(dsErr.getCause().getMessage(), containsString(TestValidator.REJECT_SENTINEL));
        // Cluster state is untouched — a rejected PUT must NOT leave a half-written entry.
        expectDataSourceMissing(dsName);

        // Dataset side: seed a valid parent first, then put a dataset whose settings the validator rejects.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(parentDsName, Map.of("region", "us-east-1"))));
        ExecutionException dsetErr = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest(datasetName, parentDsName, "test://logs/", Map.of(TestValidator.REJECT_SENTINEL, true))
            ).get()
        );
        assertThat(dsetErr.getCause(), instanceOf(ValidationException.class));
        assertThat(dsetErr.getCause().getMessage(), containsString(TestValidator.REJECT_SENTINEL));
        expectDatasetMissing(datasetName);

        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(parentDsName)));
    }

    // Request builders + assertions

    private static PutDataSourceAction.Request putDataSourceRequest(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TEST_TIMEOUT, TEST_TIMEOUT, name, "test", null, new HashMap<>(settings));
    }

    private static PutDatasetAction.Request putDatasetRequest(
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings
    ) {
        return new PutDatasetAction.Request(TEST_TIMEOUT, TEST_TIMEOUT, name, dataSource, resource, null, new HashMap<>(settings));
    }

    private static GetDataSourceAction.Request getDataSourceRequest(String name) {
        return new GetDataSourceAction.Request(TEST_TIMEOUT, new String[] { name });
    }

    private static GetDatasetAction.Request getDatasetRequest(String name) {
        GetDatasetAction.Request req = new GetDatasetAction.Request(TEST_TIMEOUT);
        req.indices(name);
        return req;
    }

    private static DeleteDataSourceAction.Request deleteDataSourceRequest(String name) {
        return new DeleteDataSourceAction.Request(TEST_TIMEOUT, TEST_TIMEOUT, name);
    }

    private static DeleteDatasetAction.Request deleteDatasetRequest(String name) {
        return new DeleteDatasetAction.Request(TEST_TIMEOUT, TEST_TIMEOUT, name);
    }

    private void expectDataSourceMissing(String name) throws ExecutionException, InterruptedException {
        GetDataSourceAction.Response resp = client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(name)).get();
        assertThat("expected no data source named [" + name + "]", resp.getDataSources(), hasSize(0));
    }

    private void expectDatasetMissing(String name) {
        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(GetDatasetAction.INSTANCE, getDatasetRequest(name)).get()
        );
        assertThat(err.getCause(), instanceOf(IndexNotFoundException.class));
    }

    private static boolean isActionSuccess(ActionFuture<AcknowledgedResponse> fut) {
        try {
            AcknowledgedResponse resp = fut.get(30, TimeUnit.SECONDS);
            return resp.isAcknowledged();
        } catch (Exception e) {
            return false;
        }
    }

    private static Throwable rootCauseOf(ActionFuture<?> fut) {
        try {
            fut.get(30, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            return cause != null ? cause : ee;
        } catch (Exception other) {
            return other;
        }
    }

    // Test infrastructure — inner classes.

    public static class LocalStateDataSource extends LocalStateCompositeXPackPlugin {

        public LocalStateDataSource(final Settings settings, final Path configPath) throws Exception {
            super(settings, configPath);

            plugins.add(new EsqlPlugin() {
                @Override
                protected XPackLicenseState getLicenseState() {
                    return LocalStateDataSource.this.getLicenseState();
                }

                @Override
                public void loadExtensions(ExtensionLoader loader) {
                    // No-op: keeps the test closed-world; SPI still picks up TestDataSourcePlugin via META-INF/services.
                }
            });
        }
    }

    public static class TestDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
            return Map.of("test", new TestValidator());
        }
    }

    /** Minimal validator: passes settings through, marks {@code secret_*} keys as secret, throws on sentinel key {@code "reject_me"}. */
    static class TestValidator implements DataSourceValidator {
        static final String REJECT_SENTINEL = "reject_me";

        @Override
        public String type() {
            return "test";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            if (datasourceSettings.containsKey(REJECT_SENTINEL)) {
                ValidationException ve = new ValidationException();
                ve.addValidationError("test validator rejected: " + REJECT_SENTINEL + " sentinel present");
                throw ve;
            }
            Map<String, DataSourceSetting> out = new HashMap<>();
            for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
                boolean secret = e.getKey().startsWith("secret_");
                out.put(e.getKey(), new DataSourceSetting(e.getValue(), secret));
            }
            return out;
        }

        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            if (datasetSettings.containsKey(REJECT_SENTINEL)) {
                ValidationException ve = new ValidationException();
                ve.addValidationError("test validator rejected dataset: " + REJECT_SENTINEL + " sentinel present");
                throw ve;
            }
            return new HashMap<>(datasetSettings);
        }
    }

}
