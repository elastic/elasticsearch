/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.GetDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * In-JVM IT for data source + dataset CRUD. Covers scenarios that need direct ClusterService
 * access — gateway persistence across restart, CyclicBarrier-coordinated races, delete-racing-put
 * ordering — which are not expressible at the REST layer. REST-level coverage (HTTP status codes,
 * secret masking on the wire, validator error shape) lives in {@code DataSourceCrudRestIT} in the
 * {@code x-pack:plugin:esql:qa:server:single-node} project.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false, minNumDataNodes = 1)
public class DataSourceCrudIT extends ESIntegTestCase {

    private static final TimeValue TEST_TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestEncryptionServicePlugin.class, LocalStateDataSource.class);
    }

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
        assertThat("secret value must be stored as an encrypted carrier", secret.rawValue(), instanceOf(EncryptedData.class));
        EncryptedData carrier = (EncryptedData) secret.rawValue();

        // E2E round-trip through DataSourceCredentials.decryptInPlace — the connector-boundary decryption step.
        // Proves: PUT encrypts → cluster state holds an EncryptedData carrier → projection forwards it by
        // reference → consumer decrypts back to the canary. Forwarding the carrier as-is is exactly what
        // DatasetRewriter.mergeSettings produces for an encrypted secret.
        DataSourceCredentials credentials = new DataSourceCredentials(new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                return new EncryptedData(TestEncryptionServicePlugin.TEST_KEY_ID, bytes);
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                return encryptedData.payload();
            }
        });
        Map<String, Object> connectorInput = new HashMap<>();
        connectorInput.put("region", "us-east-1");
        connectorInput.put("secret_access_key", carrier);
        Map<String, Object> decrypted = credentials.decryptInPlace(connectorInput);
        assertThat("decryptInPlace passes non-secrets through", decrypted.get("region"), equalTo("us-east-1"));
        assertThat("decryptInPlace materialises the plaintext canary", decrypted.get("secret_access_key"), equalTo("AKIAXYZ"));

        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    /**
     * Regression: a PUT that omits an already-stored secret (as Kibana's edit-and-save flow does, since the
     * corresponding GET masks it) must carry the secret forward rather than wiping it.
     */
    public void testPutOmittingSecretPreservesExistingSecret() throws Exception {
        final String dsName = "omit_secret_ds";
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                putDataSourceRequest(dsName, Map.of("region", "us-east-1", "secret_access_key", "AKIAXYZ"))
            )
        );

        // Update omitting the secret entirely; only region changes.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-west-2"))));

        DataSource after = client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(dsName))
            .get()
            .getDataSources()
            .iterator()
            .next();
        assertThat(after.settings().get("region").nonSecretValue(), equalTo("us-west-2"));
        DataSourceSetting secretAfter = after.settings().get("secret_access_key");
        assertNotNull("secret omitted from an update must be carried forward, not wiped", secretAfter);
        assertTrue("carried-forward secret must remain encrypted", secretAfter.isEncrypted());
        assertThat("carried-forward secret must decrypt to the original value", decryptSecret(secretAfter), equalTo("AKIAXYZ"));

        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    /**
     * Counterpart to {@link #testPutOmittingSecretPreservesExistingSecret}: an explicit JSON {@code null}
     * clears a secret, rather than carrying the old value forward.
     */
    public void testPutExplicitNullClearsSecret() throws Exception {
        final String dsName = "null_clear_ds";
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                putDataSourceRequest(dsName, Map.of("region", "us-east-1", "secret_access_key", "AKIAXYZ"))
            )
        );

        Map<String, Object> clearing = new HashMap<>();
        clearing.put("region", "us-east-1");
        clearing.put("secret_access_key", null);
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, clearing)));

        DataSource after = client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(dsName))
            .get()
            .getDataSources()
            .iterator()
            .next();
        DataSourceSetting secretAfter = after.settings().get("secret_access_key");
        assertTrue(
            "an explicit null must clear the secret rather than carry it forward",
            secretAfter == null || secretAfter.rawValue() == null
        );

        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    /**
     * Regression: an explicitly-supplied secret value must stand on its own and satisfy completeness on its
     * own merits, not inherit "still there" credit from the stored value it's replacing just because some
     * other required secret is genuinely omitted on the same request. Uses
     * {@link RequiredSecretTestValidator}, which requires {@code secret_access_key} either in the request or
     * carried forward, closely mirroring a real CSP's {@code hasCredentials()}.
     */
    public void testPutWithBlankSecretDoesNotInheritCarryForwardCredit() throws Exception {
        final String dsName = "blank_secret_ds";
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                requiredSecretDataSourceRequest(dsName, Map.of("region", "us-east-1", "secret_access_key", "AKIAXYZ"))
            )
        );

        // An empty value must fail validation on its own, not pass by inheriting the credit the omitted case
        // would get.
        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                PutDataSourceAction.INSTANCE,
                requiredSecretDataSourceRequest(dsName, Map.of("region", "us-west-2", "secret_access_key", ""))
            ).get()
        );
        assertThat(err.getCause(), instanceOf(ValidationException.class));

        // The rejected PUT must not have touched the real secret.
        DataSource after = client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(dsName))
            .get()
            .getDataSources()
            .iterator()
            .next();
        assertThat(after.settings().get("region").nonSecretValue(), equalTo("us-east-1"));
        assertThat(decryptSecret(after.settings().get("secret_access_key")), equalTo("AKIAXYZ"));

        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    /**
     * Regression: {@code DataSourceService.putDataSource} must re-validate against the authoritative state it
     * reads inside the cluster-state-update task, not just the pre-encryption snapshot taken before submitting
     * it. Races two PUTs behind a blocked master task queue (mirrors {@link #testDispatchVsTaskExecuteRace}):
     * both are coordinator-pre-validated against the same state (secret still present), but the one that
     * clears the secret is submitted first, so by the time the second PUT's task actually runs, the secret it
     * was relying on to carry forward is gone. That PUT must fail, not silently persist an incomplete data
     * source.
     */
    public void testPutRevalidatesCarriedForwardSecretAgainstAuthoritativeState() throws Exception {
        final String dsName = "race_required_secret";
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                requiredSecretDataSourceRequest(dsName, Map.of("region", "us-east-1", "secret_access_key", "AKIAXYZ"))
            )
        );

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

        // Both requests are coordinator-pre-validated against the same pre-block state, where the data source
        // (and its secret) still exists, so the PUT's pre-check passes. Submission order controls processing
        // order once the barrier releases: the delete runs first, so by the time the PUT's task actually runs,
        // the entry it was relying on to carry the secret forward from is already gone.
        ActionFuture<AcknowledgedResponse> deleteFuture = client().execute(
            DeleteDataSourceAction.INSTANCE,
            deleteDataSourceRequest(dsName)
        );
        ActionFuture<AcknowledgedResponse> omitFuture = client().execute(
            PutDataSourceAction.INSTANCE,
            requiredSecretDataSourceRequest(dsName, Map.of("region", "us-west-2"))
        );

        safeAwait(barrier); // release; master processes the delete, then the PUT

        assertAcked(deleteFuture.get(30, TimeUnit.SECONDS));

        ExecutionException err = expectThrows(ExecutionException.class, () -> omitFuture.get(30, TimeUnit.SECONDS));
        assertThat(err.getCause(), instanceOf(ValidationException.class));
        assertThat(err.getCause().getMessage(), containsString("secret_access_key is required"));

        // The PUT must not have silently created an incomplete data source in place of the deleted one.
        expectDataSourceMissing(dsName);
    }

    public void testGatewayPersistence() throws Exception {
        final String dsName = "persists_across_restart";
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-west-2"))));

        // Full-cluster restart. DataSourceMetadata.context() is GATEWAY-only, so the metadata is persisted to disk and survives restart.
        internalCluster().fullRestart();
        ensureYellow();

        GetDataSourceAction.Response resp = client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(dsName)).get();
        assertThat("data source should persist across full restart", resp.getDataSources(), hasSize(1));
        assertThat(resp.getDataSources().iterator().next().settings().get("region").nonSecretValue(), equalTo("us-west-2"));

        // Cleanup so subsequent tests start from a clean slate in a SUITE-scoped cluster.
        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

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

    public void testDeleteDataSourceRacingDatasetPut() throws Exception {
        final String dsName = "racing_parent";
        final String datasetName = "racing_child";

        // Seed: data source exists
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));

        // Race two operations from two threads
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(2);
        @SuppressWarnings("unchecked")
        final ActionFuture<AcknowledgedResponse>[] putFuture = (ActionFuture<AcknowledgedResponse>[]) new ActionFuture<?>[1];
        @SuppressWarnings("unchecked")
        final ActionFuture<AcknowledgedResponse>[] deleteFuture = (ActionFuture<AcknowledgedResponse>[]) new ActionFuture<?>[1];

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
        // a) DELETE wins → PUT dataset's CAS re-check throws ResourceNotFoundException.
        // b) PUT wins → DELETE sees the dependent dataset and throws ElasticsearchStatusException(CONFLICT).
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

        // Release barrier — master now runs DELETE task (succeeds), then PUT's task (re-check sees
        // parent is gone and throws ResourceNotFoundException).
        safeAwait(barrier);

        assertAcked(deleteFuture.get(30, TimeUnit.SECONDS));

        expectThrows(ExecutionException.class, () -> putFuture.get(30, TimeUnit.SECONDS));
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
    }

    public void testIndexCreationCollidesWithDataset() throws Exception {
        final String dsName = "collision_parent";
        final String collidingName = "my_collision";
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));
        assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(collidingName, dsName, "test://logs/", Map.of())));

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

    public void testDatasetSettingsCannotShadowParentSecretKey() throws Exception {
        // The SPI contract on DataSourceValidator.validateDataset says dataset settings carry no
        // secrets, but only convention enforces that. If a dataset key ever shadowed a parent
        // secret-keyed setting, DatasetRewriter.mergeSettings would silently overwrite the
        // EncryptedData carrier — losing secret-classification down the carrier path.
        // validatePutDataset rejects the put at
        // validate-time so the invariant is enforced where it's defined.
        final String parentDsName = "shadowing_parent";
        final String datasetName = "shadowing_ds";
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                putDataSourceRequest(parentDsName, Map.of("region", "us-east-1", "secret_access_key", "AKIAXYZ"))
            )
        );

        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest(datasetName, parentDsName, "test://logs/", Map.of("secret_access_key", "ANY"))
            ).get()
        );
        assertThat(err.getCause(), instanceOf(ValidationException.class));
        assertThat(err.getCause().getMessage(), containsString("dataset setting [secret_access_key] shadows a secret data-source setting"));
        expectDatasetMissing(datasetName);

        // A non-secret colliding key (region) is fine — only secret keys are rejected.
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest(datasetName, parentDsName, "test://logs/", Map.of("region", "eu-west-1"))
            )
        );

        assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(datasetName)));
        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(parentDsName)));
    }

    public void testUnknownTypeRejected() {
        PutDataSourceAction.Request req = new PutDataSourceAction.Request(
            TEST_TIMEOUT,
            TEST_TIMEOUT,
            "bad_ds",
            "unknown-type",
            null,
            new HashMap<>()
        );
        ExecutionException err = expectThrows(ExecutionException.class, () -> client().execute(PutDataSourceAction.INSTANCE, req).get());
        assertThat(err.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(err.getCause().getMessage(), containsString("unknown data source type [unknown-type]"));
        expectDataSourceMissing("bad_ds");
    }

    public void testDatasetParentMissing() {
        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(PutDatasetAction.INSTANCE, putDatasetRequest("orphan", "ghost_parent", "test://x/", Map.of())).get()
        );
        assertThat(err.getCause(), instanceOf(ResourceNotFoundException.class));
        assertThat(err.getCause().getMessage(), containsString("data source [ghost_parent] not found"));
        expectDatasetMissing("orphan");
    }

    public void testDatasetCollidesWithExistingIndex() throws Exception {
        final String name = "preexisting_index";
        final String dsName = "collision_parent_idx";
        assertAcked(client().execute(TransportCreateIndexAction.TYPE, new CreateIndexRequest(name)).get(30, TimeUnit.SECONDS));
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));

        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(name, dsName, "test://x/", Map.of())).get()
        );
        assertThat(err.getCause(), instanceOf(ResourceAlreadyExistsException.class));
        assertThat(err.getCause().getMessage(), containsString("dataset [" + name + "] cannot be created"));
        assertThat(err.getCause().getMessage(), containsString("an existing concrete index with that name is present"));

        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    public void testDatasetCollidesWithExistingView() throws Exception {
        final String name = "preexisting_view";
        final String dsName = "collision_parent_view";
        assertAcked(
            client().execute(PutViewAction.INSTANCE, new PutViewAction.Request(TEST_TIMEOUT, TEST_TIMEOUT, new View(name, "FROM some_idx")))
                .get(30, TimeUnit.SECONDS)
        );
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));

        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(name, dsName, "test://x/", Map.of())).get()
        );
        assertThat(err.getCause(), instanceOf(ResourceAlreadyExistsException.class));
        assertThat(err.getCause().getMessage(), containsString("dataset [" + name + "] cannot be created"));
        assertThat(err.getCause().getMessage(), containsString("an existing view with that name is present"));

        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    public void testDatasetReplacesExistingDataset() throws Exception {
        final String name = "replace_target";
        final String dsName = "replace_parent";
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));
        assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(name, dsName, "test://before/", Map.of())));

        assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(name, dsName, "test://after/", Map.of())));

        GetDatasetAction.Response resp = client().execute(GetDatasetAction.INSTANCE, getDatasetRequest(name)).get();
        assertThat(resp.getDatasets(), hasSize(1));
        assertThat(resp.getDatasets().iterator().next().resource(), equalTo("test://after/"));

        assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(name)));
        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    public void testDeleteDataSourceRejectedWithDependents() throws Exception {
        final String dsName = "parent_with_deps";
        final String datasetName = "dependent_ds";
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));
        assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(datasetName, dsName, "test://x/", Map.of())));

        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)).get()
        );
        assertThat(err.getCause(), instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException ese = (ElasticsearchStatusException) err.getCause();
        assertEquals(RestStatus.CONFLICT, ese.status());
        assertThat(ese.getMessage(), containsString("referenced by datasets [" + datasetName + "]"));

        // Clean up — dataset first, then parent.
        assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(datasetName)));
        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    /**
     * Regression for elastic/elasticsearch#152216: with {@code action.destructive_requires_name=false},
     * {@code DELETE /_query/dataset/*} must resolve the wildcard to datasets only, never to indices.
     * Before the fix the transport passed the raw names to the registry, so {@code *} (expanded across
     * the whole index namespace) brought back concrete index names and failed with a 404 naming an index.
     * The transport now type-filters via {@code IndexNameExpressionResolver.datasets}, mirroring
     * {@code TransportDeleteViewAction}.
     */
    public void testDeleteWildcardResolvesDatasetsNotIndices() throws Exception {
        final String dsName = "wildcard_parent";
        final String datasetA = "wildcard_logs_a";
        final String datasetB = "wildcard_logs_b";
        final String index = "wildcard_regular_index";
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        try {
            assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));
            assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(datasetA, dsName, "test://a/", Map.of())));
            assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(datasetB, dsName, "test://b/", Map.of())));
            assertAcked(client().execute(TransportCreateIndexAction.TYPE, new CreateIndexRequest(index)).get(30, TimeUnit.SECONDS));

            // DELETE /_query/dataset/* resolves to the datasets only.
            assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest("*")));

            // Both datasets are gone; the index is untouched.
            assertThat(client().execute(GetDatasetAction.INSTANCE, getDatasetRequest("*")).get().getDatasets(), hasSize(0));
            assertThat(indexExists(index), equalTo(true));

            // A second wildcard delete now matches zero datasets: an empty resolution acks (no 404).
            assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest("*")));

            assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
        } finally {
            updateClusterSettings(Settings.builder().putNull(DestructiveOperations.REQUIRES_NAME_SETTING.getKey()));
        }
    }

    /**
     * With {@code action.destructive_requires_name=true} (the production default), a wildcard dataset
     * delete is rejected outright by the destructive-operations guard — same as index deletion and view
     * deletion — and never reaches the index namespace. Explicitly named deletes are still allowed.
     */
    public void testDeleteWildcardGuardedByDestructiveRequiresName() throws Exception {
        final String dsName = "guard_parent";
        final String datasetName = "guard_dataset";
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));
        try {
            assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));
            assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(datasetName, dsName, "test://x/", Map.of())));

            ExecutionException guarded = expectThrows(
                ExecutionException.class,
                () -> client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest("*")).get()
            );
            assertThat(guarded.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(guarded.getCause().getMessage(), containsString("Wildcard expressions or all indices are not allowed"));

            // The dataset still exists — the guard rejected before any deletion — and a named delete works.
            assertThat(client().execute(GetDatasetAction.INSTANCE, getDatasetRequest(datasetName)).get().getDatasets(), hasSize(1));
            assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(datasetName)));
            assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
        } finally {
            updateClusterSettings(Settings.builder().putNull(DestructiveOperations.REQUIRES_NAME_SETTING.getKey()));
        }
    }

    /**
     * A concrete delete of a name that exists but is not a dataset (here, a real index) preserves explicit-name
     * not-found semantics and leaves the index untouched.
     */
    public void testDeleteConcreteNonDatasetNameIsNotFound() throws Exception {
        final String indexName = "not_a_dataset_index";
        assertAcked(client().execute(TransportCreateIndexAction.TYPE, new CreateIndexRequest(indexName)).get(30, TimeUnit.SECONDS));

        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(indexName)).get()
        );
        assertThat(err.getCause(), instanceOf(ResourceNotFoundException.class));
        assertThat(err.getCause().getMessage(), containsString("dataset [" + indexName + "] not found"));

        // The index is untouched.
        assertThat(indexExists(indexName), equalTo(true));
    }

    /**
     * PUT of an identical dataset is a no-op: it acks without publishing a new cluster state, mirroring
     * {@code ViewService.putView}. A changed PUT still publishes.
     */
    public void testPutIdenticalDatasetIsNoOp() throws Exception {
        final String dsName = "noop_parent";
        final String datasetName = "noop_dataset";
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest(dsName, Map.of("region", "us-east-1"))));
        assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(datasetName, dsName, "test://x/", Map.of())));

        final ClusterService masterCs = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        final long versionBefore = masterCs.state().version();

        // Identical re-PUT: acked, no cluster-state update published.
        assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(datasetName, dsName, "test://x/", Map.of())));
        assertThat(masterCs.state().version(), equalTo(versionBefore));

        // A changed PUT does publish a new state.
        assertAcked(client().execute(PutDatasetAction.INSTANCE, putDatasetRequest(datasetName, dsName, "test://changed/", Map.of())));
        assertThat(masterCs.state().version(), greaterThan(versionBefore));

        assertAcked(client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(datasetName)));
        assertAcked(client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dsName)));
    }

    static PutDataSourceAction.Request putDataSourceRequest(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TEST_TIMEOUT, TEST_TIMEOUT, name, "test", null, new HashMap<>(settings));
    }

    private static PutDataSourceAction.Request requiredSecretDataSourceRequest(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TEST_TIMEOUT, TEST_TIMEOUT, name, "test_requires_secret", null, new HashMap<>(settings));
    }

    /** Decrypts a secret setting's carrier using the test encryption key from {@link TestEncryptionServicePlugin}. */
    private static Object decryptSecret(DataSourceSetting secret) {
        DataSourceCredentials credentials = new DataSourceCredentials(new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                return new EncryptedData(TestEncryptionServicePlugin.TEST_KEY_ID, bytes);
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                return encryptedData.payload();
            }
        });
        Map<String, Object> input = new HashMap<>();
        input.put("secret", secret.rawValue());
        return credentials.decryptInPlace(input).get("secret");
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
        return new DeleteDataSourceAction.Request(TEST_TIMEOUT, TEST_TIMEOUT, new String[] { name });
    }

    private static DeleteDatasetAction.Request deleteDatasetRequest(String name) {
        return new DeleteDatasetAction.Request(TEST_TIMEOUT, TEST_TIMEOUT, new String[] { name });
    }

    private void expectDataSourceMissing(String name) {
        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(GetDataSourceAction.INSTANCE, getDataSourceRequest(name)).get()
        );
        assertThat(err.getCause(), instanceOf(ResourceNotFoundException.class));
    }

    private void expectDatasetMissing(String name) {
        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(GetDatasetAction.INSTANCE, getDatasetRequest(name)).get()
        );
        // GET resolves the name and translates a non-dataset/missing name to a clean dataset-shaped not-found,
        // matching expectDataSourceMissing — never a raw IndexNotFoundException.
        assertThat(err.getCause(), instanceOf(ResourceNotFoundException.class));
        assertThat(err.getCause().getMessage(), containsString("dataset [" + name + "] not found"));
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
            return Map.of("test", new TestValidator(), "test_requires_secret", new RequiredSecretTestValidator());
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

    /**
     * Requires {@code secret_access_key} (present in the request or carried forward), mirroring a real CSP's
     * {@code hasCredentials()} closely enough to exercise {@code DataSourceService}'s carry-forward and
     * re-validation behavior in tests, without needing a real S3/GCS/Azure setup. Kept separate from
     * {@link TestValidator} so it doesn't impose this requirement on the many other tests in this file that
     * never supply a secret at all.
     */
    static class RequiredSecretTestValidator implements DataSourceValidator {
        static final String REQUIRED_SECRET = "secret_access_key";

        @Override
        public String type() {
            return "test_requires_secret";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            return validateDatasource(datasourceSettings, Set.of());
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings, Set<String> existingSecretKeys) {
            Object provided = datasourceSettings.get(REQUIRED_SECRET);
            boolean hasRequiredSecret = Strings.hasText(provided == null ? null : provided.toString())
                || existingSecretKeys.contains(REQUIRED_SECRET);
            if (hasRequiredSecret == false) {
                ValidationException ve = new ValidationException();
                ve.addValidationError("test validator rejected: " + REQUIRED_SECRET + " is required");
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
            return new HashMap<>(datasetSettings);
        }
    }

}
