/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStatsTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.xpack.core.transform.utils.TransformConfigVersionUtils;
import org.elasticsearch.xpack.transform.DefaultTransformExtension;
import org.elasticsearch.xpack.transform.action.TransformUpdater.UpdateResult;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TransformUpdaterTests extends ESTestCase {

    private static final String BOB = "bob";
    private final SecurityContext bobSecurityContext = newSecurityContextFor(BOB);
    private static final String JOHN = "john";
    private final SecurityContext johnSecurityContext = newSecurityContextFor(JOHN);
    private final IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
    private TestThreadPool threadPool;
    private MyMockClient client;
    private TransformAuditor auditor;
    private TransformCloudCredentialManager cloudCredentialManager;
    private final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
    private final Settings destIndexSettings = new DefaultTransformExtension().getTransformDestinationIndexSettings();

    private static class MyMockClient extends NoOpClient {

        boolean resolveIndexCalled;
        boolean returnSourceIndices;
        boolean createIndexCalled;

        MyMockClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof HasPrivilegesRequest) {
                HasPrivilegesRequest hasPrivilegesRequest = (HasPrivilegesRequest) request;
                switch (hasPrivilegesRequest.username()) {
                    case BOB:
                        // bob has all the privileges
                        listener.onResponse((Response) new HasPrivilegesResponse());
                        break;
                    case JOHN:
                        // john does not have required privileges
                        listener.onFailure(new ElasticsearchSecurityException("missing privileges"));
                        break;
                    default:
                        fail("Unexpected username = " + hasPrivilegesRequest.username());
                }
            } else if (request instanceof ValidateTransformAction.Request) {
                listener.onResponse((Response) new ValidateTransformAction.Response(Collections.emptyMap()));
            } else if (request instanceof ResolveIndexAction.Request) {
                resolveIndexCalled = true;
                if (returnSourceIndices) {
                    try {
                        listener.onResponse((Response) buildNonEmptyResolveResponse());
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                } else {
                    listener.onResponse(
                        (Response) new ResolveIndexAction.Response(
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList()
                        )
                    );
                }
            } else if (request instanceof CreateIndexRequest createIndexRequest) {
                createIndexCalled = true;
                listener.onResponse((Response) new CreateIndexResponse(true, true, createIndexRequest.index()));
            } else {
                super.doExecute(action, request, listener);
            }
        }
    }

    private static ResolveIndexAction.Response buildNonEmptyResolveResponse() {
        // ResolvedIndex has a package-private constructor so we can't instantiate it directly. We only need
        // getIndices().isEmpty() == false to trigger dest index creation, so a single null element suffices.
        return new ResolveIndexAction.Response(Collections.singletonList(null), List.of(), List.of());
    }

    @Before
    public void setupClient() {
        if (threadPool != null) {
            threadPool.close();
        }
        threadPool = createThreadPool();
        client = new MyMockClient(threadPool);
        auditor = MockTransformAuditor.createMockAuditor();
        cloudCredentialManager = mock(TransformCloudCredentialManager.class);
    }

    @After
    public void tearDownClient() {
        threadPool.close();
    }

    public void testTransformUpdateNoAction() throws InterruptedException {
        TransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();

        TransformConfig maxCompatibleConfig = TransformConfigTests.randomTransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            TransformConfigVersion.CURRENT
        );
        transformConfigManager.putTransformConfiguration(maxCompatibleConfig, ActionListener.noop());
        assertConfiguration(
            listener -> transformConfigManager.getTransformConfiguration(maxCompatibleConfig.getId(), listener),
            config -> {}
        );

        TransformConfigUpdate update = TransformConfigUpdate.EMPTY;
        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                maxCompatibleConfig,
                update,
                null, // seqNoPrimaryTermAndIndex
                true,
                false,
                false,
                false,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                false, // mintCloudCredential
                null, // callerCredential
                listener
            ),
            updateResult -> {
                assertEquals(UpdateResult.Status.NONE, updateResult.getStatus());
                assertEquals(maxCompatibleConfig, updateResult.getConfig());
                assertNull(updateResult.getAuthState());
            }
        );
        assertConfiguration(listener -> transformConfigManager.getTransformConfiguration(maxCompatibleConfig.getId(), listener), config -> {
            assertNotNull(config);
            assertEquals(TransformConfigVersion.CURRENT, config.getVersion());
        });

        TransformConfig minCompatibleConfig = TransformConfigTests.randomTransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED
        );
        transformConfigManager.putTransformConfiguration(minCompatibleConfig, ActionListener.noop());

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                minCompatibleConfig,
                update,
                null, // seqNoPrimaryTermAndIndex
                true,
                false,
                false,
                false,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                false, // mintCloudCredential
                null, // callerCredential
                listener
            ),
            updateResult -> {
                assertEquals(UpdateResult.Status.NONE, updateResult.getStatus());
                assertEquals(minCompatibleConfig, updateResult.getConfig());
                assertNull(updateResult.getAuthState());
            }
        );
        assertConfiguration(listener -> transformConfigManager.getTransformConfiguration(minCompatibleConfig.getId(), listener), config -> {
            assertNotNull(config);
            assertEquals(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED, config.getVersion());
        });
    }

    public void testTransformUpdateRewrite() throws InterruptedException {
        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();

        TransformConfig oldConfig = TransformConfigTests.randomTransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            TransformConfigVersionUtils.randomVersionBetween(
                TransformConfigVersion.V_7_2_0,
                TransformConfigVersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
            )
        );

        transformConfigManager.putOldTransformConfiguration(oldConfig, ActionListener.noop());
        TransformCheckpoint checkpoint = new TransformCheckpoint(
            oldConfig.getId(),
            0L, // timestamp
            42L, // checkpoint
            Collections.singletonMap("index_1", new long[] { 1, 2, 3, 4 }), // index checkpoints
            0L
        );
        transformConfigManager.putOldTransformCheckpoint(checkpoint, ActionListener.noop());

        TransformStoredDoc stateDoc = new TransformStoredDoc(
            oldConfig.getId(),
            new TransformState(
                TransformTaskState.STARTED,
                IndexerState.INDEXING,
                null, // position
                42L, // checkpoint
                null, // reason
                null, // progress
                null, // node attributes
                false,// shouldStopAtNextCheckpoint
                null // auth state
            ),
            TransformIndexerStatsTests.randomStats()
        );
        transformConfigManager.putOrUpdateOldTransformStoredDoc(stateDoc, null, ActionListener.noop());

        assertConfiguration(listener -> transformConfigManager.getTransformConfiguration(oldConfig.getId(), listener), config -> {});

        TransformConfigUpdate update = TransformConfigUpdate.EMPTY;
        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                oldConfig,
                update,
                null, // seqNoPrimaryTermAndIndex
                true,
                false,
                false,
                false,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                false, // mintCloudCredential
                null, // callerCredential
                listener
            ),
            updateResult -> {
                assertEquals(UpdateResult.Status.UPDATED, updateResult.getStatus());
                assertNotEquals(oldConfig, updateResult.getConfig());
                assertNull(updateResult.getAuthState());
            }
        );
        assertConfiguration(listener -> transformConfigManager.getTransformConfiguration(oldConfig.getId(), listener), config -> {
            assertNotNull(config);
            assertEquals(TransformConfigVersion.CURRENT, config.getVersion());
        });

        assertCheckpoint(
            listener -> transformConfigManager.getTransformCheckpointForUpdate(oldConfig.getId(), 42L, listener),
            checkpointAndVersion -> {
                assertEquals(InMemoryTransformConfigManager.CURRENT_INDEX, checkpointAndVersion.v2().getIndex());
                assertEquals(42L, checkpointAndVersion.v1().getCheckpoint());
                assertEquals(checkpoint.getIndicesCheckpoints(), checkpointAndVersion.v1().getIndicesCheckpoints());
            }
        );

        assertStoredState(
            listener -> transformConfigManager.getTransformStoredDoc(oldConfig.getId(), false, listener),
            storedDocAndVersion -> {
                assertEquals(InMemoryTransformConfigManager.CURRENT_INDEX, storedDocAndVersion.v2().getIndex());
                assertEquals(stateDoc.getTransformState(), storedDocAndVersion.v1().getTransformState());
                assertEquals(stateDoc.getTransformStats(), storedDocAndVersion.v1().getTransformStats());
            }
        );
    }

    public void testTransformUpdateDryRun() throws InterruptedException {
        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();

        TransformConfig oldConfigForDryRunUpdate = TransformConfigTests.randomTransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            TransformConfigVersionUtils.randomVersionBetween(
                TransformConfigVersion.V_7_2_0,
                TransformConfigVersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
            )
        );

        transformConfigManager.putOldTransformConfiguration(oldConfigForDryRunUpdate, ActionListener.noop());
        assertConfiguration(
            listener -> transformConfigManager.getTransformConfiguration(oldConfigForDryRunUpdate.getId(), listener),
            config -> {}
        );

        TransformConfigUpdate update = TransformConfigUpdate.EMPTY;
        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                oldConfigForDryRunUpdate,
                update,
                null, // seqNoPrimaryTermAndIndex
                true,
                true,
                false,
                false,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                false, // mintCloudCredential
                null, // callerCredential
                listener
            ),
            updateResult -> {
                assertEquals(UpdateResult.Status.NEEDS_UPDATE, updateResult.getStatus());
                assertNotEquals(oldConfigForDryRunUpdate, updateResult.getConfig());
                assertEquals(TransformConfigVersion.CURRENT, updateResult.getConfig().getVersion());
                assertNull(updateResult.getAuthState());
            }
        );
        assertConfiguration(
            listener -> transformConfigManager.getTransformConfiguration(oldConfigForDryRunUpdate.getId(), listener),
            config -> {
                assertNotNull(config);
                assertEquals(oldConfigForDryRunUpdate, config);
            }
        );
    }

    public void testTransformUpdateCheckAccessSuccess() throws InterruptedException {
        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();

        TransformConfig oldConfig = TransformConfigTests.randomTransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            TransformConfigVersionUtils.randomVersionBetween(
                TransformConfigVersion.V_7_2_0,
                TransformConfigVersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
            )
        );
        transformConfigManager.putOldTransformConfiguration(oldConfig, ActionListener.noop());

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                oldConfig,
                TransformConfigUpdate.EMPTY,
                null, // seqNoPrimaryTermAndIndex
                false,
                false,
                true,
                false,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                false, // mintCloudCredential
                null, // callerCredential
                listener
            ),
            updateResult -> {
                assertThat(updateResult.getStatus(), is(equalTo(UpdateResult.Status.UPDATED)));
                assertThat(updateResult.getConfig(), is(not(equalTo(oldConfig))));
                assertThat(updateResult.getConfig().getVersion(), is(equalTo(TransformConfigVersion.CURRENT)));
                assertThat(updateResult.getAuthState(), is(notNullValue()));
                assertThat(updateResult.getAuthState().getStatus(), is(equalTo(HealthStatus.GREEN)));
                assertThat(updateResult.getAuthState().getLastAuthError(), is(nullValue()));
            }
        );
    }

    public void testTransformUpdateCheckAccessFailureDeferValidation() throws InterruptedException {
        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();

        TransformConfig oldConfig = TransformConfigTests.randomTransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            TransformConfigVersionUtils.randomVersionBetween(
                TransformConfigVersion.V_7_2_0,
                TransformConfigVersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
            )
        );
        transformConfigManager.putOldTransformConfiguration(oldConfig, ActionListener.noop());

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                johnSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                oldConfig,
                TransformConfigUpdate.EMPTY,
                null, // seqNoPrimaryTermAndIndex
                true,
                false,
                true,
                false,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                false, // mintCloudCredential
                null, // callerCredential
                listener
            ),
            updateResult -> {
                assertThat(updateResult.getStatus(), is(equalTo(UpdateResult.Status.UPDATED)));
                assertThat(updateResult.getConfig(), is(not(equalTo(oldConfig))));
                assertThat(updateResult.getConfig().getVersion(), is(equalTo(TransformConfigVersion.CURRENT)));
                assertThat(updateResult.getAuthState(), is(notNullValue()));
                assertThat(updateResult.getAuthState().getStatus(), is(equalTo(HealthStatus.RED)));
                assertThat(updateResult.getAuthState().getLastAuthError(), is(equalTo("missing privileges")));
            }
        );
    }

    public void testTransformUpdateCheckAccessFailureNoDeferValidation() {
        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();

        TransformConfig oldConfig = TransformConfigTests.randomTransformConfig();
        transformConfigManager.putOldTransformConfiguration(oldConfig, ActionListener.noop());

        TransformUpdater.updateTransform(
            johnSecurityContext,
            indexNameExpressionResolver,
            ClusterState.EMPTY_STATE,
            settings,
            client,
            transformConfigManager,
            auditor,
            oldConfig,
            TransformConfigUpdate.EMPTY,
            null, // seqNoPrimaryTermAndIndex
            false,
            false,
            true,
            false,
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
            destIndexSettings,
            cloudCredentialManager,
            false, // mintCloudCredential
            null, // callerCredential
            ActionListener.wrap(
                r -> fail("Should fail due to missing privileges"),
                e -> assertThat(e.getMessage(), is(equalTo("missing privileges")))
            )
        );
    }

    public void testTransformUpdateRewriteWithRemoteSourceAndRunningTask() throws InterruptedException {
        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();

        String transformId = "remote-source-transform";
        TransformConfig oldConfig = new TransformConfig(
            transformId,
            new SourceConfig(new String[] { "remote_cluster:remote_index" }, QueryConfig.matchAll(), Collections.emptyMap(), null),
            new DestConfig("local_dest_index", null, null),
            null,
            null,
            null,
            PivotConfigTests.randomPivotConfig(),
            null,
            null,
            null,
            null,
            null,
            null,
            TransformConfigVersionUtils.randomVersionBetween(
                TransformConfigVersion.V_7_2_0,
                TransformConfigVersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
            ).toString()
        );
        transformConfigManager.putOldTransformConfiguration(oldConfig, ActionListener.noop());

        // Build a cluster state with this transform registered as a running persistent task
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        PersistentTasksCustomMetadata.TYPE,
                        PersistentTasksCustomMetadata.builder()
                            .addTask(
                                transformId,
                                TransformTaskParams.NAME,
                                new TransformTaskParams(transformId, TransformConfigVersion.CURRENT, null, true),
                                new PersistentTasksCustomMetadata.Assignment("node-1", "")
                            )
                            .build()
                    )
            )
            .build();

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                clusterState,
                settings,
                client,
                transformConfigManager,
                auditor,
                oldConfig,
                TransformConfigUpdate.EMPTY,
                null,
                true,
                false,
                false,
                false,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                false, // mintCloudCredential
                null, // callerCredential
                listener
            ),
            updateResult -> {
                assertThat(updateResult.getStatus(), is(equalTo(UpdateResult.Status.UPDATED)));
                assertThat(updateResult.getConfig(), is(not(equalTo(oldConfig))));
                assertThat(updateResult.getConfig().getVersion(), is(equalTo(TransformConfigVersion.CURRENT)));
            }
        );
        // Verify that ResolveIndexAction was used to resolve source indices (including the remote one)
        assertThat("ResolveIndexAction should have been called for source index resolution", client.resolveIndexCalled, is(true));
    }

    public void testTransformUpdateRewriteWithRemoteSourceRunningTaskAndSourceIndicesFound() throws InterruptedException {
        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();

        String transformId = "remote-source-transform-with-dest-create";
        TransformConfig oldConfig = new TransformConfig(
            transformId,
            new SourceConfig(new String[] { "remote_cluster:remote_index" }, QueryConfig.matchAll(), Collections.emptyMap(), null),
            new DestConfig("local_dest_index", null, null),
            null,
            null,
            null,
            PivotConfigTests.randomPivotConfig(),
            null,
            null,
            null,
            null,
            null,
            null,
            TransformConfigVersionUtils.randomVersionBetween(
                TransformConfigVersion.V_7_2_0,
                TransformConfigVersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
            ).toString()
        );
        transformConfigManager.putOldTransformConfiguration(oldConfig, ActionListener.noop());

        // Build a cluster state with this transform registered as a running persistent task
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        PersistentTasksCustomMetadata.TYPE,
                        PersistentTasksCustomMetadata.builder()
                            .addTask(
                                transformId,
                                TransformTaskParams.NAME,
                                new TransformTaskParams(transformId, TransformConfigVersion.CURRENT, null, true),
                                new PersistentTasksCustomMetadata.Assignment("node-1", "")
                            )
                            .build()
                    )
            )
            .build();

        // Configure the mock to return a non-empty ResolveIndex response so the dest-creation path is exercised
        client.returnSourceIndices = true;

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                clusterState,
                settings,
                client,
                transformConfigManager,
                auditor,
                oldConfig,
                TransformConfigUpdate.EMPTY,
                null,
                true,
                false,
                false,
                false,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                false, // mintCloudCredential
                null, // callerCredential
                listener
            ),
            updateResult -> {
                assertThat(updateResult.getStatus(), is(equalTo(UpdateResult.Status.UPDATED)));
                assertThat(updateResult.getConfig(), is(not(equalTo(oldConfig))));
                assertThat(updateResult.getConfig().getVersion(), is(equalTo(TransformConfigVersion.CURRENT)));
            }
        );

        assertThat("ResolveIndexAction should have been called for source index resolution", client.resolveIndexCalled, is(true));
        assertThat("createDestinationIndex should have been called because source indices were found", client.createIndexCalled, is(true));
    }

    // ---- UIAM migration: project_routing defaulting tests ----

    /**
     * When a legacy transform (no credentialId) is migrated to a UIAM token (mintCloudCredential=true,
     * caller credential present) and neither the config nor the update carry an explicit project_routing,
     * the updater must default project_routing to LOCAL_ONLY to preserve the pre-migration local-only
     * search scope.
     */
    public void testUiamMigrationDefaultsProjectRoutingToLocalOnly() throws InterruptedException {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();
        // Legacy config: no credentialId, no project_routing — built deterministically.
        TransformConfig legacyConfig = legacyTransformConfig(randomAlphaOfLengthBetween(1, 10), null);
        transformConfigManager.putTransformConfiguration(legacyConfig, ActionListener.noop());

        CloudCredential callerCredential = stubMintWithNullTokenId(cloudCredentialManager);

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                legacyConfig,
                TransformConfigUpdate.EMPTY,
                null,
                true,
                false,
                false,
                true, // hasLinkedProjects — cross-project scope widening is possible
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                true, // mintCloudCredential — UIAM migration
                callerCredential,
                listener
            ),
            updateResult -> assertThat(
                "project_routing should be defaulted to LOCAL_ONLY on UIAM migration",
                updateResult.getConfig().getSource().getProjectRouting(),
                equalTo("_alias:_origin")
            )
        );
    }

    /**
     * When the original config already carries an explicit project_routing, the migration default
     * must not overwrite it.
     */
    public void testUiamMigrationDoesNotOverrideExistingProjectRouting() throws InterruptedException {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();
        // Legacy config (no credentialId) but it already has an explicit project_routing.
        TransformConfig legacyConfigWithRouting = legacyTransformConfig(randomAlphaOfLengthBetween(1, 10), "_alias:linked_project");
        transformConfigManager.putTransformConfiguration(legacyConfigWithRouting, ActionListener.noop());

        CloudCredential callerCredential = stubMintWithNullTokenId(cloudCredentialManager);

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                legacyConfigWithRouting,
                TransformConfigUpdate.EMPTY,
                null,
                true,
                false,
                false,
                true, // hasLinkedProjects
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                true,
                callerCredential,
                listener
            ),
            updateResult -> assertThat(
                "explicit project_routing on original config must not be overwritten",
                updateResult.getConfig().getSource().getProjectRouting(),
                equalTo("_alias:linked_project")
            )
        );
    }

    /**
     * When the update itself carries an explicit project_routing, the migration default must not
     * overwrite it.
     */
    public void testUiamMigrationHonoursExplicitProjectRoutingInUpdate() throws InterruptedException {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();
        TransformConfig legacyConfig = legacyTransformConfig(randomAlphaOfLengthBetween(1, 10), null);
        transformConfigManager.putTransformConfiguration(legacyConfig, ActionListener.noop());

        CloudCredential callerCredential = stubMintWithNullTokenId(cloudCredentialManager);

        TransformConfigUpdate updateWithRouting = new TransformConfigUpdate(
            new SourceConfig(legacyConfig.getSource().getIndex(), QueryConfig.matchAll(), Collections.emptyMap(), "_alias:explicit"),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                legacyConfig,
                updateWithRouting,
                null,
                true,
                false,
                false,
                true, // hasLinkedProjects
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                true,
                callerCredential,
                listener
            ),
            updateResult -> assertThat(
                "explicit project_routing in update must not be overwritten",
                updateResult.getConfig().getSource().getProjectRouting(),
                equalTo("_alias:explicit")
            )
        );
    }

    /**
     * When the incoming update explicitly supplies a source config that omits project_routing, that
     * is a deliberate choice by the caller and must not be overridden by the migration default —
     * even though the resulting project_routing resolves to null, exactly as it would if the update
     * hadn't touched source at all. The default only fires when source, and project_routing with it,
     * carries over unchanged from before the migration; here the caller explicitly re-supplied
     * source, so their (absent) project_routing is respected as null rather than defaulted.
     */
    public void testUiamMigrationRespectsExplicitSourceWithNullProjectRouting() throws InterruptedException {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();
        TransformConfig legacyConfig = legacyTransformConfig(randomAlphaOfLengthBetween(1, 10), null);
        transformConfigManager.putTransformConfiguration(legacyConfig, ActionListener.noop());

        CloudCredential callerCredential = stubMintWithNullTokenId(cloudCredentialManager);

        TransformConfigUpdate updateWithExplicitSourceNoRouting = new TransformConfigUpdate(
            new SourceConfig(legacyConfig.getSource().getIndex(), QueryConfig.matchAll(), Collections.emptyMap(), null),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                legacyConfig,
                updateWithExplicitSourceNoRouting,
                null,
                true,
                false,
                false,
                true, // hasLinkedProjects
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                true, // mintCloudCredential — UIAM migration
                callerCredential,
                listener
            ),
            updateResult -> assertThat(
                "explicit source in update must not be overridden by the migration default, even without project_routing",
                updateResult.getConfig().getSource().getProjectRouting(),
                nullValue()
            )
        );
    }

    /**
     * Re-keys of already-migrated transforms (credentialId != null) must not trigger the default.
     */
    public void testAlreadyMigratedTransformDoesNotDefaultRoutingOnRekey() throws InterruptedException {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();
        // Migrated config: has credentialId, no project_routing.
        TransformConfig migratedConfig = new TransformConfig.Builder(legacyTransformConfig(randomAlphaOfLengthBetween(1, 10), null))
            .setCredentialId("existing-token-id")
            .build();
        transformConfigManager.putTransformConfiguration(migratedConfig, ActionListener.noop());

        CloudCredential callerCredential = stubMintWithNullTokenId(cloudCredentialManager);

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                migratedConfig,
                TransformConfigUpdate.EMPTY,
                null,
                true,
                false,
                false,
                true, // hasLinkedProjects
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                true,
                callerCredential,
                listener
            ),
            updateResult -> assertThat(
                "re-key of already-migrated transform must not default project_routing",
                updateResult.getConfig().getSource().getProjectRouting(),
                nullValue()
            )
        );
    }

    /**
     * When no UIAM caller credential is present (mintCloudCredential=false), the routing default
     * must not apply.
     */
    public void testNoMintNoDefaultRouting() throws InterruptedException {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();
        TransformConfig legacyConfig = legacyTransformConfig(randomAlphaOfLengthBetween(1, 10), null);
        transformConfigManager.putTransformConfiguration(legacyConfig, ActionListener.noop());

        // cloudCredentialManager is mocked — no stubs needed since mintCloudCredential=false means
        // mintAndPersist() is never called on this path.

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                legacyConfig,
                TransformConfigUpdate.EMPTY,
                null,
                true,
                false,
                false,
                true, // hasLinkedProjects
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                false, // mintCloudCredential=false — Reset/Upgrade path, not a migration
                null, // callerCredential
                listener
            ),
            updateResult -> assertThat(
                "no routing default when mintCloudCredential=false",
                updateResult.getConfig().getSource().getProjectRouting(),
                nullValue()
            )
        );
    }

    /**
     * Linking is not durable: a project with no linked projects today can be linked later. If the
     * migration default were skipped whenever the project currently has no linked projects, a
     * migrated transform with no explicit project_routing would silently widen its search scope the
     * moment the project is linked. The default must therefore apply regardless of whether the
     * project currently has linked projects.
     */
    public void testUiamMigrationDefaultsProjectRoutingEvenWithoutLinkedProjects() throws InterruptedException {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        InMemoryTransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();
        TransformConfig legacyConfig = legacyTransformConfig(randomAlphaOfLengthBetween(1, 10), null);
        transformConfigManager.putTransformConfiguration(legacyConfig, ActionListener.noop());

        CloudCredential callerCredential = stubMintWithNullTokenId(cloudCredentialManager);

        assertUpdate(
            listener -> TransformUpdater.updateTransform(
                bobSecurityContext,
                indexNameExpressionResolver,
                ClusterState.EMPTY_STATE,
                settings,
                client,
                transformConfigManager,
                auditor,
                legacyConfig,
                TransformConfigUpdate.EMPTY,
                null,
                true,
                false,
                false,
                false, // hasLinkedProjects=false — irrelevant to the migration default; the project could be linked later
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                destIndexSettings,
                cloudCredentialManager,
                true,
                callerCredential,
                listener
            ),
            updateResult -> assertThat(
                "project_routing should be defaulted to LOCAL_ONLY on UIAM migration even without linked projects",
                updateResult.getConfig().getSource().getProjectRouting(),
                equalTo("_alias:_origin")
            )
        );
    }

    /**
     * Builds a deterministic legacy (pre-UIAM) transform config with no {@code credentialId} and
     * the given {@code projectRouting} (may be {@code null}). Uses a fixed source index to avoid
     * the randomness of {@link TransformConfigTests#randomTransformConfig} which can generate
     * non-null {@code credentialId} or {@code projectRouting} values.
     */
    private static TransformConfig legacyTransformConfig(String id, String projectRouting) {
        return new TransformConfig.Builder(TransformConfigTests.randomTransformConfig(id)).setCredentialId(null)
            .setSource(new SourceConfig(new String[] { "index-*" }, QueryConfig.matchAll(), Collections.emptyMap(), projectRouting))
            .build();
    }

    /**
     * Stubs {@code mintAndPersist} on the mock {@link TransformCloudCredentialManager} to simulate
     * a successful mint that returns a null tokenId (i.e. no actual UIAM round-trip). Returns a
     * non-null {@link CloudCredential} that callers must pass as {@code callerCredential} so the
     * UIAM migration predicate in {@link TransformUpdater} evaluates to {@code true}.
     */
    @SuppressWarnings("unchecked")
    private static CloudCredential stubMintWithNullTokenId(TransformCloudCredentialManager cloudCredentialManager) {
        CloudCredential fakeCredential = new CloudCredential(new SecureString("fake-caller-cred".toCharArray()));
        doAnswer(inv -> {
            inv.<ActionListener<String>>getArgument(2).onResponse(null);
            return null;
        }).when(cloudCredentialManager).mintAndPersist(any(), any(), any());
        return fakeCredential;
    }

    private void assertUpdate(Consumer<ActionListener<UpdateResult>> function, Consumer<UpdateResult> furtherTests)
        throws InterruptedException {
        assertAsync(function, furtherTests);
    }

    private void assertConfiguration(Consumer<ActionListener<TransformConfig>> function, Consumer<TransformConfig> furtherTests)
        throws InterruptedException {
        assertAsync(function, furtherTests);
    }

    private void assertCheckpoint(
        Consumer<ActionListener<Tuple<TransformCheckpoint, SeqNoPrimaryTermAndIndex>>> function,
        Consumer<Tuple<TransformCheckpoint, SeqNoPrimaryTermAndIndex>> furtherTests
    ) throws InterruptedException {
        assertAsync(function, furtherTests);
    }

    private void assertStoredState(
        Consumer<ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>>> function,
        Consumer<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> furtherTests
    ) throws InterruptedException {
        assertAsync(function, furtherTests);
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> furtherTests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }

    private static SecurityContext newSecurityContextFor(String username) {
        return new SecurityContext(Settings.EMPTY, new ThreadContext(Settings.EMPTY)) {
            @Override
            public User getUser() {
                return new User(username);
            }
        };
    }
}
