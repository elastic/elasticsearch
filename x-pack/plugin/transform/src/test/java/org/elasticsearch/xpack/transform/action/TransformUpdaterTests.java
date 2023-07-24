/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStatsTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.action.TransformUpdater.UpdateResult;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class TransformUpdaterTests extends ESTestCase {

    private static final String BOB = "bob";
    private final SecurityContext bobSecurityContext = newSecurityContextFor(BOB);
    private static final String JOHN = "john";
    private final SecurityContext johnSecurityContext = newSecurityContextFor(JOHN);
    private final IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
    private Client client;
    private ClusterService clusterService = mock(ClusterService.class);
    private TransformAuditor auditor = new MockTransformAuditor(clusterService);
    private final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();

    private static class MyMockClient extends NoOpClient {

        MyMockClient(String testName) {
            super(testName);
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
            } else {
                super.doExecute(action, request, listener);
            }
        }
    }

    @Before
    public void setupClient() {
        if (client != null) {
            client.close();
        }
        client = new MyMockClient(getTestName());
        clusterService = mock(ClusterService.class);
        auditor = new MockTransformAuditor(clusterService);
    }

    @After
    public void tearDownClient() {
        client.close();
    }

    public void testTransformUpdateNoAction() throws InterruptedException {
        TransformConfigManager transformConfigManager = new InMemoryTransformConfigManager();

        TransformConfig maxCompatibleConfig = TransformConfigTests.randomTransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            Version.CURRENT
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
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
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
            assertEquals(Version.CURRENT, config.getVersion());
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
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
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
            VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_2_0,
                VersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
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
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
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
            assertEquals(Version.CURRENT, config.getVersion());
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
            VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_2_0,
                VersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
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
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                listener
            ),
            updateResult -> {
                assertEquals(UpdateResult.Status.NEEDS_UPDATE, updateResult.getStatus());
                assertNotEquals(oldConfigForDryRunUpdate, updateResult.getConfig());
                assertEquals(Version.CURRENT, updateResult.getConfig().getVersion());
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
            VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_2_0,
                VersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
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
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                listener
            ),
            updateResult -> {
                assertThat(updateResult.getStatus(), is(equalTo(UpdateResult.Status.UPDATED)));
                assertThat(updateResult.getConfig(), is(not(equalTo(oldConfig))));
                assertThat(updateResult.getConfig().getVersion(), is(equalTo(Version.CURRENT)));
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
            VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_2_0,
                VersionUtils.getPreviousVersion(TransformConfig.CONFIG_VERSION_LAST_DEFAULTS_CHANGED)
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
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                listener
            ),
            updateResult -> {
                assertThat(updateResult.getStatus(), is(equalTo(UpdateResult.Status.UPDATED)));
                assertThat(updateResult.getConfig(), is(not(equalTo(oldConfig))));
                assertThat(updateResult.getConfig().getVersion(), is(equalTo(Version.CURRENT)));
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
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
            ActionListener.wrap(
                r -> fail("Should fail due to missing privileges"),
                e -> assertThat(e.getMessage(), is(equalTo("missing privileges")))
            )
        );
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
