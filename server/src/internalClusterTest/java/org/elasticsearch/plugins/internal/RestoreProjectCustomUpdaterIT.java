/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.plugins.internal;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.RestoreService.RestoreCompletionResponse;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.TestProjectCustomMetadata;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.junit.After;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for {@link RestoreProjectCustomUpdater} covering the two callbacks
 * ({@code onRestoreInitialized} and {@code onRestoreCompleted}), that what they return is published with the restore, and the
 * idempotency guarantee that {@code onRestoreInitialized} is not called twice when the same restore UUID is submitted
 * while the first restore is still in progress.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RestoreProjectCustomUpdaterIT extends AbstractSnapshotIntegTestCase {

    private static final String CUSTOM_TYPE = "test_restore_project_custom";

    /** The custom that the updaters in these tests maintain. */
    public static class TestRestoreCustom extends TestProjectCustomMetadata {
        TestRestoreCustom(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return CUSTOM_TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.ALL_CONTEXTS;
        }
    }

    /** Registers {@link TestRestoreCustom}, which is published to every node. */
    public static class TestRestoreCustomPlugin extends Plugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    Metadata.ProjectCustom.class,
                    CUSTOM_TYPE,
                    in -> TestProjectCustomMetadata.readFrom(TestRestoreCustom::new, in)
                ),
                new NamedWriteableRegistry.Entry(
                    NamedDiff.class,
                    CUSTOM_TYPE,
                    in -> TestProjectCustomMetadata.readDiffFrom(CUSTOM_TYPE, in)
                )
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(
                    Metadata.ProjectCustom.class,
                    new ParseField(CUSTOM_TYPE),
                    parser -> TestProjectCustomMetadata.fromXContent(TestRestoreCustom::new, parser)
                )
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(TestRestoreCustomPlugin.class)).toList();
    }

    /*
     * RestoreProjectCustomUpdater registered through SPI (see META-INF/services) that lets us set a custom updater at runtime
     */
    public static class DelegatingRestoreProjectCustomUpdater implements RestoreProjectCustomUpdater {
        private static volatile RestoreProjectCustomUpdater delegate = RestoreProjectCustomUpdater.NOOP;

        @Override
        public String projectCustomType() {
            return delegate.projectCustomType();
        }

        @Override
        public Metadata.ProjectCustom onRestoreInitialized(RestoreInProgress.Entry entry, @Nullable Metadata.ProjectCustom current) {
            return delegate.onRestoreInitialized(entry, current);
        }

        @Override
        public Metadata.ProjectCustom onRestoreCompleted(
            RestoreInProgress.Entry entry,
            ProjectMetadata project,
            @Nullable Metadata.ProjectCustom current
        ) {
            return delegate.onRestoreCompleted(entry, project, current);
        }
    }

    @After
    public void resetRestoreProjectCustomUpdater() {
        DelegatingRestoreProjectCustomUpdater.delegate = RestoreProjectCustomUpdater.NOOP;
    }

    /**
     * Sets the given updater as the delegate on the {@link DelegatingRestoreProjectCustomUpdater} used by this test
     */
    private void setRestoreProjectCustomUpdater(RestoreProjectCustomUpdater updater) {
        DelegatingRestoreProjectCustomUpdater.delegate = updater;
    }

    private Metadata.ProjectCustom customOf(String node) {
        return internalCluster().getInstance(ClusterService.class, node)
            .state()
            .metadata()
            .getProject(ProjectId.DEFAULT)
            .custom(CUSTOM_TYPE);
    }

    private static final String REPO = "test-repo";
    private static final String SNAP = "test-snap";
    private static final String IDX = "test-idx";

    /**
     * Verifies that {@code onRestoreInitialized} fires exactly once when the cluster-state update
     * installs the {@link RestoreInProgress} entry, and that {@code onRestoreCompleted} fires
     * exactly once when the cleanup task removes it. What each returns is published with the restore, so the custom is there when the
     * restore is running and has the second result when it is over.
     */
    public void testLifecycleCallbacksFireOnceForOrdinaryRestore() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepository(REPO, "mock");
        createIndex(IDX, 1, 0);
        indexRandomDocs(IDX, between(1, 50));
        ensureGreen(IDX);
        createFullSnapshot(REPO, SNAP);
        cluster().wipeIndices(IDX);

        AtomicInteger initCount = new AtomicInteger(0);
        AtomicInteger completedCount = new AtomicInteger(0);

        setRestoreProjectCustomUpdater(new RestoreProjectCustomUpdater() {
            @Override
            public String projectCustomType() {
                return CUSTOM_TYPE;
            }

            @Override
            public Metadata.ProjectCustom onRestoreInitialized(RestoreInProgress.Entry entry, Metadata.ProjectCustom current) {
                initCount.incrementAndGet();
                assertThat(current, nullValue());
                return new TestRestoreCustom("initialized");
            }

            @Override
            public Metadata.ProjectCustom onRestoreCompleted(
                RestoreInProgress.Entry entry,
                ProjectMetadata project,
                Metadata.ProjectCustom current
            ) {
                completedCount.incrementAndGet();
                assertThat(current, equalTo(new TestRestoreCustom("initialized")));
                return new TestRestoreCustom("completed");
            }
        });

        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPO, SNAP).setIndices(IDX).setWaitForCompletion(true).get();

        assertThat(initCount.get(), equalTo(1));
        assertThat(completedCount.get(), equalTo(1));
        // The custom is published with the removal of the restore entry, so it is not left until a later update.
        assertBusy(() -> assertThat(customOf(internalCluster().getMasterName()), equalTo(new TestRestoreCustom("completed"))));
    }

    /**
     * Verifies that when a second call arrives with the same restore UUID while the first restore is
     * still in progress, {@code execute()} returns the cluster state unchanged and
     * {@code onRestoreInitialized} is NOT called a second time.
     *
     * <p>The restore is kept in-flight by blocking the mock repository on all data nodes.
     */
    public void testOnRestoreInitializedNotCalledTwiceForSameUUID() throws Exception {
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode(SMALL_SNAPSHOT_POOL_SETTINGS);

        createRepository(REPO, "mock");
        createIndex(IDX, 1, 0);
        indexRandomDocs(IDX, between(1, 50));
        ensureGreen(IDX);
        createFullSnapshot(REPO, SNAP);
        cluster().wipeIndices(IDX);

        AtomicInteger initCount = new AtomicInteger(0);

        String masterName = internalCluster().getMasterName();
        RestoreService restoreService = internalCluster().getInstance(RestoreService.class, masterName);
        setRestoreProjectCustomUpdater(new RestoreProjectCustomUpdater() {
            @Override
            public String projectCustomType() {
                return CUSTOM_TYPE;
            }

            @Override
            public Metadata.ProjectCustom onRestoreInitialized(RestoreInProgress.Entry entry, Metadata.ProjectCustom current) {
                initCount.incrementAndGet();
                return new TestRestoreCustom("initialized");
            }
        });

        // Block the data node so shard recovery stalls and the RestoreInProgress entry stays in cluster state.
        blockAllDataNodes(REPO);

        String restoreUUID = UUIDs.randomBase64UUID();
        var request = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPO, SNAP)
            .setIndices(IDX)
            .setWaitForCompletion(false)
            .request();

        // First restore call — async, will not complete until data node is unblocked.
        PlainActionFuture<RestoreCompletionResponse> firstFuture = new PlainActionFuture<>();
        restoreService.restoreSnapshot(ProjectId.DEFAULT, request, restoreUUID, firstFuture, (s, b) -> {});

        // Wait until the data node is actually stuck in the repo block (shard recovery started).
        waitForBlock(dataNode, REPO);
        assertThat("onRestoreInitialized must have fired exactly once after first call", initCount.get(), equalTo(1));
        // Confirm the entry is visible in cluster state.
        assertNotNull(
            "RestoreInProgress entry for UUID [" + restoreUUID + "] must be present",
            RestoreInProgress.get(internalCluster().getInstance(ClusterService.class, masterName).state()).get(restoreUUID)
        );

        // Second call with the same UUID — idempotent retry; onRestoreInitialized must NOT fire again.
        PlainActionFuture<RestoreCompletionResponse> secondFuture = new PlainActionFuture<>();
        restoreService.restoreSnapshot(ProjectId.DEFAULT, request, restoreUUID, secondFuture, (s, b) -> {});

        // The second call's cluster-state task returns early (UUID already present) and resolves
        // with a null restoreInfo, signalling the caller that nothing was applied.
        RestoreCompletionResponse secondResponse = secondFuture.actionGet(10, TimeUnit.SECONDS);
        assertThat("idempotent retry must return null restoreInfo", secondResponse.restoreInfo(), nullValue());
        assertThat("onRestoreInitialized must still have fired only once", initCount.get(), equalTo(1));

        // Unblock the data node and let the first restore finish normally.
        unblockAllDataNodes(REPO);
        assertBusy(
            () -> assertTrue(
                "RestoreInProgress must be empty once restore completes",
                RestoreInProgress.get(internalCluster().getInstance(ClusterService.class, masterName).state()).isEmpty()
            )
        );
    }
}
