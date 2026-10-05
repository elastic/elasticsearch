/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests documenting the current behaviour of EQL queries against a shard
 * being restored from a snapshot.
 *
 * <p>Before executing the search, EQL resolves field mappings via {@code _field_caps}
 * ({@code TransportFieldCapabilitiesAction}). {@code FieldCapabilitiesFetcher.fetch()} calls
 * {@code IndexShard.readAllowed()} directly, which throws
 * {@link org.elasticsearch.index.shard.IllegalIndexShardStateException} (HTTP 404) when the shard
 * is INITIALIZING (routing state; internal {@code IndexShardState} is {@code RECOVERING}).
 * This bypasses {@code SearchReadyGate} entirely, so EQL requests fail fast rather than parking.
 *
 * <p>Setup: a single-shard, no-replica index is deleted before restore so blob reads are required,
 * which is what {@code blockAllDataNodes} actually blocks.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class EqlRestoringShardIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, LocalStateEQLXPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
            .put(XPackSettings.GRAPH_ENABLED.getKey(), false)
            .put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false)
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndexWithContent(INDEX);
        ensureGreen(INDEX);
        createRepository(REPO, "mock");
        createFullSnapshot(REPO, SNAPSHOT);
        assertAcked(indicesAdmin().prepareDelete(INDEX));
    }

    // -------------------------------------------------------------------------
    // EQL search — fails fast via _field_caps (bypasses SearchReadyGate)
    // -------------------------------------------------------------------------

    /**
     * An EQL {@code GET /{index}/_eql/search} query against a fully-restoring index fails fast
     * with {@link IllegalIndexShardStateException}. Before executing the search, EQL resolves
     * field mappings by calling {@code _field_caps} ({@code TransportFieldCapabilitiesAction}).
     * {@code FieldCapabilitiesFetcher.fetch()} calls {@code IndexShard.readAllowed()} directly,
     * bypassing {@code SearchReadyGate}. When the shard is INITIALIZING, this throws
     * {@link IllegalIndexShardStateException} (HTTP 404) immediately.
     */
    public void testEqlSearchWhileRestoringFailsFastViaFieldCaps() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            EqlSearchRequest request = new EqlSearchRequest().indices(INDEX).query("any where true");
            ExecutionException e = expectThrows(
                ExecutionException.class,
                () -> client().execute(EqlSearchAction.INSTANCE, request).get(10, TimeUnit.SECONDS)
            );
            assertThat(e.getCause().getClass(), equalTo(IllegalIndexShardStateException.class));
            assertThat(ExceptionsHelper.status(e.getCause()), equalTo(RestStatus.NOT_FOUND));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }
}
