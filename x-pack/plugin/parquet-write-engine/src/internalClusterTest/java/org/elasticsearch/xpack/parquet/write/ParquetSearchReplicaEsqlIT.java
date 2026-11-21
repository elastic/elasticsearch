/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/** ES|QL reads an index of the write engine from its search replicas, and never from the primary. */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetFlushIT.IcebergWorkerPoolFilter.class }
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ParquetSearchReplicaEsqlIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "parquet-esql";
    private static final String REPOSITORY = "lake";
    private static final int ROWS = 2_000;

    private static Path repoRoot;

    @BeforeClass
    public static void createRepoRoot() {
        repoRoot = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Environment.PATH_REPO_SETTING.getKey(), repoRoot)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ParquetWriteEnginePlugin.class);
        return plugins;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected void beforeIndexDeletion() {
        // the base class compares the copies of every shard, and the primary of this index cannot be searched
    }

    private void createIndexWithUploadedRows(int replicas) throws IOException {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoRoot.resolve(REPOSITORY).toString()))
        );
        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("host")
            .field("type", "keyword")
            .endObject()
            .startObject("count")
            .field("type", "long")
            .endObject()
            .startObject("ratio")
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", replicas)
                        .put("index.parquet.repository", REPOSITORY)
                )
                .setMapping(mapping)
        );
        ensureGreen(INDEX);
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = 0; i < ROWS; i++) {
            bulk.add(new IndexRequest(INDEX).source("host", "host-" + (i % 4), "count", i, "ratio", i * 0.25));
        }
        assertFalse(bulk.get().hasFailures());
        indicesAdmin().prepareFlush(INDEX).setForce(true).setWaitIfOngoing(true).get();
        indicesAdmin().prepareRefresh(INDEX).get();
    }

    public void testStatsOnTheSearchReplica() throws Exception {
        createIndexWithUploadedRows(1);
        try (var response = run("FROM " + INDEX + " | STATS c = COUNT(*), s = SUM(count), m = MAX(ratio)")) {
            final List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo((long) ROWS));
            assertThat(((Number) values.get(0).get(1)).longValue(), equalTo((long) ROWS * (ROWS - 1) / 2));
            assertThat(((Number) values.get(0).get(2)).doubleValue(), equalTo((ROWS - 1) * 0.25));
        }
        try (var response = run("FROM " + INDEX + " | WHERE count >= 100 AND count < 200 | STATS c = COUNT(*) BY host | SORT host")) {
            final List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(4));
            for (List<Object> row : values) {
                assertThat(((Number) row.get(0)).longValue(), equalTo(25L));
            }
        }
    }

    public void testEsqlNeverReadsThePrimary() throws Exception {
        createIndexWithUploadedRows(0);
        // the only copy is the primary, which is not searchable: ES|QL finds no copy that it may read
        final Exception e = expectThrows(Exception.class, () -> run("FROM " + INDEX + " | STATS c = COUNT(*)").close());
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(NoShardAvailableActionException.class));
        assertThat(e.getMessage(), containsString("has no active shard copy"));
    }
}
