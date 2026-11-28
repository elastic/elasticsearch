/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A library that fails to load throws an {@code Error}, and an {@code Error} on a worker thread is not handled by the code that
 * completes the listener of a request. The engine converts it into an exception, and this test shows that a flush then fails and does
 * not hang, and that the rows stay in the local files.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ParquetLinkageFailureIT extends ESIntegTestCase {

    /** The plugin whose table cannot be opened, because a class of the table libraries is missing. */
    public static class MissingClassPlugin extends ParquetWriteEnginePlugin {
        @Override
        protected ShardTable openTable(ShardId shardId, Settings indexSettings, String historyUuid) throws IOException {
            throw new NoClassDefFoundError("org/apache/iceberg/TableMetadata");
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MissingClassPlugin.class);
    }

    @Override
    protected int numberOfReplicas() {
        // the random index template of the base class would add a replica, which this format does not support
        return 0;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected void beforeIndexDeletion() {
        // the base class searches every shard, and this engine cannot be searched
    }

    public void testAFlushWithAMissingClassFailsAndDoesNotHang() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("parquet-linkage")
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", 1)
                        .put("index.parquet.repository", "not-used")
                        .put("index.parquet.durability", "local")
                )
                .setMapping("count", "type=long")
        );
        ensureGreen("parquet-linkage");
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = 0; i < 100; i++) {
            bulk.add(new IndexRequest("parquet-linkage").source("count", i));
        }
        assertFalse(bulk.get().hasFailures());

        final BroadcastResponse flush = indicesAdmin().prepareFlush("parquet-linkage").get();

        assertThat(flush.getFailedShards(), equalTo(1));
        final String failure = ExceptionsHelper.stackTrace(flush.getShardFailures()[0].getCause());
        assertThat(failure, containsString("org/apache/iceberg/TableMetadata"));
        assertThat(failure, containsString("is not available"));
        // the rows are still counted, in the local files
        assertThat(indicesAdmin().prepareStats("parquet-linkage").get().getTotal().getDocs().getCount(), equalTo(100L));
    }
}
