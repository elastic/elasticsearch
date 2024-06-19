/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.translog.ChannelFactory;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class BulkAfterWriteFsyncFailureIT extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(FailingFsyncEnginePlugin.class);
    }

    public static class FailingFsyncEnginePlugin extends Plugin implements EnginePlugin {
        static final AtomicBoolean simulateFsyncFailure = new AtomicBoolean(false);

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(config) {
                @Override
                protected ChannelFactory getTranslogChannelFactory() {
                    return (file, openOption) -> new FilterFileChannel(FileChannel.open(file, openOption)) {
                        @Override
                        public void force(boolean metaData) throws IOException {
                            if (simulateFsyncFailure.compareAndSet(true, false)) {
                                throw new IOException("Simulated fsync failure");
                            } else {
                                super.force(metaData);
                            }
                        }
                    };
                }
            });
        }
    }

    public void testFsyncFailureDoesNotAdvanceLocalCheckpoints() {
        String indexName = randomIdentifier();
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            )
            .get();
        ensureGreen(indexName);

        FailingFsyncEnginePlugin.simulateFsyncFailure.set(true);
        var localCheckpointBeforeBulk = getLocalCheckpointForShard(indexName, 0);
        var bulkResponse = client().prepareBulk().add(prepareIndex(indexName).setId("1").setSource("key", "foo", "val", 10)).get();
        assertTrue(bulkResponse.hasFailures());
        var localCheckpointAfterFailedBulk = getLocalCheckpointForShard(indexName, 0);
        // fsync for the translog failed, hence the checkpoint doesn't advance
        assertThat(localCheckpointBeforeBulk, equalTo(localCheckpointAfterFailedBulk));

        // Since background refreshes are disabled, the shard is considered green until the next operation is appended into the translog
        ensureGreen(indexName);

        var bulkResponse2 = client().prepareBulk().add(prepareIndex(indexName).setId("2").setSource("key", "bar", "val", 20)).get();
        assertFalse(bulkResponse2.hasFailures());

        var localCheckpointAfterSuccessfulBulk = getLocalCheckpointForShard(indexName, 0);
        assertThat(localCheckpointAfterSuccessfulBulk, is(greaterThan(localCheckpointAfterFailedBulk)));
    }

    long getLocalCheckpointForShard(String index, int shardId) {
        var indicesService = getInstanceFromNode(IndicesService.class);
        var indexShard = indicesService.indexServiceSafe(resolveIndex(index)).getShard(shardId);
        return indexShard.getLocalCheckpoint();
    }
}
