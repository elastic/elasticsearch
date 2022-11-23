/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskDisruptionIT extends AbstractDisruptionTestCase {

    private static DisruptTranslogFileSystemProvider disruptTranslogFileSystemProvider;

    @BeforeClass
    public static void installDisruptTranslogFS() {
        FileSystem current = PathUtils.getDefaultFileSystem();
        disruptTranslogFileSystemProvider = new DisruptTranslogFileSystemProvider(current);
        PathUtilsForTesting.installMock(disruptTranslogFileSystemProvider.getFileSystem(null));
    }

    @AfterClass
    public static void removeDisruptTranslogFS() {
        PathUtilsForTesting.teardown();
    }

    void injectTranslogFailures() {
        disruptTranslogFileSystemProvider.injectFailures.set(true);
    }

    @After
    void stopTranslogFailures() {
        disruptTranslogFileSystemProvider.injectFailures.set(false);
    }

    static class DisruptTranslogFileSystemProvider extends FilterFileSystemProvider {

        AtomicBoolean injectFailures = new AtomicBoolean();

        DisruptTranslogFileSystemProvider(FileSystem inner) {
            super("disrupttranslog://", inner);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            if (injectFailures.get() && path.toString().endsWith(".ckp")) {
                // prevents checkpoint file to be updated
                throw new IOException("fake IOException");
            }
            return super.newFileChannel(path, options, attrs);
        }

    }

    /**
     * This test checks that all operations below the global checkpoint are properly persisted.
     * It simulates a full power outage by preventing translog checkpoint files to be written and restart the cluster. This means that
     * all un-fsynced data will be lost.
     */
    public void testGlobalCheckpointIsSafe() throws Exception {
        startCluster(rarely() ? 5 : 3);

        final int numberOfShards = 1 + randomInt(2);
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
            )
        );
        ensureGreen();

        AtomicBoolean stopGlobalCheckpointFetcher = new AtomicBoolean();

        Map<Integer, Long> shardToGcp = new ConcurrentHashMap<>();
        for (int i = 0; i < numberOfShards; i++) {
            shardToGcp.put(i, SequenceNumbers.NO_OPS_PERFORMED);
        }
        final Thread globalCheckpointSampler = new Thread(() -> {
            while (stopGlobalCheckpointFetcher.get() == false) {
                try {
                    for (ShardStats shardStats : client().admin().indices().prepareStats("test").clear().get().getShards()) {
                        final int shardId = shardStats.getShardRouting().id();
                        final long globalCheckpoint = shardStats.getSeqNoStats().getGlobalCheckpoint();
                        shardToGcp.compute(shardId, (i, v) -> Math.max(v, globalCheckpoint));
                    }
                } catch (Exception e) {
                    // ignore
                    logger.debug("failed to fetch shard stats", e);
                }
            }
        });

        globalCheckpointSampler.start();

        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                "test",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.setRequestTimeout(TimeValue.ZERO);
            indexer.setIgnoreIndexingFailures(true);
            indexer.setFailureAssertion(e -> {});
            indexer.start(-1);

            waitForDocs(randomIntBetween(1, 100), indexer);

            logger.info("injecting failures");
            injectTranslogFailures();
            logger.info("stopping indexing");
        }

        logger.info("full cluster restart");
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {

            @Override
            public void onAllNodesStopped() {
                logger.info("stopping failures");
                stopTranslogFailures();
            }

        });

        stopGlobalCheckpointFetcher.set(true);

        logger.info("waiting for global checkpoint sampler");
        globalCheckpointSampler.join();

        logger.info("waiting for green");
        ensureGreen("test");

        for (ShardStats shardStats : client().admin().indices().prepareStats("test").clear().get().getShards()) {
            final int shardId = shardStats.getShardRouting().id();
            final long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
            if (shardStats.getShardRouting().active()) {
                assertThat(maxSeqNo, greaterThanOrEqualTo(shardToGcp.get(shardId)));
            }
        }
    }

}
