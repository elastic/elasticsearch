/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryPlugin;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryStrategy;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.stateless.Stateless.SHARD_READ_THREAD_POOL;
import static co.elastic.elasticsearch.stateless.Stateless.SHARD_READ_THREAD_POOL_SETTING;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StatelessPreWarmingIT extends AbstractStatelessIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testMaxConcurrentBCCHeaderReadsAreLimitedDuringRecoveries() throws Exception {
        int maxShardReadThreads = randomIntBetween(2, 4);
        var nodeSettings = Settings.builder()
            .put(SHARD_READ_THREAD_POOL_SETTING + ".core", 1)
            .put(SHARD_READ_THREAD_POOL_SETTING + ".max", maxShardReadThreads)
            .build();
        var indexNodeA = startMasterAndIndexNode(nodeSettings);
        var indexName = randomIdentifier();

        var numberOfShards = 2;
        // Use 2 shards to check that the limit is enforced across shards
        createIndex(indexName, numberOfShards, 0);
        ensureGreen(indexName);

        var numberOfBCCs = randomIntBetween(maxShardReadThreads + 1, 8);
        for (int i = 0; i < numberOfBCCs; i++) {
            indexDocs(indexName, 25);
            flush(indexName);
        }

        Set<String> recoveringBCCs = new HashSet<>();
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(resolveIndex(indexName), 0);
            var latestBCCName = BatchedCompoundCommit.blobNameFromGeneration(indexShard.commitStats().getGeneration());
            recoveringBCCs.add(latestBCCName);
        }

        var indexNodeB = startIndexNode(nodeSettings);

        var concurrentReads = new AtomicInteger();
        var checkConcurrentReads = new AtomicBoolean(true);
        setNodeRepositoryStrategy(indexNodeB, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                // BCC headers are read through the cache, meaning that the blob store read is dispatched into the SHARD_READ thread pool,
                // the rest of the pre-warming reads are dispatched into the PREWARM thread pool. Therefore, we only check for the max
                // concurrency in the SHARD_READ threads.
                if (recoveringBCCs.contains(blobName) == false
                    && Thread.currentThread().getName().contains(SHARD_READ_THREAD_POOL)
                    && checkConcurrentReads.get()) {
                    assertThat(concurrentReads.incrementAndGet(), is(lessThanOrEqualTo(maxShardReadThreads)));

                    // We cannot block the reads because pre-warming and recovery load the BCC headers concurrently,
                    // therefore we could block reading a single BCC header but both tasks would be blocked waiting
                    // for the read to complete and the test would deadlock.
                    return new FilterInputStream(originalSupplier.get()) {
                        @Override
                        public void close() throws IOException {
                            assertThat(concurrentReads.decrementAndGet(), is(lessThan(maxShardReadThreads)));
                            super.close();
                        }
                    };
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);

        ensureGreen(indexName);
        checkConcurrentReads.set(false);
    }

}
