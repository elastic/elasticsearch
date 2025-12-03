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

import co.elastic.elasticsearch.stateless.AbstractServerlessStatelessPluginIntegTestCase;
import co.elastic.elasticsearch.stateless.ServerlessStatelessPlugin;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.SequentialRangeMissingHandler;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.TestStatelessCommitService;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntConsumer;

import static co.elastic.elasticsearch.stateless.ServerlessStatelessPlugin.PREWARM_THREAD_POOL;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessBlobCacheServiceIT extends AbstractServerlessStatelessPluginIntegTestCase {

    public static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofBytes(6 * PAGE_SIZE);
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofMb(8);

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), CACHE_SIZE);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(ServerlessStatelessPlugin.class);
        plugins.add(TestCacheServerlessStatelessPlugin.class);
        plugins.add(MockRepository.Plugin.class);
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(ShutdownPlugin.class);
        return plugins;
    }

    public void testMaybeFetchRangeWithOffsetWriteRange() throws Exception {
        // we have regions of 6 pages, 24kB each
        // we will attempt to read a range in the second region, starting at a (random) offset that is maximum 3 pages into the region
        // and ending at the end of the region (i.e. 3 pages in total)

        // we want to assert that we're using a shared input stream when filling the cache, so we'll have to create a
        // gap in the range we want to read
        // the gap will be a page in size, starting one page into the region to read
        // for e.g. if we want to read [2 x PAGE_SIZE, 6 x PAGE_SIZE] we will create a gap in the cache for [3 x PAGE_SIZE, 4 x PAGE_SIZE]

        // e.g.: region range (24k - 48k), range to write (28k - 48k), gap in cache (32k - 36k)
        // region start 24k |------ range to write start | 28k ---- 32k| gap in cache 36k| ------------ 48k| region end
        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .build();
        startMasterAndIndexNode(cacheSettings);
        startSearchNode(cacheSettings);

        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, "false")
                .build()
        );
        ensureGreen(indexName);

        indexDocs(indexName, 3_000);
        flush(indexName);
        refresh(indexName);

        IndexShard indexShard = findSearchShard(indexName);
        try (Store store = indexShard.store()) {
            var searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
            var compoundCommit = searchDirectory.getCurrentCommit();
            BlobLocation maxBlobLocation = compoundCommit.commitFiles()
                .values()
                .stream()
                .max((l1, l2) -> Long.compare(l1.offset() + l1.fileLength(), l2.offset() + l2.fileLength()))
                .orElseThrow(() -> new IllegalStateException("No commit files found"));

            StatelessSharedBlobCacheService cacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(searchDirectory);
            // let's have an empty cache
            cacheService.forceEvict(key -> true);
            FileCacheKey cacheKey = new FileCacheKey(indexShard.shardId(), maxBlobLocation.primaryTerm(), maxBlobLocation.blobName());
            CacheBlobReader blobReader = searchDirectory.getCacheBlobReaderForWarming(cacheKey.fileName(), maxBlobLocation);
            // aligning the writer offset to be a factor of page size when writing to the cache
            int writerOffset = randomFrom(PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE);
            logger.info("-> writer offset: {}, blob file: {}", writerOffset, maxBlobLocation);
            long rangeStart = REGION_SIZE.getBytes() + writerOffset;
            ByteRange rangeToWrite = ByteRange.of(
                rangeStart,
                // read until the end of region 2
                Math.min(2 * REGION_SIZE.getBytes(), maxBlobLocation.offset() + maxBlobLocation.fileLength())
            );

            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, DiscoveryNodeRole.SEARCH_ROLE);
            // fetch a sub range so we create a gap in the range to write, based on the writer offset we use in the
            ByteRange gapRange = fetchRangeToCreateGap(writerOffset, cacheService, cacheKey, maxBlobLocation, blobReader, threadPool);
            logger.info("-> gap range created: " + gapRange);

            PlainActionFuture<Boolean> future = new PlainActionFuture<>();
            logger.info("-> range to write: " + rangeToWrite);
            CopyOnWriteArrayList<Integer> channelPositions = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<Integer> relativePositions = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<Integer> lengths = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<SharedBlobCacheService.SourceInputStreamFactory> streamFactories = new CopyOnWriteArrayList<>();
            cacheService.maybeFetchRange(
                cacheKey,
                1,
                rangeToWrite,
                (maxBlobLocation.offset() + maxBlobLocation.fileLength()),
                new SequentialRangeMissingHandler(
                    this,
                    cacheKey.fileName(),
                    rangeToWrite,
                    blobReader,
                    () -> writeBuffer.get().clear(),
                    bytes -> {},
                    ServerlessStatelessPlugin.PREWARM_THREAD_POOL
                ) {
                    @Override
                    public void fillCacheRange(
                        SharedBytes.IO channel,
                        int channelPos,
                        SharedBlobCacheService.SourceInputStreamFactory streamFactory,
                        int relativePos,
                        int len,
                        IntConsumer progressUpdater,
                        ActionListener<Void> completionListener
                    ) throws IOException {
                        streamFactories.add(streamFactory);
                        channelPositions.add(channelPos);
                        relativePositions.add(relativePos);
                        lengths.add(len);
                        super.fillCacheRange(channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener);
                    }
                },
                threadPool.executor(PREWARM_THREAD_POOL),
                future
            );
            future.get(10, TimeUnit.SECONDS);

            // we're expecting two gaps to fill so all tracked metrics should record 2 values
            assertThat(streamFactories.size(), is(2));
            assertThat(channelPositions.size(), is(2));
            assertThat(relativePositions.size(), is(2));
            assertThat(lengths.size(), is(2));

            // we expect both fillRange calls to use a non-null stream factory, and for it to be the same for both calls
            assertThat(streamFactories.get(0), is(notNullValue()));
            assertThat(streamFactories.get(0), is(streamFactories.get(1)));

            // normalize the requested ranges to the second region
            ByteRange requestedRegionRange = ByteRange.of(
                rangeToWrite.start() - REGION_SIZE.getBytes(),
                rangeToWrite.end() - REGION_SIZE.getBytes()
            );
            ByteRange gapRegionRange = ByteRange.of(gapRange.start() - REGION_SIZE.getBytes(), gapRange.end() - REGION_SIZE.getBytes());

            // the relative positions should be offsetted relative to the region start - the first call should be offsetted all the way to
            // beginning of the range, whilst the second call should receive a relative position that is the start second read (i.e. after
            // the gap) minus the same offset as the initial read, the start of the range to write - region size
            assertThat(relativePositions.get(0), is(0));
            assertThat(relativePositions.get(1), is(Math.toIntExact(gapRegionRange.end() - writerOffset)));
            // the length of the first call should be the difference between the start of the range and the start of the gap
            // (i.e. we read from the beginning of the range until the start of the gap)
            assertThat(lengths.getFirst(), is(Math.toIntExact(gapRange.start() - rangeToWrite.start())));
            // the length of the second call should be the difference between the end of the second region and the end of the gap we created
            assertThat(lengths.getLast(), is(Math.toIntExact(2 * REGION_SIZE.getBytes() - gapRange.end())));

            assertThat(channelPositions.get(0), is(Math.toIntExact(requestedRegionRange.start())));
            assertThat(channelPositions.get(1), is(Math.toIntExact(gapRegionRange.end())));

            // fetch all the docs to make sure we can read from the cache
            assertResponse(prepareSearch(indexName), response -> assertThat(response.getHits().getTotalHits().value(), is(3000L)));
        }
    }

    private ByteRange fetchRangeToCreateGap(
        int writerOffset,
        StatelessSharedBlobCacheService cacheService,
        FileCacheKey cacheKey,
        BlobLocation maxBlobLocation,
        CacheBlobReader blobReader,
        ThreadPool threadPool
    ) throws InterruptedException, TimeoutException, ExecutionException {
        // create a hole in the cache for the range we want to fetch (this way we'll reuse the writer input stream)
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        ByteRange range = ByteRange.of(
            REGION_SIZE.getBytes() + writerOffset + PAGE_SIZE,
            REGION_SIZE.getBytes() + writerOffset + 2 * PAGE_SIZE
        );
        cacheService.maybeFetchRange(
            cacheKey,
            1,
            range,
            (maxBlobLocation.offset() + maxBlobLocation.fileLength()),
            new SequentialRangeMissingHandler(
                this,
                cacheKey.fileName(),
                range,
                blobReader,
                () -> writeBuffer.get().clear(),
                bytes -> {},
                ServerlessStatelessPlugin.PREWARM_THREAD_POOL
            ),
            threadPool.executor(PREWARM_THREAD_POOL),
            future
        );
        future.get(10, TimeUnit.SECONDS);
        return range;
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );

    public static final class TestCacheServerlessStatelessPlugin extends ServerlessStatelessPlugin {

        public TestCacheServerlessStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(Plugin.PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new TestStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            TelemetryProvider telemetryProvider,
            Settings settings
        ) {
            // no-op the warming on shard recovery so we can manually fetch ranges into the cache on the search tier
            return new SharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, settings) {
                @Override
                public void warmCacheForShardRecovery(
                    Type type,
                    IndexShard indexShard,
                    StatelessCompoundCommit commit,
                    BlobStoreCacheDirectory directory
                ) {}
            };
        }
    }

}
