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

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;

import java.util.concurrent.Executor;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

/**
 * Service to get a {@link CacheBlobReader} for a shard and a {@link BlobLocation}. Automatically switches to fetching from the primary
 * shard if the blob has not been uploaded to the object store.
 */
public class CacheBlobReaderService {

    /**
     * The setting for the chunk size to be used for the ranges returned by {@link IndexingShardCacheBlobReader}. The chunk size is only
     * used to round down the beginning of a range. The end of the range is always rounded up to the next page.
     */
    public static final Setting<ByteSizeValue> TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING = new Setting<>(
        "stateless.transport_blob_reader.chunk_size",
        ByteSizeValue.ofKb(128).getStringRep(),
        s -> ByteSizeValue.parseBytesSizeValue(s, "stateless.indexing_shard_cache_blob_reader.chunk_size"),
        value -> {
            if (value.getBytes() <= 0L || value.getBytes() % SharedBytes.PAGE_SIZE != 0L) {
                throw new SettingsException(
                    "setting [{}] must be greater than zero and must be multiple of {}",
                    "stateless.transport_blob_reader.chunk_size",
                    SharedBytes.PAGE_SIZE
                );
            }
        },
        Setting.Property.NodeScope
    );

    private final StatelessSharedBlobCacheService cacheService;
    private final Client client;
    private final ByteSizeValue indexingShardCacheBlobReaderChunkSize;
    private final ThreadPool threadPool;

    // TODO consider specializing the CacheBlobReaderService for the indexing node to always consider blobs as uploaded (ES-8248)
    // TODO refactor CacheBlobReaderService to keep track of shard's upload info itself (ES-8248)
    public CacheBlobReaderService(Settings settings, StatelessSharedBlobCacheService cacheService, Client client, ThreadPool threadPool) {
        this.cacheService = cacheService;
        this.client = client;
        this.indexingShardCacheBlobReaderChunkSize = TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(settings);
        this.threadPool = threadPool;
    }

    /**
     * Returns a {@link CacheBlobReader} for the given shard and the blob specified by the given {@link BlobLocation}.
     *
     * @param shardId                       the shard id
     * @param blobContainer                 the blob container where the blob can be read from
     * @param blobFile                      the blob file
     * @param tracker                       the tracker to determine if the blob has been uploaded to the object store
     * @param totalBytesReadFromObjectStore counts how many bytes were read from object store
     * @param totalBytesReadFromIndexing    counts how many bytes were read from indexing nodes
     * @param cachePopulationReason         The reason that we're reading from the data source
     * @param fileName                      The actual (lucene) file that's requested from the blob location
     * @return a {@link CacheBlobReader} for the given shard and blob
     */
    public CacheBlobReader getCacheBlobReader(
        ShardId shardId,
        LongFunction<BlobContainer> blobContainer,
        BlobFile blobFile,
        MutableObjectStoreUploadTracker tracker,
        LongConsumer totalBytesReadFromObjectStore,
        LongConsumer totalBytesReadFromIndexing,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        Executor objectStoreFetchExecutor,
        String fileName
    ) {
        final var locationPrimaryTermAndGeneration = blobFile.termAndGeneration();
        final long rangeSize = cacheService.getRangeSize();
        var objectStoreCacheBlobReader = new MeteringCacheBlobReader(
            getObjectStoreCacheBlobReader(
                blobContainer.apply(blobFile.primaryTerm()),
                blobFile.blobName(),
                rangeSize,
                objectStoreFetchExecutor
            ),
            createReadCompleteCallback(fileName, totalBytesReadFromObjectStore, CachePopulationSource.BlobStore, cachePopulationReason)
        );
        var latestUploadInfo = tracker.getLatestUploadInfo(locationPrimaryTermAndGeneration);
        if (latestUploadInfo.isUploaded()) {
            return objectStoreCacheBlobReader;
        } else {
            var indexingShardCacheBlobReader = new MeteringCacheBlobReader(
                new IndexingShardCacheBlobReader(
                    shardId,
                    locationPrimaryTermAndGeneration,
                    latestUploadInfo.preferredNodeId(),
                    client,
                    indexingShardCacheBlobReaderChunkSize,
                    threadPool
                ),
                createReadCompleteCallback(fileName, totalBytesReadFromIndexing, CachePopulationSource.Peer, cachePopulationReason)
            );
            return new SwitchingCacheBlobReader(
                tracker,
                locationPrimaryTermAndGeneration,
                objectStoreCacheBlobReader,
                indexingShardCacheBlobReader
            );
        }
    }

    // protected to override in tests
    protected CacheBlobReader getObjectStoreCacheBlobReader(
        BlobContainer blobContainer,
        String blobName,
        long cacheRangeSize,
        Executor fetchExecutor
    ) {
        return new ObjectStoreCacheBlobReader(blobContainer, blobName, cacheRangeSize, fetchExecutor);
    }

    private MeteringCacheBlobReader.ReadCompleteCallback createReadCompleteCallback(
        String fileName,
        LongConsumer bytesReadCounter,
        CachePopulationSource cachePopulationSource,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason
    ) {
        return (bytesRead, readTimeNanos) -> {
            bytesReadCounter.accept(bytesRead);
            cacheService.getBlobCacheMetrics()
                .recordCachePopulationMetrics(fileName, bytesRead, readTimeNanos, cachePopulationReason, cachePopulationSource);
        };
    }
}
