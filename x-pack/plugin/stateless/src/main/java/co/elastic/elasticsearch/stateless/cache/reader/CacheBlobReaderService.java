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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.cache.reader;

import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;

import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;

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

    // TODO consider specializing the CacheBlobReaderService for the indexing node to always consider blobs as uploaded (ES-8248)
    // TODO refactor CacheBlobReaderService to keep track of shard's upload info itself (ES-8248)
    public CacheBlobReaderService(Settings settings, StatelessSharedBlobCacheService cacheService, Client client) {
        this.cacheService = cacheService;
        this.client = client;
        this.indexingShardCacheBlobReaderChunkSize = TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(settings);
    }

    /**
     * Returns a {@link CacheBlobReader} for the given shard and the blob specified by the given {@link BlobLocation}.
     * @param shardId the shard id
     * @param blobContainer the blob container where the location's blob can be read from
     * @param location the blob's location. only the blob name, and the BCC primary term and generation are used. the offset and fileLength
     *                 is disregarded.
     * @param objectStoreUploadTracker the tracker to determine if the blob has been uploaded to the object store
     * @return a {@link CacheBlobReader} for the given shard and blob
     */
    public CacheBlobReader getCacheBlobReader(
        ShardId shardId,
        LongFunction<BlobContainer> blobContainer,
        BlobLocation location,
        ObjectStoreUploadTracker objectStoreUploadTracker
    ) {
        final var locationPrimaryTermAndGeneration = location.getBatchedCompoundCommitTermAndGeneration();
        final long rangeSize = cacheService.getRangeSize();
        var objectStoreCacheBlobReader = new ObjectStoreCacheBlobReader(
            blobContainer.apply(location.primaryTerm()),
            location.blobName(),
            rangeSize
        );
        if (objectStoreUploadTracker.isUploaded(locationPrimaryTermAndGeneration)) {
            return objectStoreCacheBlobReader;
        } else {
            var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
                shardId,
                locationPrimaryTermAndGeneration,
                client,
                indexingShardCacheBlobReaderChunkSize
            );
            return new SwitchingCacheBlobReader(
                locationPrimaryTermAndGeneration,
                objectStoreUploadTracker,
                objectStoreCacheBlobReader,
                indexingShardCacheBlobReader
            );
        }
    }
}
