/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobstore.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.xpack.searchablesnapshots.cache.ByteRange;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;

public class BlobStoreCacheService {

    private static final Logger logger = LogManager.getLogger(BlobStoreCacheService.class);

    /**
     * Before 8.0.0 blobs were cached using a 4KB or 8KB maximum length.
     */
    private static final Version OLD_CACHED_BLOB_SIZE_VERSION = Version.V_8_0_0; // TODO adjust after backport

    public static final int DEFAULT_CACHED_BLOB_SIZE = ByteSizeUnit.KB.toIntBytes(1);
    private static final Cache<String, String> LOG_EXCEEDING_FILES_CACHE = CacheBuilder.<String, String>builder()
        .setExpireAfterAccess(TimeValue.timeValueMinutes(60L))
        .build();

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Client client;
    private final String index;

    public BlobStoreCacheService(ClusterService clusterService, ThreadPool threadPool, Client client, String index) {
        this.client = new OriginSettingClient(client, SEARCHABLE_SNAPSHOTS_ORIGIN);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.index = index;
    }

    public CachedBlob get(String repository, String name, String path, long offset) {
        assert Thread.currentThread().getName().contains('[' + ThreadPool.Names.SYSTEM_READ + ']') == false : "must not block ["
            + Thread.currentThread().getName()
            + "] for a cache read";

        final PlainActionFuture<CachedBlob> future = PlainActionFuture.newFuture();
        getAsync(repository, name, path, offset, future);
        try {
            return future.actionGet(5, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            if (logger.isDebugEnabled()) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "get from cache index timed out after [5s], retrieving from blob store instead [id={}]",
                        CachedBlob.generateId(repository, name, path, offset)
                    ),
                    e
                );
            } else {
                logger.warn("get from cache index timed out after [5s], retrieving from blob store instead");
            }
            return CachedBlob.CACHE_NOT_READY;
        }
    }

    protected void getAsync(String repository, String name, String path, long offset, ActionListener<CachedBlob> listener) {
        final GetRequest request = new GetRequest(index).id(CachedBlob.generateId(repository, name, path, offset));
        client.get(request, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse response) {
                if (response.isExists()) {
                    logger.debug("cache hit : [{}]", request.id());
                    assert response.isSourceEmpty() == false;

                    final CachedBlob cachedBlob = CachedBlob.fromSource(response.getSource());
                    assert response.getId().equals(cachedBlob.generatedId());
                    listener.onResponse(cachedBlob);
                } else {
                    logger.debug("cache miss: [{}]", request.id());
                    listener.onResponse(CachedBlob.CACHE_MISS);
                }
            }

            @Override
            public void onFailure(Exception e) {
                // In case the blob cache system index is unavailable, we indicate it's not ready and move on. We do not fail the request:
                // a failure here is not fatal since the data exists in the blob store, so we can simply indicate the cache is not ready.
                if (isExpectedCacheGetException(e)) {
                    logger.debug(() -> new ParameterizedMessage("failed to retrieve cached blob from system index [{}]", index), e);
                } else {
                    logger.warn(() -> new ParameterizedMessage("failed to retrieve cached blob from system index [{}]", index), e);
                    assert false : e;
                }
                listener.onResponse(CachedBlob.CACHE_NOT_READY);
            }
        });
    }

    private static boolean isExpectedCacheGetException(Exception e) {
        if (TransportActions.isShardNotAvailableException(e)
            || e instanceof ConnectTransportException
            || e instanceof ClusterBlockException) {
            return true;
        }
        final Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause instanceof NodeClosedException || cause instanceof ConnectTransportException;
    }

    public void putAsync(String repository, String name, String path, long offset, BytesReference content, ActionListener<Void> listener) {
        try {
            final CachedBlob cachedBlob = new CachedBlob(
                Instant.ofEpochMilli(threadPool.absoluteTimeInMillis()),
                Version.CURRENT,
                repository,
                name,
                path,
                content,
                offset
            );
            final IndexRequest request = new IndexRequest(index).id(cachedBlob.generatedId());
            try (XContentBuilder builder = jsonBuilder()) {
                request.source(cachedBlob.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }

            client.index(request, new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    logger.trace("cache fill ({}): [{}]", indexResponse.status(), request.id());
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(new ParameterizedMessage("failure in cache fill: [{}]", request.id()), e);
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("cache fill failure: [{}]", CachedBlob.generateId(repository, name, path, offset)), e);
            listener.onFailure(e);
        }
    }

    private static final Set<String> METADATA_FILES_EXTENSIONS;
    private static final Set<String> OTHER_FILES_EXTENSIONS;
    static {
        // List of Lucene file extensions that are considered as "metadata" and should therefore be fully cached in the blob store cache.
        // Those files are usually fully read by Lucene when it opens a Directory.
        METADATA_FILES_EXTENSIONS = Set.of(
            "cfe", // compound file's entry table
            "dvm", // doc values metadata file
            "fdm", // stored fields metadata file
            "fnm", // field names metadata file
            "kdm", // Lucene 8.6 point format metadata file
            "nvm", // norms metadata file
            "tmd", // Lucene 8.6 terms metadata file
            "tvm", // terms vectors metadata file
            "vem"  // Lucene 9.0 indexed vectors metadata
        );

        // List of extensions for which Lucene usually only reads the first 1024 byte and checks a header checksum when opening a Directory.
        OTHER_FILES_EXTENSIONS = Set.of(
            "cfs",
            "dii",
            "dim",
            "doc",
            "dvd",
            "fdt",
            "fdx",
            "kdd",
            "kdi",
            "liv",
            "nvd",
            "pay",
            "pos",
            "tim",
            "tip",
            "tvd",
            "tvx",
            "vec"
        );
        assert Sets.intersection(METADATA_FILES_EXTENSIONS, OTHER_FILES_EXTENSIONS).isEmpty();
    }

    /**
     * Computes the {@link ByteRange} corresponding to the header of a Lucene file. This range can vary depending of the type of the file
     * which is indicated by the file's extension. The returned byte range can never be larger than the file's length but it can be smaller.
     *
     * For files that are declared as metadata files in {@link #METADATA_FILES_EXTENSIONS}, the header can be as large as the specified
     * maximum metadata length parameter {@code maxMetadataLength}. Non-metadata files have a fixed length header of maximum 1KB.
     *
     * @param fileName the name of the file
     * @param fileLength the length of the file
     * @param maxMetadataLength the maximum accepted length for metadata files
     *
     * @return the header {@link ByteRange}
     */
    public ByteRange computeBlobCacheByteRange(String fileName, long fileLength, ByteSizeValue maxMetadataLength) {
        final String fileExtension = IndexFileNames.getExtension(fileName);
        assert fileExtension == null || METADATA_FILES_EXTENSIONS.contains(fileExtension) || OTHER_FILES_EXTENSIONS.contains(fileExtension)
            : "unknown Lucene file extension [" + fileExtension + "] - should it be considered a metadata file?";

        if (useLegacyCachedBlobSizes()) {
            if (fileLength <= ByteSizeUnit.KB.toBytes(8L)) {
                return ByteRange.of(0L, fileLength);
            } else {
                return ByteRange.of(0L, ByteSizeUnit.KB.toBytes(4L));
            }
        }

        if (METADATA_FILES_EXTENSIONS.contains(fileExtension)) {
            final long maxAllowedLengthInBytes = maxMetadataLength.getBytes();
            if (fileLength > maxAllowedLengthInBytes) {
                logExceedingFile(fileExtension, fileLength, maxMetadataLength);
            }
            return ByteRange.of(0L, Math.min(fileLength, maxAllowedLengthInBytes));
        }
        return ByteRange.of(0L, Math.min(fileLength, DEFAULT_CACHED_BLOB_SIZE));
    }

    protected boolean useLegacyCachedBlobSizes() {
        final Version minNodeVersion = clusterService.state().nodes().getMinNodeVersion();
        return minNodeVersion.before(OLD_CACHED_BLOB_SIZE_VERSION);
    }

    private static void logExceedingFile(String extension, long length, ByteSizeValue maxAllowedLength) {
        if (logger.isWarnEnabled()) {
            try {
                // Use of a cache to prevent too many log traces per hour
                LOG_EXCEEDING_FILES_CACHE.computeIfAbsent(extension, key -> {
                    logger.warn(
                        "file with extension [{}] is larger ([{}]) than the max. length allowed [{}] to cache metadata files in blob cache",
                        extension,
                        length,
                        maxAllowedLength
                    );
                    return key;
                });
            } catch (ExecutionException e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Failed to log information about exceeding file type [{}] with length [{}]",
                        extension,
                        length
                    ),
                    e
                );
            }
        }
    }
}
