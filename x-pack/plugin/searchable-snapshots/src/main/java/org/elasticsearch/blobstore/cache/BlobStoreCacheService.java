/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;

public class BlobStoreCacheService {

    private static final Logger logger = LogManager.getLogger(BlobStoreCacheService.class);

    public static final int DEFAULT_CACHED_BLOB_SIZE = ByteSizeUnit.KB.toIntBytes(4);

    private final ThreadPool threadPool;
    private final Client client;
    private final String index;

    public BlobStoreCacheService(ThreadPool threadPool, Client client, String index) {
        this.client = new OriginSettingClient(client, SEARCHABLE_SNAPSHOTS_ORIGIN);
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
}
