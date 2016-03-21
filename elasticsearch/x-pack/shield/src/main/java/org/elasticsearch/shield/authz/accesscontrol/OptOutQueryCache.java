/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.shield.authz.InternalAuthorizationService;

/**
 * Opts out of the query cache if field level security is active for the current request.
 */
public final class OptOutQueryCache extends AbstractIndexComponent implements QueryCache {

    final IndicesQueryCache indicesQueryCache;

    public OptOutQueryCache(IndexSettings indexSettings, IndicesQueryCache indicesQueryCache) {
        super(indexSettings);
        this.indicesQueryCache = indicesQueryCache;
    }

    @Override
    public void close() throws ElasticsearchException {
        clear("close");
    }

    @Override
    public void clear(String reason) {
        logger.debug("full cache clear, reason [{}]", reason);
        indicesQueryCache.clearIndex(index().getName());
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        final RequestContext context = RequestContext.current();
        if (context == null) {
            throw new IllegalStateException("opting out of the query cache. current request can't be found");
        }
        IndicesAccessControl indicesAccessControl = context.getThreadContext().getTransient(
                InternalAuthorizationService.INDICES_PERMISSIONS_KEY);
        if (indicesAccessControl == null) {
            logger.debug("opting out of the query cache. current request doesn't hold indices permissions");
            return weight;
        }

        // At this level only IndicesRequest
        final String indexName;
        if (context.getRequest() instanceof ShardSearchRequest) {
            indexName = ((ShardSearchRequest) context.getRequest()).shardId().getIndexName();
        } else if (context.getRequest() instanceof BroadcastShardRequest) {
            indexName = ((BroadcastShardRequest) context.getRequest()).shardId().getIndexName();
        } else {
            return weight;
        }

        IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(indexName);
        if (indexAccessControl != null && indexAccessControl.getFields() != null) {
            logger.debug("opting out of the query cache. request for index [{}] has field level security enabled", indexName);
            // If in the future there is a Query#extractFields() then we can be smart on when to skip the query cache.
            // (only cache if all fields in the query also are defined in the role)
            return weight;
        } else {
            logger.trace("not opting out of the query cache. request for index [{}] has field level security disabled", indexName);
            return indicesQueryCache.doCache(weight, policy);
        }
    }
}
