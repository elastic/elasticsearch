/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public class ShardSearchRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    private static final Logger logger = LogManager.getLogger(ShardSearchRequestInterceptor.class);

    private final Function<ShardId, String> currentIndexName;

    public ShardSearchRequestInterceptor(ThreadPool threadPool) {
        this(threadPool, shardId -> null);
    }

    /**
     * @param currentIndexName the name that the index of a shard has now, or {@code null} if it is not known: the name in the request
     *                         can be the one the index had before a rename, while the access control of the request holds the names that
     *                         the data node resolved
     */
    public ShardSearchRequestInterceptor(ThreadPool threadPool, Function<ShardId, String> currentIndexName) {
        super(threadPool.getThreadContext());
        this.currentIndexName = currentIndexName;
    }

    @Override
    void disableFeatures(
        IndicesRequest indicesRequest,
        Map<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex,
        ActionListener<Void> listener
    ) {
        final ShardSearchRequest request = (ShardSearchRequest) indicesRequest;
        if (dlsUsesStoredScripts(request, indexAccessControlByIndex)) {
            logger.debug("Disable shard search request cache because DLS queries use stored scripts");
            request.requestCache(false);
        }
        listener.onResponse(null);
    }

    @Override
    String[] requestIndices(IndicesRequest indicesRequest) {
        final ShardSearchRequest request = (ShardSearchRequest) indicesRequest;
        final String requestedName = request.shardId().getIndexName();
        final String currentName = currentIndexName.apply(request.shardId());
        return currentName == null || currentName.equals(requestedName)
            ? new String[] { requestedName }
            : new String[] { currentName, requestedName };
    }

    @Override
    public boolean supports(IndicesRequest request) {
        return request instanceof ShardSearchRequest;
    }

    static boolean dlsUsesStoredScripts(
        ShardSearchRequest request,
        Map<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex
    ) {
        // the map holds the entries found for the names of the index that the request is for, which can be two after a rename
        assert indexAccessControlByIndex.isEmpty() == false : "index access control cannot be null";
        for (IndicesAccessControl.IndexAccessControl indexAccessControl : indexAccessControlByIndex.values()) {
            final DocumentPermissions documentPermissions = indexAccessControl.getDocumentPermissions();
            if (documentPermissions.hasDocumentLevelPermissions()) {
                try {
                    if (documentPermissions.hasStoredScript()) {
                        return true;
                    }
                } catch (IOException e) {
                    throw new ElasticsearchException(e);
                }
            }
        }
        return false;
    }
}
