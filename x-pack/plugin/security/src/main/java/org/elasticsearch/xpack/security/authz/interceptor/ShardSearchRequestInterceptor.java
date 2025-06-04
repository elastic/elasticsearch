/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;

import java.io.IOException;
import java.util.Map;

public class ShardSearchRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    private static final Logger logger = LogManager.getLogger(ShardSearchRequestInterceptor.class);

    public ShardSearchRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
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
        return new String[] { request.shardId().getIndexName() };
    }

    @Override
    public boolean supports(IndicesRequest request) {
        return request instanceof ShardSearchRequest;
    }

    static boolean dlsUsesStoredScripts(
        ShardSearchRequest request,
        Map<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex
    ) {
        final String indexName = request.shardId().getIndexName();
        final IndicesAccessControl.IndexAccessControl indexAccessControl = indexAccessControlByIndex.get(indexName);
        assert indexAccessControl != null : "index access control cannot be null";
        final DocumentPermissions documentPermissions = indexAccessControl.getDocumentPermissions();
        if (documentPermissions.hasDocumentLevelPermissions()) {
            try {
                return documentPermissions.hasStoredScript();
            } catch (IOException e) {
                throw new ElasticsearchException(e);
            }
        } else {
            return false;
        }
    }
}
