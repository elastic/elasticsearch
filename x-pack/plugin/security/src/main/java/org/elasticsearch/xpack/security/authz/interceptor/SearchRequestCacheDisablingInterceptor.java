/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;

public class SearchRequestCacheDisablingInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    public SearchRequestCacheDisablingInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
    }

    @Override
    void disableFeatures(
        IndicesRequest indicesRequest,
        Map<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex,
        ActionListener<Void> listener
    ) {
        final SearchRequest request = (SearchRequest) indicesRequest;
        request.requestCache(false);
        listener.onResponse(null);
    }

    @Override
    public boolean supports(IndicesRequest request) {
        if (request instanceof SearchRequest searchRequest) {
            return hasRemoteIndices(searchRequest);
        } else {
            return false;
        }
    }

    // package private for test
    static boolean hasRemoteIndices(SearchRequest request) {
        return Arrays.stream(request.indices()).anyMatch(name -> name.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR) >= 0);
    }
}
