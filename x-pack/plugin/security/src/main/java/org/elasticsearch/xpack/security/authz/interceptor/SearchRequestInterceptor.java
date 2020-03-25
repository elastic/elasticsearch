/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * If field level security is enabled this interceptor disables the request cache for search requests.
 */
public class SearchRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    public SearchRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
    }

    @Override
    public void disableFeatures(IndicesRequest indicesRequest, boolean fieldLevelSecurityEnabled, boolean documentLevelSecurityEnabled,
                                ActionListener<Void> listener) {
        final SearchRequest request = (SearchRequest) indicesRequest;
        request.requestCache(false);

        if (documentLevelSecurityEnabled) {
            if (request.source() != null && request.source().suggest() != null) {
                listener.onFailure(new ElasticsearchSecurityException("Suggest isn't supported if document level security is enabled",
                        RestStatus.BAD_REQUEST));
            } else if (request.source() != null && request.source().profile()) {
                listener.onFailure(new ElasticsearchSecurityException("A search request cannot be profiled if document level security " +
                    "is enabled", RestStatus.BAD_REQUEST));
            } else {
                listener.onResponse(null);
            }
        } else {
            listener.onResponse(null);
        }
    }

    @Override
    public boolean supports(IndicesRequest request) {
        return request instanceof SearchRequest;
    }
}
