/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

/**
 * If field level security is enabled this interceptor disables the request cache for search requests.
 */
public class SearchRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor<SearchRequest> {

    public SearchRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
    }

    @Override
    public void disableFeatures(SearchRequest request, boolean fieldLevelSecurityEnabled, boolean documentLevelSecurityEnabled) {
        request.requestCache(false);

        if (documentLevelSecurityEnabled) {
            if (request.source() != null && request.source().suggest() != null) {
                throw new ElasticsearchSecurityException("Suggest isn't supported if document level security is enabled",
                        RestStatus.BAD_REQUEST);
            }
            if (request.source() != null && request.source().profile()) {
                throw new ElasticsearchSecurityException("A search request cannot be profiled if document level security is enabled",
                        RestStatus.BAD_REQUEST);
            }
        }
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof SearchRequest;
    }
}
