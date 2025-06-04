/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Map;

public class SearchRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    public SearchRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
    }

    @Override
    void disableFeatures(
        IndicesRequest indicesRequest,
        Map<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex,
        ActionListener<Void> listener
    ) {
        final SearchRequest request = (SearchRequest) indicesRequest;
        if (indexAccessControlByIndex.values().stream().anyMatch(iac -> iac.getDocumentPermissions().hasDocumentLevelPermissions())) {
            if (hasSuggest(request)) {
                listener.onFailure(
                    new ElasticsearchSecurityException(
                        "Suggest isn't supported if document level security is enabled",
                        RestStatus.BAD_REQUEST
                    )
                );
            } else if (hasProfile(request)) {
                listener.onFailure(
                    new ElasticsearchSecurityException(
                        "A search request cannot be profiled if document level security " + "is enabled",
                        RestStatus.BAD_REQUEST
                    )
                );
            } else {
                if (hasZeroMinDocTermsAggregation(request)) {
                    assert request.source() != null && request.source().aggregations() != null;
                    request.source().aggregations().forceTermsAggsToExcludeDeletedDocs();
                }

                listener.onResponse(null);
            }
        } else {
            listener.onResponse(null);
        }
    }

    @Override
    public boolean supports(IndicesRequest request) {
        if (request instanceof SearchRequest searchRequest) {
            return hasSuggest(searchRequest) || hasProfile(searchRequest) || hasZeroMinDocTermsAggregation(searchRequest);
        } else {
            return false;
        }
    }

    private static boolean hasSuggest(SearchRequest searchRequest) {
        return searchRequest.source() != null && searchRequest.source().suggest() != null;
    }

    private static boolean hasProfile(SearchRequest searchRequest) {
        return searchRequest.source() != null && searchRequest.source().profile();
    }

    private static boolean hasZeroMinDocTermsAggregation(SearchRequest searchRequest) {
        return searchRequest.source() != null
            && searchRequest.source().aggregations() != null
            && searchRequest.source().aggregations().hasZeroMinDocTermsAggregation();
    }

}
