/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;

public class SearchRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    private final ClusterService clusterService;

    public SearchRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState, ClusterService clusterService) {
        super(threadPool.getThreadContext(), licenseState);
        this.clusterService = clusterService;
    }

    @Override
    void disableFeatures(
        IndicesRequest indicesRequest,
        Map<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex,
        ActionListener<Void> listener
    ) {
        final SearchRequest request = (SearchRequest) indicesRequest;
        if (hasRemoteIndices(request)) {
            request.requestCache(false);
        }

        final SearchSourceBuilder source = request.source();

        if (indexAccessControlByIndex.values().stream().anyMatch(iac -> iac.getDocumentPermissions().hasDocumentLevelPermissions())) {
            if (source != null && source.suggest() != null) {
                listener.onFailure(
                    new ElasticsearchSecurityException(
                        "Suggest isn't supported if document level security is enabled",
                        RestStatus.BAD_REQUEST
                    )
                );
            } else if (source != null && source.profile()) {
                listener.onFailure(
                    new ElasticsearchSecurityException(
                        "A search request cannot be profiled if document level security " + "is enabled",
                        RestStatus.BAD_REQUEST
                    )
                );
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

    // package private for test
    static boolean hasRemoteIndices(SearchRequest request) {
        return Arrays.stream(request.indices()).anyMatch(name -> name.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR) >= 0);
    }
}
