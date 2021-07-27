/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
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

    public static final Version VERSION_SHARD_SEARCH_INTERCEPTOR = Version.V_7_11_2;
    private final ClusterService clusterService;

    public SearchRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState, ClusterService clusterService) {
        super(threadPool.getThreadContext(), licenseState);
        this.clusterService = clusterService;
    }

    @Override
    void disableFeatures(IndicesRequest indicesRequest,
                         Map<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex,
                         ActionListener<Void> listener) {
        final SearchRequest request = (SearchRequest) indicesRequest;
        // The 7.11.2 version check is needed because request caching has a bug related to DLS/FLS
        // versions before 7.11.2. It is fixed by #69505. See also ESA-2021-08.
        // TODO: The version check can be removed in 8.0 because 7.last will have support for request caching with DLS/FLS
        if (clusterService.state().nodes().getMinNodeVersion().before(VERSION_SHARD_SEARCH_INTERCEPTOR) || hasRemoteIndices(request)) {
            request.requestCache(false);
        }

        final SearchSourceBuilder source = request.source();

        if (indexAccessControlByIndex.values().stream().anyMatch(iac -> iac.getDocumentPermissions().hasDocumentLevelPermissions())) {
            if (source != null && source.suggest() != null) {
                listener.onFailure(new ElasticsearchSecurityException("Suggest isn't supported if document level security is enabled",
                    RestStatus.BAD_REQUEST));
            } else if (source != null && source.profile()) {
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
        // unfortunately we have to skip these
        return request instanceof SearchRequest && ((SearchRequest) request).pointInTimeBuilder() == null;
    }

    // package private for test
    boolean hasRemoteIndices(SearchRequest request) {
        return Arrays.stream(request.indices()).anyMatch(name -> name.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR) >= 0);
    }
}
