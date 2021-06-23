/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.SortedMap;

public class ShardSearchRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    private final ClusterService clusterService;

    public ShardSearchRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState, ClusterService clusterService) {
        super(threadPool.getThreadContext(), licenseState);
        this.clusterService = clusterService;
    }

    @Override
    void disableFeatures(IndicesRequest indicesRequest,
                         SortedMap<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex,
                         ActionListener<Void> listener) {
        final ShardSearchRequest request = (ShardSearchRequest) indicesRequest;
        // The 7.11.2 version check is needed because request caching has a bug related to DLS/FLS
        // versions before 7.11.2. It is fixed by #69505. See also ESA-2021-08.
        if (clusterService.state().nodes().getMinNodeVersion().before(Version.V_7_11_2)) {
            request.requestCache(false);
        }
        listener.onResponse(null);
    }

    @Override
    public boolean supports(IndicesRequest request) {
        return request instanceof ShardSearchRequest;
    }
}
