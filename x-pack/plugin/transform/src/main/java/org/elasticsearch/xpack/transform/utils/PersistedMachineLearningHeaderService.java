/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.HashMap;
import java.util.Map;

public class PersistedMachineLearningHeaderService {
    private static final String SERVERLESS_AUTHENTICATING_TOKEN_HEADER = "_security_serverless_authenticating_token";

    private final ThreadPool threadPool;
    private final SecurityContext securityContext;
    private final CrossProjectModeDecider crossProjectModeDecider;

    public PersistedMachineLearningHeaderService(
        ThreadPool threadPool,
        SecurityContext securityContext,
        CrossProjectModeDecider crossProjectModeDecider
    ) {
        this.threadPool = threadPool;
        this.securityContext = securityContext;
        this.crossProjectModeDecider = crossProjectModeDecider;
    }

    public Map<String, String> getPersistedHeaders(ClusterState clusterState) {
        SetOnce<Map<String, String>> persistedHeadersHolder = new SetOnce<>();
        SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable(securityContext, () -> {
            Map<String, String> persistedHeaders = ClientHelper.getPersistableSafeSecurityHeaders(
                threadPool.getThreadContext(),
                clusterState
            );
            if (crossProjectModeDecider.crossProjectEnabled() && TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled()) {
                String authenticatingToken = threadPool.getThreadContext().getHeader(SERVERLESS_AUTHENTICATING_TOKEN_HEADER);
                if (authenticatingToken != null) {
                    HashMap<String, String> mutableHeaders = new HashMap<>(persistedHeaders);
                    mutableHeaders.put(SERVERLESS_AUTHENTICATING_TOKEN_HEADER, authenticatingToken);
                    persistedHeaders = Map.copyOf(mutableHeaders);
                }
            }
            persistedHeadersHolder.set(persistedHeaders);
        });
        return persistedHeadersHolder.get();
    }
}
