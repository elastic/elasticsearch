/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.enrich;

import java.util.function.LongSupplier;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

public class EnrichPolicyExecutor {

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final LongSupplier nowSupplier;
    private final int fetchSize;

    EnrichPolicyExecutor(Settings settings,
                         ClusterService clusterService,
                         Client client,
                         ThreadPool threadPool,
                         IndexNameExpressionResolver indexNameExpressionResolver,
                         LongSupplier nowSupplier) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
        this.fetchSize = EnrichPlugin.ENRICH_FETCH_SIZE_SETTING.get(settings);
    }

    public void runPolicy(String policyId, ActionListener<PolicyExecutionResult> listener) {
        // Look up policy in policy store and execute it
        EnrichPolicy policy = EnrichStore.getPolicy(policyId, clusterService.state());
        if (policy == null) {
            throw new ElasticsearchException("Policy execution failed. Could not locate policy with id [{}]", policyId);
        } else {
            runPolicy(policyId, policy, listener);
        }
    }

    public void runPolicy(String policyName, EnrichPolicy policy, ActionListener<PolicyExecutionResult> listener) {
        EnrichPolicyRunner runnable = new EnrichPolicyRunner(policyName, policy, listener, clusterService, client,
            indexNameExpressionResolver, nowSupplier, fetchSize);
        threadPool.executor(ThreadPool.Names.GENERIC).execute(runnable);
    }
}
