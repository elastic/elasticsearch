/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.enrich;

import java.util.function.LongSupplier;

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
    private final EnrichPolicyLocks policyLocks;

    EnrichPolicyExecutor(Settings settings,
                         ClusterService clusterService,
                         Client client,
                         ThreadPool threadPool,
                         IndexNameExpressionResolver indexNameExpressionResolver,
                         EnrichPolicyLocks policyLocks,
                         LongSupplier nowSupplier) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
        this.policyLocks = policyLocks;
        this.fetchSize = EnrichPlugin.ENRICH_FETCH_SIZE_SETTING.get(settings);
    }

    private class PolicyUnlockingListener implements ActionListener<PolicyExecutionResult> {
        private final String policyName;
        private final ActionListener<PolicyExecutionResult> listener;

        PolicyUnlockingListener(String policyName, ActionListener<PolicyExecutionResult> listener) {
            this.policyName = policyName;
            this.listener = listener;
        }

        @Override
        public void onResponse(PolicyExecutionResult policyExecutionResult) {
            policyLocks.releasePolicy(policyName);
            listener.onResponse(policyExecutionResult);
        }

        @Override
        public void onFailure(Exception e) {
            policyLocks.releasePolicy(policyName);
            listener.onFailure(e);
        }
    }

    protected Runnable createPolicyRunner(String policyName, EnrichPolicy policy, ActionListener<PolicyExecutionResult> listener) {
        return new EnrichPolicyRunner(policyName, policy, listener, clusterService, client, indexNameExpressionResolver, nowSupplier,
            fetchSize);
    }

    public void runPolicy(String policyId, ActionListener<PolicyExecutionResult> listener) {
        // Look up policy in policy store and execute it
        EnrichPolicy policy = EnrichStore.getPolicy(policyId, clusterService.state());
        if (policy == null) {
            throw new IllegalArgumentException("Policy execution failed. Could not locate policy with id [" + policyId + "]");
        } else {
            runPolicy(policyId, policy, listener);
        }
    }

    public void runPolicy(String policyName, EnrichPolicy policy, ActionListener<PolicyExecutionResult> listener) {
        policyLocks.lockPolicy(policyName);
        try {
            Runnable runnable = createPolicyRunner(policyName, policy, new PolicyUnlockingListener(policyName, listener));
            threadPool.executor(ThreadPool.Names.GENERIC).execute(runnable);
        } catch (Exception e) {
            // Be sure to unlock if submission failed.
            policyLocks.releasePolicy(policyName);
            throw e;
        }
    }
}
