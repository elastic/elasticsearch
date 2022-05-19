/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.enrich.action.InternalExecutePolicyAction;

import java.util.concurrent.Semaphore;
import java.util.function.LongSupplier;

public class EnrichPolicyExecutor {

    public static final String TASK_ACTION = "policy_execution";

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final LongSupplier nowSupplier;
    private final int fetchSize;
    private final EnrichPolicyLocks policyLocks;
    private final int maximumConcurrentPolicyExecutions;
    private final int maxForceMergeAttempts;
    private final Semaphore policyExecutionPermits;

    public EnrichPolicyExecutor(
        Settings settings,
        ClusterService clusterService,
        Client client,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver,
        EnrichPolicyLocks policyLocks,
        LongSupplier nowSupplier
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
        this.policyLocks = policyLocks;
        this.fetchSize = EnrichPlugin.ENRICH_FETCH_SIZE_SETTING.get(settings);
        this.maximumConcurrentPolicyExecutions = EnrichPlugin.ENRICH_MAX_CONCURRENT_POLICY_EXECUTIONS.get(settings);
        this.maxForceMergeAttempts = EnrichPlugin.ENRICH_MAX_FORCE_MERGE_ATTEMPTS.get(settings);
        this.policyExecutionPermits = new Semaphore(maximumConcurrentPolicyExecutions);
    }

    public void coordinatePolicyExecution(
        ExecuteEnrichPolicyAction.Request request,
        ActionListener<ExecuteEnrichPolicyAction.Response> listener
    ) {
        tryLockingPolicy(request.getName());
        try {
            client.execute(InternalExecutePolicyAction.INSTANCE, request, ActionListener.wrap(response -> {
                if (response.getStatus() != null) {
                    releasePolicy(request.getName());
                    listener.onResponse(response);
                } else {
                    waitAndThenRelease(request.getName(), response);
                    listener.onResponse(response);
                }
            }, e -> {
                releasePolicy(request.getName());
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            // Be sure to unlock if submission failed.
            releasePolicy(request.getName());
            throw e;
        }
    }

    public void runPolicyLocally(ExecuteEnrichPolicyTask task, String policyName, ActionListener<ExecuteEnrichPolicyStatus> listener) {
        try {
            EnrichPolicy policy = EnrichStore.getPolicy(policyName, clusterService.state());
            if (policy == null) {
                throw new ResourceNotFoundException("policy [{}] does not exist", policyName);
            }

            task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.SCHEDULED));
            Runnable runnable = createPolicyRunner(policyName, policy, task, listener);
            threadPool.executor(ThreadPool.Names.GENERIC).execute(runnable);
        } catch (Exception e) {
            task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.FAILED));
            throw e;
        }
    }

    private void tryLockingPolicy(String policyName) {
        policyLocks.lockPolicy(policyName);
        if (policyExecutionPermits.tryAcquire() == false) {
            // Release policy lock, and throw a different exception
            policyLocks.releasePolicy(policyName);
            throw new EsRejectedExecutionException(
                "Policy execution failed. Policy execution for ["
                    + policyName
                    + "] would exceed "
                    + "maximum concurrent policy executions ["
                    + maximumConcurrentPolicyExecutions
                    + "]"
            );
        }
    }

    private void releasePolicy(String policyName) {
        try {
            policyExecutionPermits.release();
        } finally {
            policyLocks.releasePolicy(policyName);
        }
    }

    private void waitAndThenRelease(String policyName, ExecuteEnrichPolicyAction.Response response) {
        GetTaskRequest getTaskRequest = new GetTaskRequest();
        getTaskRequest.setTaskId(response.getTaskId());
        getTaskRequest.setWaitForCompletion(true);
        client.admin().cluster().getTask(getTaskRequest, ActionListener.wrap(() -> releasePolicy(policyName)));
    }

    private Runnable createPolicyRunner(
        String policyName,
        EnrichPolicy policy,
        ExecuteEnrichPolicyTask task,
        ActionListener<ExecuteEnrichPolicyStatus> listener
    ) {
        return new EnrichPolicyRunner(
            policyName,
            policy,
            task,
            listener,
            clusterService,
            client,
            indexNameExpressionResolver,
            nowSupplier,
            fetchSize,
            maxForceMergeAttempts
        );
    }

}
