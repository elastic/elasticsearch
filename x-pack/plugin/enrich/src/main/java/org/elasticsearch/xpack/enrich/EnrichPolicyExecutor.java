/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.enrich;

import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.EnrichPolicyExecutionTask;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;

public class EnrichPolicyExecutor {

    private final ClusterService clusterService;
    private final Client client;
    private final TaskManager taskManager;
    private final ThreadPool threadPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final LongSupplier nowSupplier;
    private final int fetchSize;
    private final EnrichPolicyLocks policyLocks;
    private final int maximumConcurrentPolicyExecutions;
    private final Semaphore policyExecutionPermits;

    EnrichPolicyExecutor(Settings settings,
                         ClusterService clusterService,
                         Client client,
                         TaskManager taskManager,
                         ThreadPool threadPool,
                         IndexNameExpressionResolver indexNameExpressionResolver,
                         EnrichPolicyLocks policyLocks,
                         LongSupplier nowSupplier) {
        this.clusterService = clusterService;
        this.client = client;
        this.taskManager = taskManager;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
        this.policyLocks = policyLocks;
        this.fetchSize = EnrichPlugin.ENRICH_FETCH_SIZE_SETTING.get(settings);
        this.maximumConcurrentPolicyExecutions = EnrichPlugin.ENRICH_MAX_CONCURRENT_POLICY_EXECUTIONS.get(settings);
        this.policyExecutionPermits = new Semaphore(maximumConcurrentPolicyExecutions);
    }

    private void tryLockingPolicy(String policyName) {
        policyLocks.lockPolicy(policyName);
        if (policyExecutionPermits.tryAcquire() == false) {
            // Release policy lock, and throw a different exception
            policyLocks.releasePolicy(policyName);
            throw new EsRejectedExecutionException("Policy execution failed. Policy execution for [" + policyName + "] would exceed " +
                "maximum concurrent policy executions [" + maximumConcurrentPolicyExecutions + "]");
        }
    }

    private void releasePolicy(String policyName) {
        try {
            policyExecutionPermits.release();
        } finally {
            policyLocks.releasePolicy(policyName);
        }
    }

    private class PolicyCompletionListener implements ActionListener<EnrichPolicyExecutionTask.Status> {
        private final String policyName;
        private final EnrichPolicyExecutionTask task;
        private final BiConsumer<Task, EnrichPolicyExecutionTask.Status> onResponse;
        private final BiConsumer<Task, Exception> onFailure;

        PolicyCompletionListener(String policyName, EnrichPolicyExecutionTask task,
                                 BiConsumer<Task, EnrichPolicyExecutionTask.Status> onResponse, BiConsumer<Task, Exception> onFailure) {
            this.policyName = policyName;
            this.task = task;
            this.onResponse = onResponse;
            this.onFailure = onFailure;
        }

        @Override
        public void onResponse(EnrichPolicyExecutionTask.Status status) {
            assert EnrichPolicyExecutionTask.PolicyPhases.COMPLETE.equals(status.getPhase()) : "incomplete task returned";
            releasePolicy(policyName);
            try {
                taskManager.unregister(task);
            } finally {
                onResponse.accept(task, status);
            }
        }

        @Override
        public void onFailure(Exception e) {
            // Set task status to failed to avoid having to catch and rethrow exceptions everywhere
            task.setStatus(new EnrichPolicyExecutionTask.Status(EnrichPolicyExecutionTask.PolicyPhases.FAILED));
            releasePolicy(policyName);
            try {
                taskManager.unregister(task);
            } finally {
                onFailure.accept(task, e);
            }
        }
    }

    protected Runnable createPolicyRunner(String policyName, EnrichPolicy policy, EnrichPolicyExecutionTask task,
                                          ActionListener<EnrichPolicyExecutionTask.Status> listener) {
        return new EnrichPolicyRunner(policyName, policy, task, listener, clusterService, client, indexNameExpressionResolver, nowSupplier,
            fetchSize);
    }

    private EnrichPolicy getPolicy(ExecuteEnrichPolicyAction.Request request) {
        // Look up policy in policy store and execute it
        EnrichPolicy policy = EnrichStore.getPolicy(request.getName(), clusterService.state());
        if (policy == null) {
            throw new IllegalArgumentException("Policy execution failed. Could not locate policy with id [" + request.getName() + "]");
        }
        return policy;
    }

    public void runPolicy(ExecuteEnrichPolicyAction.Request request, ActionListener<EnrichPolicyExecutionTask.Status> listener) {
        runPolicy(request, getPolicy(request), listener);
    }

    public void runPolicy(ExecuteEnrichPolicyAction.Request request, TaskListener<EnrichPolicyExecutionTask.Status> listener) {
        runPolicy(request, getPolicy(request), listener);
    }

    public Task runPolicy(ExecuteEnrichPolicyAction.Request request, EnrichPolicy policy,
                          ActionListener<EnrichPolicyExecutionTask.Status> listener) {
        return runPolicy(request, policy, (t, r) -> listener.onResponse(r), (t, e) -> listener.onFailure(e));
    }

    public Task runPolicy(ExecuteEnrichPolicyAction.Request request, EnrichPolicy policy,
                          TaskListener<EnrichPolicyExecutionTask.Status> listener) {
        return runPolicy(request, policy, listener::onResponse, listener::onFailure);
    }

    private Task runPolicy(ExecuteEnrichPolicyAction.Request request, EnrichPolicy policy,
                           BiConsumer<Task, EnrichPolicyExecutionTask.Status> onResponse, BiConsumer<Task, Exception> onFailure) {
        tryLockingPolicy(request.getName());
        try {
            return runPolicyTask(request, policy, onResponse, onFailure);
        } catch (Exception e) {
            // Be sure to unlock if submission failed.
            releasePolicy(request.getName());
            throw e;
        }
    }

    private Task runPolicyTask(ExecuteEnrichPolicyAction.Request request, EnrichPolicy policy,
                               BiConsumer<Task, EnrichPolicyExecutionTask.Status> onResponse, BiConsumer<Task, Exception> onFailure) {
        Task asyncTask = taskManager.register("enrich", "", request);
        EnrichPolicyExecutionTask task = (EnrichPolicyExecutionTask) asyncTask;
        try {
            task.setStatus(new EnrichPolicyExecutionTask.Status(EnrichPolicyExecutionTask.PolicyPhases.SCHEDULED));
            PolicyCompletionListener completionListener = new PolicyCompletionListener(request.getName(), task, onResponse, onFailure);
            Runnable runnable = createPolicyRunner(request.getName(), policy, task, completionListener);
            threadPool.executor(ThreadPool.Names.GENERIC).execute(runnable);
            return asyncTask;
        } catch (Exception e) {
            // Unregister task in case of exception
            task.setStatus(new EnrichPolicyExecutionTask.Status(EnrichPolicyExecutionTask.PolicyPhases.FAILED));
            taskManager.unregister(asyncTask);
            throw e;
        }
    }
}
