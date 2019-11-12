/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.enrich;

import java.util.Map;
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
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;

public class EnrichPolicyExecutor {

    public static final String TASK_ACTION = "policy_execution";

    private final ClusterService clusterService;
    private final Client client;
    private final TaskManager taskManager;
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
        TaskManager taskManager,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver,
        EnrichPolicyLocks policyLocks,
        LongSupplier nowSupplier
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.taskManager = taskManager;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
        this.policyLocks = policyLocks;
        this.fetchSize = EnrichPlugin.ENRICH_FETCH_SIZE_SETTING.get(settings);
        this.maximumConcurrentPolicyExecutions = EnrichPlugin.ENRICH_MAX_CONCURRENT_POLICY_EXECUTIONS.get(settings);
        this.maxForceMergeAttempts = EnrichPlugin.ENRICH_MAX_FORCE_MERGE_ATTEMPTS.get(settings);
        this.policyExecutionPermits = new Semaphore(maximumConcurrentPolicyExecutions);
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

    private class PolicyCompletionListener implements ActionListener<ExecuteEnrichPolicyStatus> {
        private final String policyName;
        private final ExecuteEnrichPolicyTask task;
        private final BiConsumer<Task, ExecuteEnrichPolicyStatus> onResponse;
        private final BiConsumer<Task, Exception> onFailure;

        PolicyCompletionListener(
            String policyName,
            ExecuteEnrichPolicyTask task,
            BiConsumer<Task, ExecuteEnrichPolicyStatus> onResponse,
            BiConsumer<Task, Exception> onFailure
        ) {
            this.policyName = policyName;
            this.task = task;
            this.onResponse = onResponse;
            this.onFailure = onFailure;
        }

        @Override
        public void onResponse(ExecuteEnrichPolicyStatus status) {
            assert ExecuteEnrichPolicyStatus.PolicyPhases.COMPLETE.equals(status.getPhase()) : "incomplete task returned";
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
            task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.FAILED));
            releasePolicy(policyName);
            try {
                taskManager.unregister(task);
            } finally {
                onFailure.accept(task, e);
            }
        }
    }

    protected Runnable createPolicyRunner(
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

    private EnrichPolicy getPolicy(ExecuteEnrichPolicyAction.Request request) {
        // Look up policy in policy store and execute it
        EnrichPolicy policy = EnrichStore.getPolicy(request.getName(), clusterService.state());
        if (policy == null) {
            throw new IllegalArgumentException("Policy execution failed. Could not locate policy with id [" + request.getName() + "]");
        }
        return policy;
    }

    public Task runPolicy(ExecuteEnrichPolicyAction.Request request, ActionListener<ExecuteEnrichPolicyStatus> listener) {
        return runPolicy(request, getPolicy(request), listener);
    }

    public Task runPolicy(ExecuteEnrichPolicyAction.Request request, TaskListener<ExecuteEnrichPolicyStatus> listener) {
        return runPolicy(request, getPolicy(request), listener);
    }

    public Task runPolicy(
        ExecuteEnrichPolicyAction.Request request,
        EnrichPolicy policy,
        ActionListener<ExecuteEnrichPolicyStatus> listener
    ) {
        return runPolicy(request, policy, (t, r) -> listener.onResponse(r), (t, e) -> listener.onFailure(e));
    }

    public Task runPolicy(
        ExecuteEnrichPolicyAction.Request request,
        EnrichPolicy policy,
        TaskListener<ExecuteEnrichPolicyStatus> listener
    ) {
        return runPolicy(request, policy, listener::onResponse, listener::onFailure);
    }

    private Task runPolicy(
        ExecuteEnrichPolicyAction.Request request,
        EnrichPolicy policy,
        BiConsumer<Task, ExecuteEnrichPolicyStatus> onResponse,
        BiConsumer<Task, Exception> onFailure
    ) {
        tryLockingPolicy(request.getName());
        try {
            return runPolicyTask(request, policy, onResponse, onFailure);
        } catch (Exception e) {
            // Be sure to unlock if submission failed.
            releasePolicy(request.getName());
            throw e;
        }
    }

    private Task runPolicyTask(
        final ExecuteEnrichPolicyAction.Request request,
        EnrichPolicy policy,
        BiConsumer<Task, ExecuteEnrichPolicyStatus> onResponse,
        BiConsumer<Task, Exception> onFailure
    ) {
        Task asyncTask = taskManager.register("enrich", TASK_ACTION, new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {
                request.setParentTask(taskId);
            }

            @Override
            public TaskId getParentTask() {
                return request.getParentTask();
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new ExecuteEnrichPolicyTask(id, type, action, getDescription(), parentTaskId, headers);
            }

            @Override
            public String getDescription() {
                return request.getName();
            }
        });
        ExecuteEnrichPolicyTask task = (ExecuteEnrichPolicyTask) asyncTask;
        try {
            task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.SCHEDULED));
            PolicyCompletionListener completionListener = new PolicyCompletionListener(request.getName(), task, onResponse, onFailure);
            Runnable runnable = createPolicyRunner(request.getName(), policy, task, completionListener);
            threadPool.executor(ThreadPool.Names.GENERIC).execute(runnable);
            return asyncTask;
        } catch (Exception e) {
            // Unregister task in case of exception
            task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.FAILED));
            taskManager.unregister(asyncTask);
            throw e;
        }
    }
}
