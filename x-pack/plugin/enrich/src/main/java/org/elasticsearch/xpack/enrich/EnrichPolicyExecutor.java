/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks.EnrichPolicyLock;
import org.elasticsearch.xpack.enrich.action.InternalExecutePolicyAction;
import org.elasticsearch.xpack.enrich.action.InternalExecutePolicyAction.Request;

import java.util.concurrent.Semaphore;
import java.util.function.LongSupplier;

public class EnrichPolicyExecutor {

    private static final Logger logger = LogManager.getLogger(EnrichPolicyExecutor.class);

    public static final String TASK_ACTION = "policy_execution";

    private final ClusterService clusterService;
    private final IndicesService indicesService;
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
        IndicesService indicesService,
        Client client,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver,
        EnrichPolicyLocks policyLocks,
        LongSupplier nowSupplier
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
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
        long nowTimestamp = nowSupplier.getAsLong();
        String policyName = request.getName();
        String enrichIndexName = EnrichPolicy.getIndexName(request.getName(), nowTimestamp);
        Releasable policyLock = tryLockingPolicy(request.getName(), enrichIndexName);
        try {
            Request internalRequest = new Request(request.masterNodeTimeout(), request.getName(), enrichIndexName);
            internalRequest.setWaitForCompletion(request.isWaitForCompletion());
            internalRequest.setParentTask(request.getParentTask());
            client.execute(InternalExecutePolicyAction.INSTANCE, internalRequest, ActionListener.wrap(response -> {
                if (response.getStatus() != null) {
                    logger.debug("Unlocking enrich policy [{}:{}] on complete with no task scheduled", policyName, enrichIndexName);
                    policyLock.close();
                    listener.onResponse(response);
                } else {
                    assert response.getTaskId() != null : "If the execute response does not have a status it must return a task id";
                    awaitTaskCompletionAndThenRelease(response.getTaskId(), () -> {
                        logger.debug("Unlocking enrich policy [{}:{}] on completion of task status", policyName, enrichIndexName);
                        policyLock.close();
                    }, policyName, enrichIndexName);
                    listener.onResponse(response);
                }
            }, e -> {
                logger.debug("Unlocking enrich policy [{}:{}] on failure to execute internal action", policyName, enrichIndexName);
                policyLock.close();
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            // Be sure to unlock if submission failed.
            policyLock.close();
            throw e;
        }
    }

    public void runPolicyLocally(
        ProjectId projectId,
        ExecuteEnrichPolicyTask task,
        String policyName,
        String enrichIndexName,
        ActionListener<ExecuteEnrichPolicyStatus> listener
    ) {
        try {
            EnrichPolicy policy = EnrichStore.getPolicy(policyName, clusterService.state().metadata().getProject(projectId));
            if (policy == null) {
                throw new ResourceNotFoundException("policy [{}] does not exist", policyName);
            }

            task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.SCHEDULED));
            var policyRunner = createPolicyRunner(projectId, policyName, policy, enrichIndexName, task);
            threadPool.executor(ThreadPool.Names.GENERIC)
                .execute(ActionRunnable.wrap(ActionListener.assertOnce(listener), policyRunner::run));
        } catch (Exception e) {
            task.setStatus(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.FAILED));
            throw e;
        }
    }

    private Releasable tryLockingPolicy(String policyName, String enrichIndexName) {
        EnrichPolicyLock policyLock = policyLocks.lockPolicy(policyName, enrichIndexName);
        if (policyExecutionPermits.tryAcquire() == false) {
            // Release policy lock, and throw a different exception
            policyLock.close();
            throw new EsRejectedExecutionException(
                "Policy execution failed. Policy execution for ["
                    + policyName
                    + "] would exceed "
                    + "maximum concurrent policy executions ["
                    + maximumConcurrentPolicyExecutions
                    + "]"
            );
        }
        // Wrap the result so that when releasing it we also release the held execution permit.
        return () -> {
            try (policyLock) {
                policyExecutionPermits.release();
            }
        };
    }

    private void awaitTaskCompletionAndThenRelease(
        TaskId taskId,
        Releasable policyLock,
        final String policyName,
        final String enrichIndexName
    ) {
        GetTaskRequest getTaskRequest = new GetTaskRequest().setTaskId(taskId).setWaitForCompletion(true).setTimeout(TimeValue.MAX_VALUE);
        client.admin().cluster().getTask(getTaskRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetTaskResponse getTaskResponse) {
                policyLock.close();
            }

            @Override
            public void onFailure(Exception exception) {
                if (ExceptionsHelper.unwrap(exception, ResourceNotFoundException.class) != null) {
                    // Could not find task, which means it completed, failed, or the node is gone. Clean up policy lock.
                    logger.debug(
                        "Assuming async policy [{}:{}] execution task [{}] has ended after not being able to retrieve it from remote host",
                        policyName,
                        enrichIndexName,
                        taskId
                    );
                    policyLock.close();
                } else if (ExceptionsHelper.unwrap(exception, ElasticsearchTimeoutException.class) != null) {
                    // Timeout occurred while waiting for completion, launch the wait again
                    logger.debug(
                        "Retrying task wait after encountering timeout during async policy execution result [{}:{}]",
                        policyName,
                        enrichIndexName
                    );
                    awaitTaskCompletionAndThenRelease(taskId, policyLock, policyName, enrichIndexName);
                } else {
                    // We've encountered an unforeseen problem while waiting for the policy to complete. Could be a network error or
                    // something else. Instead of keeping the policy locked forever and potentially jamming the enrich feature during
                    // an unstable cluster event, we should unlock it and accept the possibility of an inconsistent execution.
                    logger.error(
                        "Emergency unlock for enrich policy ["
                            + policyName
                            + ":"
                            + enrichIndexName
                            + "] on failure to determine task status caused by unhandled exception",
                        exception
                    );
                    policyLock.close();
                }
            }
        });
    }

    private EnrichPolicyRunner createPolicyRunner(
        ProjectId projectId,
        String policyName,
        EnrichPolicy policy,
        String enrichIndexName,
        ExecuteEnrichPolicyTask task
    ) {
        return new EnrichPolicyRunner(
            projectId,
            policyName,
            policy,
            task,
            clusterService,
            indicesService,
            client,
            indexNameExpressionResolver,
            enrichIndexName,
            fetchSize,
            maxForceMergeAttempts
        );
    }

}
