/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.prelert.action.UpdateJobStatusAction;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobService;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

public class JobLifeCycleService extends AbstractComponent implements ClusterStateListener {

    volatile Set<String> localAllocatedJobs = Collections.emptySet();
    private final Client client;
    private final ScheduledJobService scheduledJobService;
    private DataProcessor dataProcessor;
    private final Executor executor;

    public JobLifeCycleService(Settings settings, Client client, ClusterService clusterService, ScheduledJobService scheduledJobService,
                               DataProcessor dataProcessor, Executor executor) {
        super(settings);
        clusterService.add(this);
        this.client = Objects.requireNonNull(client);
        this.scheduledJobService = Objects.requireNonNull(scheduledJobService);
        this.dataProcessor = Objects.requireNonNull(dataProcessor);
        this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        PrelertMetadata prelertMetadata = event.state().getMetaData().custom(PrelertMetadata.TYPE);
        if (prelertMetadata == null) {
            logger.debug("Prelert metadata not installed");
            return;
        }

        // Single volatile read:
        Set<String> localAllocatedJobs = this.localAllocatedJobs;

        DiscoveryNode localNode = event.state().nodes().getLocalNode();
        for (Allocation allocation : prelertMetadata.getAllocations().values()) {
            if (localNode.getId().equals(allocation.getNodeId())) {
                handleLocallyAllocatedJob(prelertMetadata, allocation);
            }
        }

        for (String localAllocatedJob : localAllocatedJobs) {
            Allocation allocation = prelertMetadata.getAllocations().get(localAllocatedJob);
            if (allocation != null) {
                if (localNode.getId().equals(allocation.getNodeId()) == false) {
                    stopJob(localAllocatedJob);
                }
            } else {
                stopJob(localAllocatedJob);
            }
        }
    }

    private void handleLocallyAllocatedJob(PrelertMetadata prelertMetadata, Allocation allocation) {
        Job job = prelertMetadata.getJobs().get(allocation.getJobId());
        if (localAllocatedJobs.contains(allocation.getJobId()) == false) {
            startJob(job);
        }

        handleJobStatusChange(job, allocation.getStatus());
        handleSchedulerStatusChange(job, allocation);
    }

    private void handleJobStatusChange(Job job, JobStatus status) {
        switch (status) {
            case PAUSING:
                executor.execute(() -> pauseJob(job));
                break;
            case RUNNING:
                break;
            case CLOSING:
                break;
            case CLOSED:
                break;
            case PAUSED:
                break;
            default:
                throw new IllegalStateException("Unknown job status [" + status + "]");
        }
    }

    private void handleSchedulerStatusChange(Job job, Allocation allocation) {
        SchedulerState schedulerState = allocation.getSchedulerState();
        if (schedulerState != null) {
            switch (schedulerState.getStatus()) {
                case STARTING:
                    scheduledJobService.start(job, allocation);
                    break;
                case STARTED:
                    break;
                case STOPPING:
                    scheduledJobService.stop(allocation);
                    break;
                case STOPPED:
                    break;
                default:
                    throw new IllegalStateException("Unhandled scheduler state [" + schedulerState.getStatus() + "]");
            }
        }
    }

    void startJob(Job job) {
        logger.info("Starting job [" + job.getId() + "]");
        // noop now, but should delegate to a task / ProcessManager that actually starts the job

        // update which jobs are now allocated locally
        Set<String> newSet = new HashSet<>(localAllocatedJobs);
        newSet.add(job.getId());
        localAllocatedJobs = newSet;
    }

    void stopJob(String jobId) {
        logger.info("Stopping job [" + jobId + "]");
        // noop now, but should delegate to a task / ProcessManager that actually stops the job

        // update which jobs are now allocated locally
        Set<String> newSet = new HashSet<>(localAllocatedJobs);
        newSet.remove(jobId);
        localAllocatedJobs = newSet;
    }

    private void pauseJob(Job job) {
        try {
            // NORELEASE Ensure this also removes the job auto-close timeout task
            dataProcessor.closeJob(job.getId());
        } catch (ElasticsearchException e) {
            logger.error("Failed to close job [" + job.getId() + "] while pausing", e);
            updateJobStatus(job.getId(), JobStatus.FAILED);
            return;
        }
        updateJobStatus(job.getId(), JobStatus.PAUSED);
    }

    private void updateJobStatus(String jobId, JobStatus status) {
        UpdateJobStatusAction.Request request = new UpdateJobStatusAction.Request(jobId, status);
        client.execute(UpdateJobStatusAction.INSTANCE, request, new ActionListener<UpdateJobStatusAction.Response>() {
            @Override
            public void onResponse(UpdateJobStatusAction.Response response) {
                logger.info("Successfully set job status to [{}] for job [{}]", status, jobId);
                // NORELEASE Audit job paused
                // audit(jobId).info(Messages.getMessage(Messages.JOB_AUDIT_PAUSED));
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Could not set job status to [" + status + "] for job [" + jobId +"]", e);
            }
        });
    }
}
