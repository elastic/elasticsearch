/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.core.ml.MLMetadataField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DeleteJobIT extends BaseMlIntegTestCase {

    public void testWaitForDelete() throws ExecutionException, InterruptedException {
        final String jobId = "wait-for-delete-job";
        Job.Builder job = createJob(jobId);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).get();

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        CountDownLatch markAsDeletedLatch = new CountDownLatch(1);
        clusterService().submitStateUpdateTask("mark-job-as-deleted", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return markJobAsDeleted(jobId, currentState);
            }

            @Override
            public void onFailure(String source, Exception e) {
                markAsDeletedLatch.countDown();
                exceptionHolder.set(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                markAsDeletedLatch.countDown();
            }
        });

        assertTrue("Timed out waiting for state update", markAsDeletedLatch.await(5, TimeUnit.SECONDS));
        assertNull("mark-job-as-deleted task failed: " + exceptionHolder.get(), exceptionHolder.get());

        // Job is marked as deleting so now a delete request should wait for it.
        AtomicBoolean isDeleted = new AtomicBoolean(false);
        AtomicReference<Exception> deleteFailure = new AtomicReference<>();
        ActionListener<DeleteJobAction.Response> deleteListener = new ActionListener<DeleteJobAction.Response>() {
            @Override
            public void onResponse(DeleteJobAction.Response response) {
                isDeleted.compareAndSet(false, response.isAcknowledged());
            }

            @Override
            public void onFailure(Exception e) {
                deleteFailure.set(e);
            }
        };

        client().execute(DeleteJobAction.INSTANCE, new DeleteJobAction.Request(jobId), deleteListener);
        awaitBusy(isDeleted::get, 1, TimeUnit.SECONDS);
        // still waiting
        assertFalse(isDeleted.get());

        CountDownLatch removeJobLatch = new CountDownLatch(1);
        clusterService().submitStateUpdateTask("remove-job-from-state", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                assertFalse(isDeleted.get());
                return removeJobFromClusterState(jobId, currentState);
            }

            @Override
            public void onFailure(String source, Exception e) {
                removeJobLatch.countDown();
                exceptionHolder.set(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                removeJobLatch.countDown();
            }
        });

        assertTrue("Timed out waiting for remove job from state response", removeJobLatch.await(5, TimeUnit.SECONDS));
        assertNull("remove-job-from-state task failed: " + exceptionHolder.get(), exceptionHolder.get());

        assertNull("Job deletion failed: " + deleteFailure.get(), deleteFailure.get());
        assertTrue("Job was not deleted", isDeleted.get());
    }

    private ClusterState markJobAsDeleted(String jobId, ClusterState currentState) {
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(currentState);
        assertNotNull(mlMetadata);

        MlMetadata.Builder builder = new MlMetadata.Builder(mlMetadata);
        PersistentTasksCustomMetaData tasks = currentState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        builder.markJobAsDeleted(jobId, tasks, true);

        ClusterState.Builder newState = ClusterState.builder(currentState);
        return newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MLMetadataField.TYPE, builder.build()).build())
                .build();
    }

    private ClusterState removeJobFromClusterState(String jobId, ClusterState currentState) {
        MlMetadata.Builder builder = new MlMetadata.Builder(MlMetadata.getMlMetadata(currentState));
        builder.deleteJob(jobId, currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));

        ClusterState.Builder newState = ClusterState.builder(currentState);
        return newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MLMetadataField.TYPE, builder.build()).build())
                .build();
    }
}
