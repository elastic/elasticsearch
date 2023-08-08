/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.tasks.TaskId;

/**
 * Triggers a force merge operation on the downsample target index
 */
public class ForceMergeActionListener implements ActionListener<AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(ForceMergeActionListener.class);
    final ActionListener<AcknowledgedResponse> failureDelegate;
    final Client client;
    private final TaskId parentTask;
    private final String rollupIndexName;

    public ForceMergeActionListener(
        final TaskId parentTask,
        final String rollupIndexName,
        final Client client,
        final ActionListener<AcknowledgedResponse> onFailure
    ) {
        this.parentTask = parentTask;
        this.rollupIndexName = rollupIndexName;
        this.client = client;
        this.failureDelegate = onFailure;
    }

    @Override
    public void onResponse(final AcknowledgedResponse response) {
        if (response.isAcknowledged()) {
            /*
             * At this point rollup has been created
             * successfully even force merge fails.
             * So, we should not fail the rollup operation
             */
            ForceMergeRequest request = new ForceMergeRequest(rollupIndexName);
            request.maxNumSegments(1);
            request.setParentTask(parentTask);
            this.client.admin()
                .indices()
                .forceMerge(request, ActionListener.wrap(mergeIndexResp -> failureDelegate.onResponse(AcknowledgedResponse.TRUE), t -> {
                    /*
                     * At this point rollup has been created
                     * successfully even force merge fails.
                     * So, we should not fail the rollup operation
                     */
                    logger.error("Failed to force-merge " + "rollup index [" + rollupIndexName + "]", t);
                    failureDelegate.onResponse(AcknowledgedResponse.TRUE);
                }));
        }
    }

    @Override
    public void onFailure(Exception e) {
        this.failureDelegate.onFailure(e);
    }

}
