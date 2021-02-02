/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get node tasks
 */
public class ListTasksRequest extends BaseTasksRequest<ListTasksRequest> {

    private boolean detailed = false;
    private boolean waitForCompletion = false;

    public ListTasksRequest() {
    }

    public ListTasksRequest(StreamInput in) throws IOException {
        super(in);
        detailed = in.readBoolean();
        waitForCompletion = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(detailed);
        out.writeBoolean(waitForCompletion);
    }

    /**
     * Should the detailed task information be returned.
     */
    public boolean getDetailed() {
        return this.detailed;
    }

    /**
     * Should the detailed task information be returned.
     */
    public ListTasksRequest setDetailed(boolean detailed) {
        this.detailed = detailed;
        return this;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public ListTasksRequest setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

}
