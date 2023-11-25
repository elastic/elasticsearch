/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

import static org.elasticsearch.TransportVersions.PENDING_CLUSTER_TASKS_DETAILS_ADDED;

public class PendingClusterTasksRequest extends MasterNodeReadRequest<PendingClusterTasksRequest> {

    private final boolean detailed;

    public PendingClusterTasksRequest(boolean detailed) {
        this.detailed = detailed;
    }

    public PendingClusterTasksRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(PENDING_CLUSTER_TASKS_DETAILS_ADDED)) {
            detailed = in.readBoolean();
        } else {
            // earlier versions don't support detailed mode
            detailed = false;
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public boolean detailed() {
        return detailed;
    }
}
