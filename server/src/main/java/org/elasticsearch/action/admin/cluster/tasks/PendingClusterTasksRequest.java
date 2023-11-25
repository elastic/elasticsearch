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
import org.elasticsearch.common.io.stream.StreamOutput;

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
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(PENDING_CLUSTER_TASKS_DETAILS_ADDED)) {
            out.writeBoolean(detailed);
        }
        // else just drop the flag, earlier versions don't support detailed mode anyway. Note that pending tasks are created locally on the
        // elected master, so if we're sending this to an older master node then its pending tasks will have some detail recorded in the
        // source field anyway. There's a risk that we send this to an older master just as it stops being the master, so the recipient
        // forwards the request on to the next master which might be a newer version again, so its pending tasks have less info in the
        // source field. That's tolerable if not ideal: a retry will likely avoid the detour through the older master and get more info.
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public boolean detailed() {
        return detailed;
    }
}
