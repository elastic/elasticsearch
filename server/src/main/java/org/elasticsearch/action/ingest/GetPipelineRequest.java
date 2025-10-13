/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public class GetPipelineRequest extends LocalClusterStateRequest {

    private final String[] ids;
    private final boolean summary;

    public GetPipelineRequest(TimeValue masterNodeTimeout, boolean summary, String... ids) {
        super(masterNodeTimeout);
        if (ids == null) {
            throw new IllegalArgumentException("ids cannot be null");
        }
        this.ids = ids;
        this.summary = summary;
    }

    public GetPipelineRequest(TimeValue masterNodeTimeout, String... ids) {
        this(masterNodeTimeout, false, ids);
    }

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    public GetPipelineRequest(StreamInput in) throws IOException {
        super(in);
        ids = in.readStringArray();
        summary = in.readBoolean();
    }

    public String[] getIds() {
        return ids;
    }

    public boolean isSummary() {
        return summary;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }
}
