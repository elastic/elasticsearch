/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public class IndicesSegmentsRequest extends BroadcastRequest<IndicesSegmentsRequest> {

    private boolean includeVectorFormatsInfo;

    public IndicesSegmentsRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public IndicesSegmentsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            in.readBoolean();   // old 'verbose' option, since removed
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            this.includeVectorFormatsInfo = in.readBoolean();
        }
    }

    public IndicesSegmentsRequest(String... indices) {
        super(indices);
        this.includeVectorFormatsInfo = false;
    }

    public IndicesSegmentsRequest withVectorFormatsInfo(boolean includeVectorFormatsInfo) {
        this.includeVectorFormatsInfo = includeVectorFormatsInfo;
        return this;
    }

    public boolean isIncludeVectorFormatsInfo() {
        return includeVectorFormatsInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeBoolean(false);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeBoolean(includeVectorFormatsInfo);
        }
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }
}
