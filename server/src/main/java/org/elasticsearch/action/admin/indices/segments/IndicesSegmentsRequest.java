/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.TransportVersion;
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

    private static final TransportVersion SEGMENT_AUTO_CALIBRATION = TransportVersion.fromName("segment_auto_calibration");

    private boolean includeVectorFormatsInfo;
    private boolean includeAutoCalibration;

    public IndicesSegmentsRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public IndicesSegmentsRequest(StreamInput in) throws IOException {
        super(in);
        this.includeVectorFormatsInfo = in.readBoolean();
        if (in.getTransportVersion().supports(SEGMENT_AUTO_CALIBRATION)) {
            this.includeAutoCalibration = in.readBoolean();
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

    public IndicesSegmentsRequest withAutoCalibration(boolean includeAutoCalibration) {
        this.includeAutoCalibration = includeAutoCalibration;
        return this;
    }

    public boolean isIncludeAutoCalibration() {
        return includeAutoCalibration;
    }

    public boolean isIncludeVectorFormatsInfo() {
        return includeVectorFormatsInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(includeVectorFormatsInfo);
        if (out.getTransportVersion().supports(SEGMENT_AUTO_CALIBRATION)) {
            out.writeBoolean(includeAutoCalibration);
        }
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }
}
