/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.croneval;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class CronEvaluationResponse extends ActionResponse {

    private List<String> timestamps;

    CronEvaluationResponse() {
        this(Collections.emptyList());
    }

    public CronEvaluationResponse(List<String> timestamps) {
        this.timestamps = timestamps;
    }

    public List<String> getTimestamps() {
        return timestamps;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        timestamps = in.readList(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringList(timestamps);
    }
}
