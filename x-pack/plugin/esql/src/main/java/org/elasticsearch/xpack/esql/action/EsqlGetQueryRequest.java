/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

public class EsqlGetQueryRequest extends LegacyActionRequest {
    private final TaskId id;

    public EsqlGetQueryRequest(TaskId id) {
        this.id = id;
    }

    public TaskId id() {
        return id;
    }

    public EsqlGetQueryRequest(StreamInput streamInput) throws IOException {
        super(streamInput);
        id = TaskId.readFromStream(streamInput);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeWriteable(id);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
