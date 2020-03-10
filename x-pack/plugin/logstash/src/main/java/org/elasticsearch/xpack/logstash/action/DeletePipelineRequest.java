/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class DeletePipelineRequest extends ActionRequest {

    private final String id;

    public DeletePipelineRequest(String id) {
        this.id = id;
    }

    public DeletePipelineRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
    }

    public String id() {
        return id;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
