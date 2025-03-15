/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class EsqlGetQueryRequest extends ActionRequest {
    private final String id;

    public EsqlGetQueryRequest(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public EsqlGetQueryRequest(StreamInput streamInput) throws IOException {
        super(streamInput);
        id = streamInput.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
