/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class EsqlProjectRoutingResponse extends ActionResponse {

    private final List<String> projects;

    public EsqlProjectRoutingResponse(List<String> projects) {
        this.projects = projects;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericList(projects, StreamOutput::writeString);
    }

    public List<String> getProjects() {
        return projects;
    }
}
