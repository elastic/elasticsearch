/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

@Deprecated
public class RepositoryStatsNodeRequest extends BaseNodeRequest {

    private final String repository;

    public RepositoryStatsNodeRequest(String repository) {
        this.repository = repository;
    }

    public RepositoryStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.repository = in.readString();
    }

    public String getRepository() {
        return repository;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
    }

}
