/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class RepositoryStatsRequest extends BaseNodesRequest<RepositoryStatsRequest> {

    private final String repository;

    public RepositoryStatsRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
    }

    public RepositoryStatsRequest(String repository, String... nodesIds) {
        super(nodesIds);
        this.repository = repository;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
    }

    public String getRepository() {
        return repository;
    }

}
