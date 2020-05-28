/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class RepositoryStatsNodeRequest extends TransportRequest {

    private final String repository;

    public RepositoryStatsNodeRequest(String repository) {
        this.repository = repository;
    }

    public RepositoryStatsNodeRequest(StreamInput in) throws IOException {
        this.repository = in.readString();
    }

    public String getRepository() {
        return repository;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
    }

}
