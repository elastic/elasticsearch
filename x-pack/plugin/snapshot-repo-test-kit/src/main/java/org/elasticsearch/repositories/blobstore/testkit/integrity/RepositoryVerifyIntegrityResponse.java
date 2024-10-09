/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class RepositoryVerifyIntegrityResponse extends ActionResponse {
    private final RepositoryVerifyIntegrityTask.Status finalTaskStatus;
    private final long finalRepositoryGeneration;

    RepositoryVerifyIntegrityResponse(RepositoryVerifyIntegrityTask.Status finalTaskStatus, long finalRepositoryGeneration) {
        this.finalTaskStatus = finalTaskStatus;
        this.finalRepositoryGeneration = finalRepositoryGeneration;
    }

    RepositoryVerifyIntegrityResponse(StreamInput in) throws IOException {
        finalRepositoryGeneration = in.readLong();
        finalTaskStatus = new RepositoryVerifyIntegrityTask.Status(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(finalRepositoryGeneration);
        finalTaskStatus.writeTo(out);
    }

    public long finalRepositoryGeneration() {
        return finalRepositoryGeneration;
    }

    public RepositoryVerifyIntegrityTask.Status finalTaskStatus() {
        return finalTaskStatus;
    }

    public long originalRepositoryGeneration() {
        return finalTaskStatus.repositoryGeneration();
    }
}
