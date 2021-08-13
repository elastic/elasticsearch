/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to create a new Machine Learning Job given a {@link Job} configuration
 */
public class PutJobRequest implements Validatable, ToXContentObject {

    private final Job job;

    /**
     * Construct a new PutJobRequest
     *
     * @param job a {@link Job} configuration to create
     */
    public PutJobRequest(Job job) {
        this.job = job;
    }

    public Job getJob() {
        return job;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return job.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PutJobRequest request = (PutJobRequest) object;
        return Objects.equals(job, request.job);
    }

    @Override
    public int hashCode() {
        return Objects.hash(job);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

}
