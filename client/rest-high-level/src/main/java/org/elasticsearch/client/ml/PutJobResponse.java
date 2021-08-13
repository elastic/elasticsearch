/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response containing the newly created {@link Job}
 */
public class PutJobResponse implements ToXContentObject {

    private Job job;

    public static PutJobResponse fromXContent(XContentParser parser) throws IOException {
        return new PutJobResponse(Job.PARSER.parse(parser, null).build());
    }

    PutJobResponse(Job job) {
        this.job = job;
    }

    public Job getResponse() {
        return job;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        job.toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        PutJobResponse response = (PutJobResponse) object;
        return Objects.equals(job, response.job);
    }

    @Override
    public int hashCode() {
        return Objects.hash(job);
    }
}
