/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Updates a {@link org.elasticsearch.client.ml.job.config.Job} with the passed {@link JobUpdate}
 * settings
 */
public class UpdateJobRequest implements Validatable, ToXContentObject {

    private final JobUpdate update;

    public UpdateJobRequest(JobUpdate update) {
        this.update = update;
    }

    public JobUpdate getJobUpdate() {
        return update;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return update.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UpdateJobRequest that = (UpdateJobRequest) o;
        return Objects.equals(update, that.update);
    }

    @Override
    public int hashCode() {
        return Objects.hash(update);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

}
