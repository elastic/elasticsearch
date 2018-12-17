/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.job;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class DataFrameJob extends AbstractDiffable<DataFrameJob> implements XPackPlugin.XPackPersistentTaskParams {

    public static final String NAME = DataFrameField.TASK_NAME;

    private String jobId;

    public static final ConstructingObjectParser<DataFrameJob, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new DataFrameJob((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameField.ID);
    }

    public DataFrameJob(String jobId) {
        this.jobId = jobId;
    }

    public DataFrameJob(StreamInput in) throws IOException {
        this.jobId  = in.readString();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        // TODO: to be changed once target version has been defined
        return Version.CURRENT;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), jobId);
        builder.endObject();
        return builder;
    }

    public String getId() {
        return jobId;
    }

    public static DataFrameJob fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameJob that = (DataFrameJob) other;

        return Objects.equals(this.jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId);
    }

    public Map<String, String> getHeaders() {
        return Collections.emptyMap();
    }
}
