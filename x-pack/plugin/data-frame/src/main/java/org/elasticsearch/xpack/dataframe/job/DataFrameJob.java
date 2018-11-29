/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.job;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.dataframe.DataFrame;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class DataFrameJob implements XPackPlugin.XPackPersistentTaskParams {

    public static final ParseField ID = new ParseField("id");
    public static final String NAME = DataFrame.TASK_NAME;

    // note: this is used to match tasks
    public static final String PERSISTENT_TASK_DESCRIPTION_PREFIX = "data_frame_";

    private DataFrameJobConfig config;

    private static final ParseField CONFIG = new ParseField("config");

    public static final ConstructingObjectParser<DataFrameJob, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new DataFrameJob((DataFrameJobConfig) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DataFrameJobConfig.fromXContent(p, null),
                CONFIG);
    }

    public DataFrameJob(DataFrameJobConfig config) {
        this.config = Objects.requireNonNull(config);
    }

    public DataFrameJob(StreamInput in) throws IOException {
        this.config = new DataFrameJobConfig(in);
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
        config.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONFIG.getPreferredName(), config);
        builder.endObject();
        return builder;
    }

    public DataFrameJobConfig getConfig() {
        return config;
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

        return Objects.equals(this.config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }

    public Map<String, String> getHeaders() {
        return Collections.emptyMap();
    }
}
