/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to preview a MachineLearning Datafeed
 */
public class PreviewDatafeedRequest implements Validatable, ToXContentObject {

    private static final ParseField DATAFEED_CONFIG = new ParseField("datafeed_config");
    private static final ParseField JOB_CONFIG = new ParseField("job_config");

    public static final ConstructingObjectParser<PreviewDatafeedRequest, Void> PARSER = new ConstructingObjectParser<>(
        "preview_datafeed_request",
        a -> new PreviewDatafeedRequest((String) a[0], (DatafeedConfig.Builder) a[1], (Job.Builder) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DatafeedConfig.ID);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), DatafeedConfig.PARSER, DATAFEED_CONFIG);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Job.PARSER, JOB_CONFIG);
    }

    public static PreviewDatafeedRequest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String datafeedId;
    private final DatafeedConfig datafeedConfig;
    private final Job jobConfig;

    private PreviewDatafeedRequest(@Nullable String datafeedId,
                                   @Nullable DatafeedConfig.Builder datafeedConfig,
                                   @Nullable Job.Builder jobConfig) {
        this.datafeedId = datafeedId;
        this.datafeedConfig = datafeedConfig == null ? null : datafeedConfig.build();
        this.jobConfig = jobConfig == null ? null : jobConfig.build();
    }

    /**
     * Create a new request with the desired datafeedId
     *
     * @param datafeedId unique datafeedId, must not be null
     */
    public PreviewDatafeedRequest(String datafeedId) {
        this.datafeedId = Objects.requireNonNull(datafeedId, "[datafeed_id] must not be null");
        this.datafeedConfig = null;
        this.jobConfig = null;
    }

    /**
     * Create a new request to preview the provided datafeed config and optional job config
     * @param datafeedConfig The datafeed to preview
     * @param jobConfig The associated job config (required if the datafeed does not refer to an existing job)
     */
    public PreviewDatafeedRequest(DatafeedConfig datafeedConfig, Job jobConfig) {
        this.datafeedId = null;
        this.datafeedConfig = datafeedConfig;
        this.jobConfig = jobConfig;
    }

    public String getDatafeedId() {
        return datafeedId;
    }

    public DatafeedConfig getDatafeedConfig() {
        return datafeedConfig;
    }

    public Job getJobConfig() {
        return jobConfig;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (datafeedId != null) {
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
        }
        if (datafeedConfig != null) {
            builder.field(DATAFEED_CONFIG.getPreferredName(), datafeedConfig);
        }
        if (jobConfig != null) {
            builder.field(JOB_CONFIG.getPreferredName(), jobConfig);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeedId, datafeedConfig, jobConfig);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        PreviewDatafeedRequest that = (PreviewDatafeedRequest) other;
        return Objects.equals(datafeedId, that.datafeedId)
            && Objects.equals(datafeedConfig, that.datafeedConfig)
            && Objects.equals(jobConfig, that.jobConfig);
    }
}
