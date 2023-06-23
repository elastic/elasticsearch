/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.assertNoAuthorizationHeader;

/**
 * This class is the main wrapper object that is serialized into the PersistentTask's cluster state.
 * It holds the config (RollupJobConfig) and a map of authentication headers.  Only RollupJobConfig
 * is ever serialized to the user, so the headers should never leak
 */
public class RollupJob implements SimpleDiffable<RollupJob>, PersistentTaskParams {

    public static final String NAME = "xpack/rollup/job";

    private final Map<String, String> headers;
    private final RollupJobConfig config;

    private static final ParseField CONFIG = new ParseField("config");
    private static final ParseField HEADERS = new ParseField("headers");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RollupJob, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new RollupJob((RollupJobConfig) a[0], (Map<String, String>) a[1])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> RollupJobConfig.fromXContent(p, null), CONFIG);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
    }

    public RollupJob(RollupJobConfig config, Map<String, String> headers) {
        this.config = Objects.requireNonNull(config);
        this.headers = headers == null ? Collections.emptyMap() : headers;
    }

    public RollupJob(StreamInput in) throws IOException {
        this.config = new RollupJobConfig(in);
        headers = in.readMap(StreamInput::readString);
    }

    public RollupJobConfig getConfig() {
        return config;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONFIG.getPreferredName(), config);
        assertNoAuthorizationHeader(headers);
        builder.field(HEADERS.getPreferredName(), headers);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        config.writeTo(out);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    static Diff<RollupJob> readJobDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(RollupJob::new, in);
    }

    public static RollupJob fromXContent(XContentParser parser) throws IOException {
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

        RollupJob that = (RollupJob) other;

        return Objects.equals(this.config, that.config) && Objects.equals(this.headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, headers);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.MINIMUM_COMPATIBLE;
    }
}
