/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class FeatureIndexBuilderJob implements XPackPlugin.XPackPersistentTaskParams {

    public static final String NAME = "xpack/feature_index_builder/job";

    private FeatureIndexBuilderJobConfig config;

    private static final ParseField CONFIG = new ParseField("config");

    public static final ConstructingObjectParser<FeatureIndexBuilderJob, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new FeatureIndexBuilderJob((FeatureIndexBuilderJobConfig) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> FeatureIndexBuilderJobConfig.fromXContent(p, null),
                CONFIG);
    }

    public FeatureIndexBuilderJob(FeatureIndexBuilderJobConfig config) {
        this.config = Objects.requireNonNull(config);
    }

    public FeatureIndexBuilderJob(StreamInput in) throws IOException {
        this.config = new FeatureIndexBuilderJobConfig(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_0_0_alpha1;
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

    public FeatureIndexBuilderJobConfig getConfig() {
        return config;
    }

    public static FeatureIndexBuilderJob fromXContent(XContentParser parser) throws IOException {
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

        FeatureIndexBuilderJob that = (FeatureIndexBuilderJob) other;

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
