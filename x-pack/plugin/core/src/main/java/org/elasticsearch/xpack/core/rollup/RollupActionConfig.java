/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the configuration details of a {@link RollupAction} job, such as the groupings, metrics, what
 * index to rollup and where to roll them to.
 */
public class RollupActionConfig implements NamedWriteable, ToXContentObject {

    private static final String NAME = "xpack/rollup/action/config";

    private final RollupActionGroupConfig groupConfig;
    private final List<MetricConfig> metricsConfig;

    private static final ConstructingObjectParser<RollupActionConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, false, (args) -> {
            RollupActionGroupConfig groupConfig = (RollupActionGroupConfig) args[0];
            @SuppressWarnings("unchecked")
            List<MetricConfig> metricsConfig = (List<MetricConfig>) args[1];
            return new RollupActionConfig(groupConfig, metricsConfig);
        });
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> RollupActionGroupConfig.fromXContent(p),
            new ParseField(RollupActionGroupConfig.NAME));
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> MetricConfig.fromXContent(p), new ParseField(MetricConfig.NAME));
    }

    public RollupActionConfig(final RollupActionGroupConfig groupConfig, final List<MetricConfig> metricsConfig) {
        if (groupConfig == null && (metricsConfig == null || metricsConfig.isEmpty())) {
            throw new IllegalArgumentException("At least one grouping or metric must be configured");
        } else if (metricsConfig == null || metricsConfig.isEmpty()) {
            throw new IllegalArgumentException("At least one metric must be configured");
        }
        this.groupConfig = groupConfig;
        this.metricsConfig = metricsConfig != null ? metricsConfig : Collections.emptyList();
    }

    public RollupActionConfig(final StreamInput in) throws IOException {
        groupConfig = in.readOptionalWriteable(RollupActionGroupConfig::new);
        metricsConfig = in.readList(MetricConfig::new);
    }

    public RollupActionGroupConfig getGroupConfig() {
        return groupConfig;
    }

    public List<MetricConfig> getMetricsConfig() {
        return metricsConfig;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public Set<String> getAllFields() {
        final Set<String> fields = new HashSet<>();
        if (groupConfig != null) {
            fields.addAll(groupConfig.getAllFields());
        }
        if (metricsConfig != null) {
            for (MetricConfig metric : metricsConfig) {
                fields.add(metric.getField());
            }
        }
        return Collections.unmodifiableSet(fields);
    }

    public void validateMappings(final Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                 final ActionRequestValidationException validationException) {
        groupConfig.validateMappings(fieldCapsResponse, validationException);
        for (MetricConfig m : metricsConfig) {
            m.validateMappings(fieldCapsResponse, validationException);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            if (groupConfig != null) {
                builder.field(RollupActionGroupConfig.NAME, groupConfig);
            }
            if (metricsConfig != null) {
                builder.startArray(MetricConfig.NAME);
                for (MetricConfig metric : metricsConfig) {
                    metric.toXContent(builder, params);
                }
                builder.endArray();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalWriteable(groupConfig);
        out.writeList(metricsConfig);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final RollupActionConfig that = (RollupActionConfig) other;
        return Objects.equals(this.groupConfig, that.groupConfig)
            && Objects.equals(this.metricsConfig, that.metricsConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupConfig, metricsConfig);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static RollupActionConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
