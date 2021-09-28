/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the configuration details of a rollup job, such as the groupings, metrics, what
 * index to rollup and where to roll them to.
 */
public class RollupJobConfig implements NamedWriteable, ToXContentObject {

    private static final String NAME = "xpack/rollup/jobconfig";
    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(20);
    private static final String ID = "id";
    private static final String TIMEOUT = "timeout";
    private static final String CRON = "cron";
    private static final String PAGE_SIZE = "page_size";
    private static final String INDEX_PATTERN = "index_pattern";
    private static final String ROLLUP_INDEX = "rollup_index";

    private final String id;
    private final String indexPattern;
    private final String rollupIndex;
    private final GroupConfig groupConfig;
    private final List<MetricConfig> metricsConfig;
    private final TimeValue timeout;
    private final String cron;
    private final int pageSize;

    private static final ConstructingObjectParser<RollupJobConfig, String> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, false, (args, optionalId) -> {
            String id = args[0] != null ? (String) args[0] : optionalId;
            String indexPattern = (String) args[1];
            String rollupIndex = (String) args[2];
            GroupConfig groupConfig = (GroupConfig) args[3];
            @SuppressWarnings("unchecked")
            List<MetricConfig> metricsConfig = (List<MetricConfig>) args[4];
            TimeValue timeout = (TimeValue) args[5];
            String cron = (String) args[6];
            int pageSize = (int) args[7];
            return new RollupJobConfig(id, indexPattern, rollupIndex, cron, pageSize, groupConfig, metricsConfig, timeout);
        });
        PARSER.declareString(optionalConstructorArg(), new ParseField(ID));
        PARSER.declareString(constructorArg(), new ParseField(INDEX_PATTERN));
        PARSER.declareString(constructorArg(), new ParseField(ROLLUP_INDEX));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> GroupConfig.fromXContent(p), new ParseField(GroupConfig.NAME));
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> MetricConfig.fromXContent(p), new ParseField(MetricConfig.NAME));
        PARSER.declareField(optionalConstructorArg(), (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), TIMEOUT),
            new ParseField(TIMEOUT), ObjectParser.ValueType.STRING_OR_NULL);
        PARSER.declareString(constructorArg(), new ParseField(CRON));
        PARSER.declareInt(constructorArg(), new ParseField(PAGE_SIZE));
    }

    public RollupJobConfig(final String id,
                           final String indexPattern,
                           final String rollupIndex,
                           final String cron,
                           final int pageSize,
                           final GroupConfig groupConfig,
                           final List<MetricConfig> metricsConfig,
                           final @Nullable TimeValue timeout) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Id must be a non-null, non-empty string");
        }
        if (indexPattern == null || indexPattern.isEmpty()) {
            throw new IllegalArgumentException("Index pattern must be a non-null, non-empty string");
        }
        if (Regex.isMatchAllPattern(indexPattern)) {
            throw new IllegalArgumentException("Index pattern must not match all indices (as it would match it's own rollup index");
        }
        if (Regex.isSimpleMatchPattern(indexPattern)) {
            if (Regex.simpleMatch(indexPattern, rollupIndex)) {
                throw new IllegalArgumentException("Index pattern would match rollup index name which is not allowed");
            }
        }
        if (indexPattern.equals(rollupIndex)) {
            throw new IllegalArgumentException("Rollup index may not be the same as the index pattern");
        }
        if (rollupIndex == null || rollupIndex.isEmpty()) {
            throw new IllegalArgumentException("Rollup index must be a non-null, non-empty string");
        }
        if (cron == null || cron.isEmpty()) {
            throw new IllegalArgumentException("Cron schedule must be a non-null, non-empty string");
        }
        if (pageSize <= 0) {
            throw new IllegalArgumentException("Page size is mandatory and  must be a positive long");
        }
        if (groupConfig == null && (metricsConfig == null || metricsConfig.isEmpty())) {
            throw new IllegalArgumentException("At least one grouping or metric must be configured");
        }

        this.id = id;
        this.indexPattern = indexPattern;
        this.rollupIndex = rollupIndex;
        this.groupConfig = groupConfig;
        this.metricsConfig = metricsConfig != null ? metricsConfig : Collections.emptyList();
        this.timeout = timeout != null ? timeout : DEFAULT_TIMEOUT;
        this.cron = cron;
        this.pageSize = pageSize;
    }

    public RollupJobConfig(final StreamInput in) throws IOException {
        id = in.readString();
        indexPattern = in.readString();
        rollupIndex = in.readString();
        cron = in.readString();
        groupConfig = in.readOptionalWriteable(GroupConfig::new);
        metricsConfig = in.readList(MetricConfig::new);
        timeout = in.readTimeValue();
        pageSize = in.readInt();
    }

    public String getId() {
        return id;
    }

    public GroupConfig getGroupConfig() {
        return groupConfig;
    }

    public List<MetricConfig> getMetricsConfig() {
        return metricsConfig;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public String getIndexPattern() {
        return indexPattern;
    }

    public String getRollupIndex() {
        return rollupIndex;
    }

    public String getCron() {
        return cron;
    }

    public int getPageSize() {
        return pageSize;
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
            builder.field(ID, id);
            builder.field(INDEX_PATTERN, indexPattern);
            builder.field(ROLLUP_INDEX, rollupIndex);
            builder.field(CRON, cron);
            if (groupConfig != null) {
                builder.field(GroupConfig.NAME, groupConfig);
            }
            if (metricsConfig != null) {
                builder.startArray(MetricConfig.NAME);
                for (MetricConfig metric : metricsConfig) {
                    metric.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (timeout != null) {
                builder.field(TIMEOUT, timeout.getStringRep());
            }
            builder.field(PAGE_SIZE, pageSize);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(indexPattern);
        out.writeString(rollupIndex);
        out.writeString(cron);
        out.writeOptionalWriteable(groupConfig);
        out.writeList(metricsConfig);
        out.writeTimeValue(timeout);
        out.writeInt(pageSize);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final RollupJobConfig that = (RollupJobConfig) other;
        return Objects.equals(this.id, that.id)
                && Objects.equals(this.indexPattern, that.indexPattern)
                && Objects.equals(this.rollupIndex, that.rollupIndex)
                && Objects.equals(this.cron, that.cron)
                && Objects.equals(this.groupConfig, that.groupConfig)
                && Objects.equals(this.metricsConfig, that.metricsConfig)
                && Objects.equals(this.timeout, that.timeout)
                && Objects.equals(this.pageSize, that.pageSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, indexPattern, rollupIndex, cron, groupConfig, metricsConfig, timeout, pageSize);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    /**
     * Same as toString() but more explicitly named so the caller knows this is turned into JSON
     */
    public String toJSONString() {
        return toString();
    }

    public static RollupJobConfig fromXContent(final XContentParser parser, @Nullable final String optionalJobId) throws IOException {
        return PARSER.parse(parser, optionalJobId);
    }
}
