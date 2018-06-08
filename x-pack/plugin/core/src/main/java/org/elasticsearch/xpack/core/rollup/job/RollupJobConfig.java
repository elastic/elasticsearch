/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.scheduler.Cron;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class holds the configuration details of a rollup job, such as the groupings, metrics, what
 * index to rollup and where to roll them to.
 */
public class RollupJobConfig implements NamedWriteable, ToXContentObject {
    private static final String NAME = "xpack/rollup/jobconfig";

    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final ParseField CURRENT = new ParseField("current");
    public static final ParseField CRON = new ParseField("cron");
    public static final ParseField PAGE_SIZE = new ParseField("page_size");

    private static final ParseField INDEX_PATTERN = new ParseField("index_pattern");
    private static final ParseField ROLLUP_INDEX = new ParseField("rollup_index");
    private static final ParseField GROUPS = new ParseField("groups");
    private static final ParseField METRICS = new ParseField("metrics");

    private String id;
    private String indexPattern;
    private String rollupIndex;
    private GroupConfig groupConfig;
    private List<MetricConfig> metricsConfig = Collections.emptyList();
    private TimeValue timeout = TimeValue.timeValueSeconds(20);
    private String cron;
    private int pageSize;

    public static final ObjectParser<RollupJobConfig.Builder, Void> PARSER = new ObjectParser<>(NAME, false, RollupJobConfig.Builder::new);

    static {
        PARSER.declareString(RollupJobConfig.Builder::setId, RollupField.ID);
        PARSER.declareObject(RollupJobConfig.Builder::setGroupConfig, (p, c) -> GroupConfig.PARSER.apply(p,c).build(), GROUPS);
        PARSER.declareObjectArray(RollupJobConfig.Builder::setMetricsConfig, MetricConfig.PARSER, METRICS);
        PARSER.declareString((params, val) ->
                params.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        PARSER.declareString(RollupJobConfig.Builder::setIndexPattern, INDEX_PATTERN);
        PARSER.declareString(RollupJobConfig.Builder::setRollupIndex, ROLLUP_INDEX);
        PARSER.declareString(RollupJobConfig.Builder::setCron, CRON);
        PARSER.declareInt(RollupJobConfig.Builder::setPageSize, PAGE_SIZE);
    }

    RollupJobConfig(String id, String indexPattern, String rollupIndex, String cron, int pageSize, GroupConfig groupConfig,
                    List<MetricConfig> metricsConfig, TimeValue timeout) {
        this.id = id;
        this.indexPattern = indexPattern;
        this.rollupIndex = rollupIndex;
        this.groupConfig = groupConfig;
        this.metricsConfig = metricsConfig;
        this.timeout = timeout;
        this.cron = cron;
        this.pageSize = pageSize;
    }

    public RollupJobConfig(StreamInput in) throws IOException {
        id = in.readString();
        indexPattern = in.readString();
        rollupIndex = in.readString();
        cron = in.readString();
        groupConfig = in.readOptionalWriteable(GroupConfig::new);
        metricsConfig = in.readList(MetricConfig::new);
        timeout = in.readTimeValue();
        pageSize = in.readInt();
    }

    public RollupJobConfig() {}

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
        Set<String> fields = new HashSet<>(groupConfig.getAllFields());
        fields.addAll(metricsConfig.stream().map(MetricConfig::getField).collect(Collectors.toSet()));
        return fields;
    }

    public void validateMappings(Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                                             ActionRequestValidationException validationException) {
        groupConfig.validateMappings(fieldCapsResponse, validationException);
        for (MetricConfig m : metricsConfig) {
            m.validateMappings(fieldCapsResponse, validationException);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(RollupField.ID.getPreferredName(), id);
        builder.field(INDEX_PATTERN.getPreferredName(), indexPattern);
        builder.field(ROLLUP_INDEX.getPreferredName(), rollupIndex);
        builder.field(CRON.getPreferredName(), cron);
        if (groupConfig != null) {
            builder.field(GROUPS.getPreferredName(), groupConfig);
        }
        if (metricsConfig != null) {
            builder.startArray(METRICS.getPreferredName());
            for (MetricConfig config : metricsConfig) {
                builder.startObject();
                config.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        if (timeout != null) {
            builder.field(TIMEOUT.getPreferredName(), timeout);
        }
        builder.field(PAGE_SIZE.getPreferredName(), pageSize);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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

        RollupJobConfig that = (RollupJobConfig) other;

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
        return Objects.hash(id, indexPattern, rollupIndex, cron, groupConfig,
                metricsConfig, timeout, pageSize);
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

    public static class Builder implements Writeable, ToXContentObject {
        private String id;
        private String indexPattern;
        private String rollupIndex;
        private GroupConfig groupConfig;
        private List<MetricConfig> metricsConfig = Collections.emptyList();
        private TimeValue timeout = TimeValue.timeValueSeconds(20);
        private String cron;
        private int pageSize = 0;

        public Builder(RollupJobConfig job) {
            this.id = job.getId();
            this.indexPattern = job.getIndexPattern();
            this.rollupIndex = job.getRollupIndex();
            this.groupConfig = job.getGroupConfig();
            this.metricsConfig = job.getMetricsConfig();
            this.timeout = job.getTimeout();
            this.cron = job.getCron();
            this.pageSize = job.getPageSize();
        }

        public static RollupJobConfig.Builder fromXContent(String id, XContentParser parser) {
            RollupJobConfig.Builder config = RollupJobConfig.PARSER.apply(parser, null);
            if (id != null) {
                config.setId(id);
            }
            return config;
        }

        public Builder() {}

        public String getId() {
            return id;
        }

        public RollupJobConfig.Builder setId(String id) {
            this.id = id;
            return this;
        }

        public String getIndexPattern() {
            return indexPattern;
        }

        public RollupJobConfig.Builder setIndexPattern(String indexPattern) {
            this.indexPattern = indexPattern;
            return this;
        }

        public String getRollupIndex() {
            return rollupIndex;
        }

        public RollupJobConfig.Builder setRollupIndex(String rollupIndex) {
            this.rollupIndex = rollupIndex;
            return this;
        }

        public GroupConfig getGroupConfig() {
            return groupConfig;
        }

        public RollupJobConfig.Builder setGroupConfig(GroupConfig groupConfig) {
            this.groupConfig = groupConfig;
            return this;
        }

        public List<MetricConfig> getMetricsConfig() {
            return metricsConfig;
        }

        public RollupJobConfig.Builder setMetricsConfig(List<MetricConfig> metricsConfig) {
            this.metricsConfig = metricsConfig;
            return this;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public RollupJobConfig.Builder setTimeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public String getCron() {
            return cron;
        }

        public RollupJobConfig.Builder setCron(String cron) {
            this.cron = cron;
            return this;
        }

        public int getPageSize() {
            return pageSize;
        }

        public RollupJobConfig.Builder setPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public RollupJobConfig build() {
            if (id == null || id.isEmpty()) {
                throw new IllegalArgumentException("An ID is mandatory.");
            }
            if (indexPattern == null || indexPattern.isEmpty()) {
                throw new IllegalArgumentException("An index pattern is mandatory.");
            }
            if (Regex.isMatchAllPattern(indexPattern)) {
                throw new IllegalArgumentException("Index pattern must not match all indices (as it would match it's own rollup index");
            }
            if (Regex.isSimpleMatchPattern(indexPattern)) {
                if (Regex.simpleMatch(indexPattern, rollupIndex)) {
                    throw new IllegalArgumentException("Index pattern would match rollup index name which is not allowed.");
                }
            }
            if (indexPattern.equals(rollupIndex)) {
                throw new IllegalArgumentException("Rollup index may not be the same as the index pattern.");
            }
            if (rollupIndex == null || rollupIndex.isEmpty()) {
                throw new IllegalArgumentException("A rollup index name is mandatory.");
            }
            if (cron == null || cron.isEmpty()) {
                throw new IllegalArgumentException("A cron schedule is mandatory.");
            }
            if (pageSize <= 0) {
                throw new IllegalArgumentException("Parameter [" + PAGE_SIZE.getPreferredName()
                        + "] is mandatory and  must be a positive long.");
            }
            // Cron doesn't have a parse helper method to see if the cron is valid,
            // so just construct a temporary cron object and if the cron is bad, it'll
            // throw an exception
            Cron testCron = new Cron(cron);
            if (groupConfig == null && (metricsConfig == null || metricsConfig.isEmpty())) {
                throw new IllegalArgumentException("At least one grouping or metric must be configured.");
            }
            return new RollupJobConfig(id, indexPattern, rollupIndex, cron, pageSize, groupConfig,
                    metricsConfig, timeout);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (id != null) {
                builder.field(RollupField.ID.getPreferredName(), id);
            }
            if (indexPattern != null) {
                builder.field(INDEX_PATTERN.getPreferredName(), indexPattern);
            }
            if (indexPattern != null) {
                builder.field(ROLLUP_INDEX.getPreferredName(), rollupIndex);
            }
            if (cron != null) {
                builder.field(CRON.getPreferredName(), cron);
            }
            if (groupConfig != null) {
                builder.field(GROUPS.getPreferredName(), groupConfig);
            }
            if (metricsConfig != null) {
                builder.startArray(METRICS.getPreferredName());
                for (MetricConfig config : metricsConfig) {
                    builder.startObject();
                    config.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();
            }
            if (timeout != null) {
                builder.field(TIMEOUT.getPreferredName(), timeout);
            }
            builder.field(PAGE_SIZE.getPreferredName(), pageSize);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeOptionalString(indexPattern);
            out.writeOptionalString(rollupIndex);
            out.writeOptionalString(cron);
            out.writeOptionalWriteable(groupConfig);
            out.writeList(metricsConfig);
            out.writeTimeValue(timeout);
            out.writeInt(pageSize);
        }
    }
}
