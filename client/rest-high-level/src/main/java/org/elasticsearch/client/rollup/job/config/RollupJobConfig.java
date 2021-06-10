/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup.job.config;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the configuration details of a rollup job, such as the groupings, metrics, what
 * index to rollup and where to roll them to.
 */
public class RollupJobConfig implements Validatable, ToXContentObject {

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
        PARSER = new ConstructingObjectParser<>("rollup_job_config", true, (args, optionalId) -> {
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
        this.id = id;
        this.indexPattern = indexPattern;
        this.rollupIndex = rollupIndex;
        this.groupConfig = groupConfig;
        this.metricsConfig = metricsConfig != null ? metricsConfig : Collections.emptyList();
        this.timeout = timeout != null ? timeout : DEFAULT_TIMEOUT;
        this.cron = cron;
        this.pageSize = pageSize;
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (id == null || id.isEmpty()) {
            validationException.addValidationError("Id must be a non-null, non-empty string");
        }
        if (indexPattern == null || indexPattern.isEmpty()) {
            validationException.addValidationError("Index pattern must be a non-null, non-empty string");
        } else if (Regex.isMatchAllPattern(indexPattern)) {
            validationException.addValidationError("Index pattern must not match all indices (as it would match it's own rollup index");
        } else if (indexPattern != null && indexPattern.equals(rollupIndex)) {
            validationException.addValidationError("Rollup index may not be the same as the index pattern");
        } else if (Regex.isSimpleMatchPattern(indexPattern) && Regex.simpleMatch(indexPattern, rollupIndex)) {
            validationException.addValidationError("Index pattern would match rollup index name which is not allowed");
        }

        if (rollupIndex == null || rollupIndex.isEmpty()) {
            validationException.addValidationError("Rollup index must be a non-null, non-empty string");
        }
        if (cron == null || cron.isEmpty()) {
            validationException.addValidationError("Cron schedule must be a non-null, non-empty string");
        }
        if (pageSize <= 0) {
            validationException.addValidationError("Page size is mandatory and  must be a positive long");
        }
        if (groupConfig == null && (metricsConfig == null || metricsConfig.isEmpty())) {
            validationException.addValidationError("At least one grouping or metric must be configured");
        }
        if (groupConfig != null) {
            final Optional<ValidationException> groupValidationErrors = groupConfig.validate();
            if (groupValidationErrors != null && groupValidationErrors.isPresent()) {
                validationException.addValidationErrors(groupValidationErrors.get());
            }
        }
        if (metricsConfig != null) {
            for (MetricConfig metricConfig : metricsConfig) {
                final Optional<ValidationException> metricsValidationErrors = metricConfig.validate();
                if (metricsValidationErrors != null && metricsValidationErrors.isPresent()) {
                    validationException.addValidationErrors(metricsValidationErrors.get());
                }
            }
        }
        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(validationException);
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

    public static RollupJobConfig fromXContent(final XContentParser parser, @Nullable final String optionalJobId) throws IOException {
        return PARSER.parse(parser, optionalJobId);
    }
}
