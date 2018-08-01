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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The configuration object for the histograms in the rollup config
 *
 * {
 *     "groups": [
 *        "histogram": {
 *            "fields" : [ "foo", "bar" ],
 *            "interval" : 123
 *        }
 *     ]
 * }
 */
public class HistoGroupConfig implements Writeable, ToXContentFragment {
    private static final String NAME = "histo_group_config";
    public static final ObjectParser<HistoGroupConfig.Builder, Void> PARSER
            = new ObjectParser<>(NAME, HistoGroupConfig.Builder::new);

    private static final ParseField INTERVAL = new ParseField("interval");
    private static final ParseField FIELDS = new ParseField("fields");

    private final long interval;
    private final String[] fields;

    static {
        PARSER.declareLong(HistoGroupConfig.Builder::setInterval, INTERVAL);
        PARSER.declareStringArray(HistoGroupConfig.Builder::setFields, FIELDS);
    }

    private HistoGroupConfig(long interval, String[] fields) {
        this.interval = interval;
        this.fields = fields;
    }

    HistoGroupConfig(StreamInput in) throws IOException {
        interval = in.readVLong();
        fields = in.readStringArray();
    }

    public long getInterval() {
        return interval;
    }

    public String[] getFields() {
        return fields;
    }

    /**
     * This returns a set of aggregation builders which represent the configured
     * set of histograms.  Used by the rollup indexer to iterate over historical data
     */
    public List<CompositeValuesSourceBuilder<?>> toBuilders() {
        if (fields.length == 0) {
            return Collections.emptyList();
        }

        return Arrays.stream(fields).map(f -> {
            HistogramValuesSourceBuilder vsBuilder
                    = new HistogramValuesSourceBuilder(RollupField.formatIndexerAggName(f, HistogramAggregationBuilder.NAME));
            vsBuilder.interval(interval);
            vsBuilder.field(f);
            vsBuilder.missingBucket(true);
            return vsBuilder;
        }).collect(Collectors.toList());
    }

    /**
     * @return A map representing this config object as a RollupCaps aggregation object
     */
    public Map<String, Object> toAggCap() {
        Map<String, Object> map = new HashMap<>(2);
        map.put("agg", HistogramAggregationBuilder.NAME);
        map.put(INTERVAL.getPreferredName(), interval);
        return map;
    }

    public Map<String, Object> getMetadata() {
        return Collections.singletonMap(RollupField.formatMetaField(RollupField.INTERVAL), interval);
    }

    public Set<String> getAllFields() {
        return Arrays.stream(fields).collect(Collectors.toSet());
    }

    public void validateMappings(Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                 ActionRequestValidationException validationException) {

        Arrays.stream(fields).forEach(field -> {
            Map<String, FieldCapabilities> fieldCaps = fieldCapsResponse.get(field);
            if (fieldCaps != null && fieldCaps.isEmpty() == false) {
                fieldCaps.forEach((key, value) -> {
                    if (RollupField.NUMERIC_FIELD_MAPPER_TYPES.contains(key)) {
                        if (value.isAggregatable() == false) {
                            validationException.addValidationError("The field [" + field + "] must be aggregatable across all indices, " +
                                    "but is not.");
                        }
                    } else {
                        validationException.addValidationError("The field referenced by a histo group must be a [numeric] type, " +
                                "but found " + fieldCaps.keySet().toString() + " for field [" + field + "]");
                    }
                });
            } else {
                validationException.addValidationError("Could not find a [numeric] field with name [" + field
                        + "] in any of the indices matching the index pattern.");
            }
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INTERVAL.getPreferredName(), interval);
        builder.field(FIELDS.getPreferredName(), fields);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(interval);
        out.writeStringArray(fields);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        HistoGroupConfig that = (HistoGroupConfig) other;

        return Objects.equals(this.interval, that.interval)
                && Arrays.equals(this.fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, Arrays.hashCode(fields));
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class Builder {
        private long interval = 0;
        private List<String> fields;

        public long getInterval() {
            return interval;
        }

        public HistoGroupConfig.Builder setInterval(long interval) {
            this.interval = interval;
            return this;
        }

        public List<String> getFields() {
            return fields;
        }

        public HistoGroupConfig.Builder setFields(List<String> fields) {
            this.fields = fields;
            return this;
        }

        public HistoGroupConfig build() {
            if (interval <= 0) {
                throw new IllegalArgumentException("Parameter [" + INTERVAL.getPreferredName() + "] must be a positive long.");
            }
            if (fields == null || fields.isEmpty()) {
                throw new IllegalArgumentException("Parameter [" + FIELDS + "] must have at least one value.");
            }
            return new HistoGroupConfig(interval, fields.toArray(new String[0]));
        }
    }
}
