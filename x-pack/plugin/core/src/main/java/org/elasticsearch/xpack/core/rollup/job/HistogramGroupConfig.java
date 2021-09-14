/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

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
public class HistogramGroupConfig implements Writeable, ToXContentObject {

    public static final String NAME = "histogram";
    public static final String INTERVAL = "interval";
    private static final String FIELDS = "fields";
    private static final ConstructingObjectParser<HistogramGroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, args -> {
            @SuppressWarnings("unchecked") List<String> fields = (List<String>) args[1];
            return new HistogramGroupConfig((long) args[0], fields != null ? fields.toArray(new String[fields.size()]) : null);
        });
        PARSER.declareLong(constructorArg(), new ParseField(INTERVAL));
        PARSER.declareStringArray(constructorArg(), new ParseField(FIELDS));
    }

    private final long interval;
    private final String[] fields;

    public HistogramGroupConfig(final long interval, final String... fields) {
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be a positive long");
        }
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("Fields must have at least one value");
        }
        this.interval = interval;
        this.fields = fields;
    }

    public HistogramGroupConfig(final StreamInput in) throws IOException {
        interval = in.readVLong();
        fields = in.readStringArray();
    }

    public long getInterval() {
        return interval;
    }

    public String[] getFields() {
        return fields;
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
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(INTERVAL, interval);
            builder.field(FIELDS, fields);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(interval);
        out.writeStringArray(fields);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final HistogramGroupConfig that = (HistogramGroupConfig) other;
        return Objects.equals(interval, that.interval) && Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, Arrays.hashCode(fields));
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static HistogramGroupConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
