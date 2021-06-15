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
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
public class HistogramGroupConfig implements Validatable, ToXContentObject {

    static final String NAME = "histogram";
    private static final String INTERVAL = "interval";
    private static final String FIELDS = "fields";

    private static final ConstructingObjectParser<HistogramGroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, true, args -> {
            @SuppressWarnings("unchecked") List<String> fields = (List<String>) args[1];
            return new HistogramGroupConfig((long) args[0], fields != null ? fields.toArray(new String[fields.size()]) : null);
        });
        PARSER.declareLong(constructorArg(), new ParseField(INTERVAL));
        PARSER.declareStringArray(constructorArg(), new ParseField(FIELDS));
    }

    private final long interval;
    private final String[] fields;

    public HistogramGroupConfig(final long interval, final String... fields) {
        this.interval = interval;
        this.fields = fields;
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (fields == null || fields.length == 0) {
            validationException.addValidationError("Fields must have at least one value");
        }
        if (interval <= 0) {
            validationException.addValidationError("Interval must be a positive long");
        }
        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(validationException);
    }

    public long getInterval() {
        return interval;
    }

    public String[] getFields() {
        return fields;
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

    public static HistogramGroupConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
