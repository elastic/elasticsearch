/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The configuration object for the groups section in the rollup config.
 * Basically just a wrapper for histo/date histo/terms objects
 *
 * {
 *     "groups": [
 *        "date_histogram": {...},
 *        "histogram" : {...},
 *        "terms" : {...}
 *     ]
 * }
 */
public class RollupActionGroupConfig implements Writeable, ToXContentObject {

    public static final String NAME = "groups";
    private static final ConstructingObjectParser<RollupActionGroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, args ->
            new RollupActionGroupConfig((RollupActionDateHistogramGroupConfig) args[0], (HistogramGroupConfig) args[1],
                (TermsGroupConfig) args[2]));
        PARSER.declareObject(constructorArg(),
            (p, c) -> RollupActionDateHistogramGroupConfig.fromXContent(p), new ParseField(RollupActionDateHistogramGroupConfig.NAME));
        PARSER.declareObject(optionalConstructorArg(),
            (p, c) -> HistogramGroupConfig.fromXContent(p), new ParseField(HistogramGroupConfig.NAME));
        PARSER.declareObject(optionalConstructorArg(),
            (p, c) -> TermsGroupConfig.fromXContent(p), new ParseField(TermsGroupConfig.NAME));
    }

    private final RollupActionDateHistogramGroupConfig dateHistogram;
    private final @Nullable HistogramGroupConfig histogram;
    private final @Nullable TermsGroupConfig terms;

    public RollupActionGroupConfig(final RollupActionDateHistogramGroupConfig dateHistogram) {
        this(dateHistogram, null, null);
    }

    public RollupActionGroupConfig(final RollupActionDateHistogramGroupConfig dateHistogram,
                                   final @Nullable HistogramGroupConfig histogram,
                                   final @Nullable TermsGroupConfig terms) {
        if (dateHistogram == null) {
            throw new IllegalArgumentException("Date histogram must not be null");
        }
        this.dateHistogram = dateHistogram;
        this.histogram = histogram;
        this.terms = terms;
    }

    public RollupActionGroupConfig(final StreamInput in) throws IOException {
        dateHistogram = RollupActionDateHistogramGroupConfig.readFrom(in);
        histogram = in.readOptionalWriteable(HistogramGroupConfig::new);
        terms = in.readOptionalWriteable(TermsGroupConfig::new);
    }

    /**
     * @return the configuration of the date histogram
     */
    public RollupActionDateHistogramGroupConfig getDateHistogram() {
        return dateHistogram;
    }

    /**
     * @return the configuration of the histogram
     */
    @Nullable
    public HistogramGroupConfig getHistogram() {
        return histogram;
    }

    /**
     * @return the configuration of the terms
     */
    @Nullable
    public TermsGroupConfig getTerms() {
        return terms;
    }

    public Set<String> getAllFields() {
        Set<String> fields = new HashSet<>();
        fields.add(dateHistogram.getField());
        if (histogram != null) {
            fields.addAll(asList(histogram.getFields()));
        }
        if (terms != null) {
            fields.addAll(asList(terms.getFields()));
        }
        return Collections.unmodifiableSet(fields);
    }

    public void validateMappings(final Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                 final ActionRequestValidationException validationException) {
        dateHistogram.validateMappings(fieldCapsResponse, validationException);
        if (histogram != null) {
            histogram.validateMappings(fieldCapsResponse, validationException);
        }
        if (terms != null) {
            terms.validateMappings(fieldCapsResponse, validationException);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(RollupActionDateHistogramGroupConfig.NAME, dateHistogram);
            if (histogram != null) {
                builder.field(HistogramGroupConfig.NAME, histogram);
            }
            if (terms != null) {
                builder.field(TermsGroupConfig.NAME, terms);
            }
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        dateHistogram.writeTo(out);
        out.writeOptionalWriteable(histogram);
        out.writeOptionalWriteable(terms);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final RollupActionGroupConfig that = (RollupActionGroupConfig) other;
        return Objects.equals(dateHistogram, that.dateHistogram)
            && Objects.equals(histogram, that.histogram)
            && Objects.equals(terms, that.terms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateHistogram, histogram, terms);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static RollupActionGroupConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
