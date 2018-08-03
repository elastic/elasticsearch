/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Arrays.asList;

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
public class GroupConfig implements Writeable, ToXContentObject {
    private static final String NAME = "grouping_config";
    private static final ParseField DATE_HISTO = new ParseField("date_histogram");
    private static final ParseField HISTO = new ParseField("histogram");
    private static final ParseField TERMS = new ParseField("terms");

    private final DateHistogramGroupConfig dateHisto;
    private final HistogramGroupConfig histo;
    private final TermsGroupConfig terms;

    public static final ObjectParser<GroupConfig.Builder, Void> PARSER = new ObjectParser<>(NAME, GroupConfig.Builder::new);

    static {
        PARSER.declareObject(GroupConfig.Builder::setDateHisto, (p,c) -> DateHistogramGroupConfig.fromXContent(p), DATE_HISTO);
        PARSER.declareObject(GroupConfig.Builder::setHisto, (p,c) -> HistogramGroupConfig.fromXContent(p), HISTO);
        PARSER.declareObject(GroupConfig.Builder::setTerms, (p,c) -> TermsGroupConfig.fromXContent(p), TERMS);
    }

    private GroupConfig(DateHistogramGroupConfig dateHisto, @Nullable HistogramGroupConfig histo, @Nullable TermsGroupConfig terms) {
        this.dateHisto = Objects.requireNonNull(dateHisto, "A date_histogram group is mandatory");
        this.histo = histo;
        this.terms = terms;
    }

    GroupConfig(StreamInput in) throws IOException {
        dateHisto = new DateHistogramGroupConfig(in);
        histo = in.readOptionalWriteable(HistogramGroupConfig::new);
        terms = in.readOptionalWriteable(TermsGroupConfig::new);
    }

    public DateHistogramGroupConfig getDateHisto() {
        return dateHisto;
    }

    public HistogramGroupConfig getHisto() {
        return histo;
    }

    public TermsGroupConfig getTerms() {
        return terms;
    }

    public Set<String> getAllFields() {
        Set<String> fields = new HashSet<>();
        fields.add(dateHisto.getField());
        if (histo != null) {
            fields.addAll(asList(histo.getFields()));
        }
        if (terms != null) {
            fields.addAll(asList(terms.getFields()));
        }
        return fields;
    }

    public void validateMappings(Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                                             ActionRequestValidationException validationException) {
        dateHisto.validateMappings(fieldCapsResponse, validationException);
        if (histo != null) {
            histo.validateMappings(fieldCapsResponse, validationException);
        }
        if (terms != null) {
            terms.validateMappings(fieldCapsResponse, validationException);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DATE_HISTO.getPreferredName(), dateHisto);
        if (histo != null) {
            builder.field(HISTO.getPreferredName(), histo);
        }
        if (terms != null) {
            builder.field(TERMS.getPreferredName(), terms);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        dateHisto.writeTo(out);
        out.writeOptionalWriteable(histo);
        out.writeOptionalWriteable(terms);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        GroupConfig that = (GroupConfig) other;

        return Objects.equals(this.dateHisto, that.dateHisto)
                && Objects.equals(this.histo, that.histo)
                && Objects.equals(this.terms, that.terms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateHisto, histo, terms);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class Builder {
        private DateHistogramGroupConfig dateHisto;
        private HistogramGroupConfig histo;
        private TermsGroupConfig terms;

        public DateHistogramGroupConfig getDateHisto() {
            return dateHisto;
        }

        public GroupConfig.Builder setDateHisto(DateHistogramGroupConfig dateHisto) {
            this.dateHisto = dateHisto;
            return this;
        }

        public HistogramGroupConfig getHisto() {
            return histo;
        }

        public GroupConfig.Builder setHisto(HistogramGroupConfig histo) {
            this.histo = histo;
            return this;
        }

        public TermsGroupConfig getTerms() {
            return terms;
        }

        public GroupConfig.Builder setTerms(TermsGroupConfig terms) {
            this.terms = terms;
            return this;
        }

        public GroupConfig build() {
            if (dateHisto == null) {
                throw new IllegalArgumentException("A date_histogram group is mandatory");
            }
            return new GroupConfig(dateHisto, histo, terms);
        }
    }
}
