/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.rollup.job.config;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

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
public class GroupConfig implements Validatable, ToXContentObject {

    static final String NAME = "groups";
    private static final ConstructingObjectParser<GroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, true, args ->
            new GroupConfig((DateHistogramGroupConfig) args[0], (HistogramGroupConfig) args[1], (TermsGroupConfig) args[2]));
        PARSER.declareObject(constructorArg(),
            (p, c) -> DateHistogramGroupConfig.fromXContent(p), new ParseField(DateHistogramGroupConfig.NAME));
        PARSER.declareObject(optionalConstructorArg(),
            (p, c) -> HistogramGroupConfig.fromXContent(p), new ParseField(HistogramGroupConfig.NAME));
        PARSER.declareObject(optionalConstructorArg(),
            (p, c) -> TermsGroupConfig.fromXContent(p), new ParseField(TermsGroupConfig.NAME));
    }

    private final DateHistogramGroupConfig dateHistogram;
    private final @Nullable
    HistogramGroupConfig histogram;
    private final @Nullable
    TermsGroupConfig terms;

    public GroupConfig(final DateHistogramGroupConfig dateHistogram) {
        this(dateHistogram, null, null);
    }

    public GroupConfig(final DateHistogramGroupConfig dateHistogram,
                       final @Nullable HistogramGroupConfig histogram,
                       final @Nullable TermsGroupConfig terms) {
        this.dateHistogram = dateHistogram;
        this.histogram = histogram;
        this.terms = terms;
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (dateHistogram != null) {
            final Optional<ValidationException> dateHistogramValidationErrors = dateHistogram.validate();
            if (dateHistogramValidationErrors != null && dateHistogramValidationErrors.isPresent()) {
                validationException.addValidationErrors(dateHistogramValidationErrors.get());
            }
        } else {
            validationException.addValidationError("Date histogram must not be null");
        }
        if (histogram != null) {
            final Optional<ValidationException> histogramValidationErrors = histogram.validate();
            if (histogramValidationErrors != null && histogramValidationErrors.isPresent()) {
                validationException.addValidationErrors(histogramValidationErrors.get());
            }
        }
        if (terms != null) {
            final Optional<ValidationException> termsValidationErrors = terms.validate();
            if (termsValidationErrors != null && termsValidationErrors.isPresent()) {
                validationException.addValidationErrors(termsValidationErrors.get());
            }
        }
        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(validationException);
    }

    /**
     * @return the configuration of the date histogram
     */
    public DateHistogramGroupConfig getDateHistogram() {
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(DateHistogramGroupConfig.NAME, dateHistogram);
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
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final GroupConfig that = (GroupConfig) other;
        return Objects.equals(dateHistogram, that.dateHistogram)
            && Objects.equals(histogram, that.histogram)
            && Objects.equals(terms, that.terms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateHistogram, histogram, terms);
    }

    public static GroupConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
