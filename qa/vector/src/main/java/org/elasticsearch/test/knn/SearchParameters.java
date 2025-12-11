/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Optional;

public record SearchParameters(
    int numCandidates,
    int topK,
    double visitPercentage,
    float overSamplingFactor,
    int searchThreads,
    int numSearchers,
    float filterSelectivity,
    boolean filterCached,
    boolean earlyTermination
) implements ToXContentObject {

    static final ObjectParser<SearchParameters.Builder, Void> PARSER = new ObjectParser<>(
        "search_params",
        false,
        SearchParameters.Builder::new
    );

    static {
        PARSER.declareInt(Builder::setNumCandidates, CmdLineArgs.NUM_CANDIDATES_FIELD);
        PARSER.declareInt(Builder::setTopK, CmdLineArgs.K_FIELD);
        PARSER.declareDouble(Builder::setVisitPercentage, CmdLineArgs.VISIT_PERCENTAGE_FIELD);
        PARSER.declareFloat(Builder::setOverSamplingFactor, CmdLineArgs.OVER_SAMPLING_FACTOR_FIELD);
        PARSER.declareInt(Builder::setSearchThreads, CmdLineArgs.SEARCH_THREADS_FIELD);
        PARSER.declareInt(Builder::setNumSearchers, CmdLineArgs.NUM_SEARCHERS_FIELD);
        PARSER.declareBoolean(Builder::setEarlyTermination, CmdLineArgs.EARLY_TERMINATION_FIELD);
        PARSER.declareBoolean(Builder::setFilterCached, CmdLineArgs.FILTER_CACHED);
        PARSER.declareFloat(Builder::setFilterSelectivity, CmdLineArgs.FILTER_SELECTIVITY_FIELD);
    }

    static SearchParameters.Builder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CmdLineArgs.NUM_CANDIDATES_FIELD.getPreferredName(), numCandidates);
        builder.field(CmdLineArgs.K_FIELD.getPreferredName(), topK);
        builder.field(CmdLineArgs.VISIT_PERCENTAGE_FIELD.getPreferredName(), visitPercentage);
        builder.field(CmdLineArgs.OVER_SAMPLING_FACTOR_FIELD.getPreferredName(), overSamplingFactor);
        builder.field(CmdLineArgs.SEARCH_THREADS_FIELD.getPreferredName(), searchThreads);
        builder.field(CmdLineArgs.NUM_SEARCHERS_FIELD.getPreferredName(), numSearchers);
        builder.field(CmdLineArgs.FILTER_CACHED.getPreferredName(), filterCached);
        builder.field(CmdLineArgs.FILTER_SELECTIVITY_FIELD.getPreferredName(), filterSelectivity);
        builder.field(CmdLineArgs.EARLY_TERMINATION_FIELD.getPreferredName(), earlyTermination);
        return builder.endObject();
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private Integer numCandidates;
        private Integer topK;
        private Double visitPercentage;
        private Float overSamplingFactor;
        private Integer searchThreads;
        private Integer numSearchers;
        private Float filterSelectivity;
        private Boolean filterCached;
        private Boolean earlyTermination;

        public Builder setNumCandidates(int numCandidates) {
            this.numCandidates = numCandidates;
            return this;
        }

        public Builder setTopK(int topK) {
            this.topK = topK;
            return this;
        }

        public Builder setVisitPercentage(double visitPercentage) {
            this.visitPercentage = visitPercentage;
            return this;
        }

        public Builder setOverSamplingFactor(float overSamplingFactor) {
            this.overSamplingFactor = overSamplingFactor;
            return this;
        }

        public Builder setSearchThreads(int searchThreads) {
            this.searchThreads = searchThreads;
            return this;
        }

        public Builder setNumSearchers(int numSearchers) {
            this.numSearchers = numSearchers;
            return this;
        }

        public Builder setFilterCached(boolean filterCached) {
            this.filterCached = filterCached;
            return this;
        }

        public Builder setFilterSelectivity(float filterSelectivity) {
            this.filterSelectivity = filterSelectivity;
            return this;
        }

        public Builder setEarlyTermination(boolean earlyTermination) {
            this.earlyTermination = earlyTermination;
            return this;
        }

        private Builder setNullValues(SearchParameters params) {
            // Only set the null members, don't overwrite the set values
            this.numCandidates = Optional.ofNullable(numCandidates).orElse(params.numCandidates());
            this.topK = Optional.ofNullable(topK).orElse(params.topK());
            this.visitPercentage = Optional.ofNullable(visitPercentage).orElse(params.visitPercentage());
            this.overSamplingFactor = Optional.ofNullable(overSamplingFactor).orElse(params.overSamplingFactor());
            this.searchThreads = Optional.ofNullable(searchThreads).orElse(params.searchThreads());
            this.numSearchers = Optional.ofNullable(numSearchers).orElse(params.numSearchers());
            this.filterCached = Optional.ofNullable(filterCached).orElse(params.filterCached());
            this.filterSelectivity = Optional.ofNullable(filterSelectivity).orElse(params.filterSelectivity());
            this.earlyTermination = Optional.ofNullable(earlyTermination).orElse(params.earlyTermination());
            return this;
        }

        /**
         * Builds a new {@code SearchOptions} object using the current values in the builder combined
         * with the default values provided in the specified {@code SearchOptions} instance.
         * If a field in the builder is null, the corresponding value from the provided defaults
         * is applied.
         *
         * This slightly unusual builder pattern enables the parser to create partially populated
         * {@code Builders}, the unset values are then set before use.
         *
         * @param defaults the {@code SearchOptions} instance containing default values
         * @return a new {@code SearchOptions} object with the combined values
         */
        public SearchParameters buildWithDefaults(SearchParameters defaults) {
            setNullValues(defaults);
            return new SearchParameters(
                numCandidates,
                topK,
                visitPercentage,
                overSamplingFactor,
                searchThreads,
                numSearchers,
                filterSelectivity,
                filterCached,
                earlyTermination
            );
        }
    }
}
