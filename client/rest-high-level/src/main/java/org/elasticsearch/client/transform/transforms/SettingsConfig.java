/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SettingsConfig implements ToXContentObject {

    private static final ParseField MAX_PAGE_SEARCH_SIZE = new ParseField("max_page_search_size");
    private static final ParseField DOCS_PER_SECOND = new ParseField("docs_per_second");
    private static final ParseField DATES_AS_EPOCH_MILLIS = new ParseField("dates_as_epoch_millis");
    private static final ParseField INTERIM_RESULTS = new ParseField("interim_results");
    private static final int DEFAULT_MAX_PAGE_SEARCH_SIZE = -1;
    private static final float DEFAULT_DOCS_PER_SECOND = -1F;

    // use an integer as we need to code 4 states: true, false, null (unchanged), default (defined server side)
    private static final int DEFAULT_DATES_AS_EPOCH_MILLIS = -1;

    // use an integer as we need to code 4 states: true, false, null (unchanged), default (defined server side)
    private static final int DEFAULT_INTERIM_RESULTS = -1;

    private final Integer maxPageSearchSize;
    private final Float docsPerSecond;
    private final Integer datesAsEpochMillis;
    private final Integer interimResults;

    private static final ConstructingObjectParser<SettingsConfig, Void> PARSER = new ConstructingObjectParser<>(
        "settings_config",
        true,
        args -> new SettingsConfig((Integer) args[0], (Float) args[1], (Integer) args[2], (Integer) args[3])
    );

    static {
        PARSER.declareIntOrNull(optionalConstructorArg(), DEFAULT_MAX_PAGE_SEARCH_SIZE, MAX_PAGE_SEARCH_SIZE);
        PARSER.declareFloatOrNull(optionalConstructorArg(), DEFAULT_DOCS_PER_SECOND, DOCS_PER_SECOND);
        // this boolean requires 4 possible values: true, false, not_specified, default, therefore using a custom parser
        PARSER.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_DATES_AS_EPOCH_MILLIS : p.booleanValue() ? 1 : 0,
            DATES_AS_EPOCH_MILLIS,
            ValueType.BOOLEAN_OR_NULL
        );
        // this boolean requires 4 possible values: true, false, not_specified, default, therefore using a custom parser
        PARSER.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_INTERIM_RESULTS : p.booleanValue() ? 1 : 0,
            INTERIM_RESULTS,
            ValueType.BOOLEAN_OR_NULL
        );
    }

    public static SettingsConfig fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    SettingsConfig(Integer maxPageSearchSize, Float docsPerSecond, Integer datesAsEpochMillis, Integer interimResults) {
        this.maxPageSearchSize = maxPageSearchSize;
        this.docsPerSecond = docsPerSecond;
        this.datesAsEpochMillis = datesAsEpochMillis;
        this.interimResults = interimResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (maxPageSearchSize != null) {
            if (maxPageSearchSize.equals(DEFAULT_MAX_PAGE_SEARCH_SIZE)) {
                builder.field(MAX_PAGE_SEARCH_SIZE.getPreferredName(), (Integer) null);
            } else {
                builder.field(MAX_PAGE_SEARCH_SIZE.getPreferredName(), maxPageSearchSize);
            }
        }
        if (docsPerSecond != null) {
            if (docsPerSecond.equals(DEFAULT_DOCS_PER_SECOND)) {
                builder.field(DOCS_PER_SECOND.getPreferredName(), (Float) null);
            } else {
                builder.field(DOCS_PER_SECOND.getPreferredName(), docsPerSecond);
            }
        }
        if (datesAsEpochMillis != null) {
            if (datesAsEpochMillis.equals(DEFAULT_DATES_AS_EPOCH_MILLIS)) {
                builder.field(DATES_AS_EPOCH_MILLIS.getPreferredName(), (Boolean) null);
            } else {
                builder.field(DATES_AS_EPOCH_MILLIS.getPreferredName(), datesAsEpochMillis > 0 ? true : false);
            }
        }
        if (interimResults != null) {
            if (interimResults.equals(DEFAULT_INTERIM_RESULTS)) {
                builder.field(INTERIM_RESULTS.getPreferredName(), (Boolean) null);
            } else {
                builder.field(INTERIM_RESULTS.getPreferredName(), interimResults > 0 ? true : false);
            }
        }
        builder.endObject();
        return builder;
    }

    public Integer getMaxPageSearchSize() {
        return maxPageSearchSize;
    }

    public Float getDocsPerSecond() {
        return docsPerSecond;
    }

    public Boolean getDatesAsEpochMillis() {
        return datesAsEpochMillis != null ? datesAsEpochMillis > 0 : null;
    }

    public Boolean getInterimResults() {
        return interimResults != null ? interimResults > 0 : null;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        SettingsConfig that = (SettingsConfig) other;
        return Objects.equals(maxPageSearchSize, that.maxPageSearchSize)
            && Objects.equals(docsPerSecond, that.docsPerSecond)
            && Objects.equals(datesAsEpochMillis, that.datesAsEpochMillis)
            && Objects.equals(interimResults, that.interimResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxPageSearchSize, docsPerSecond, datesAsEpochMillis, interimResults);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Integer maxPageSearchSize;
        private Float docsPerSecond;
        private Integer datesAsEpochMillis;
        private Integer interimResults;

        /**
         * Sets the paging maximum paging maxPageSearchSize that transform can use when
         * pulling the data from the source index.
         *
         * If OOM is triggered, the paging maxPageSearchSize is dynamically reduced so that the transform can continue to gather data.
         *
         * @param maxPageSearchSize Integer value between 10 and 10_000
         * @return the {@link Builder} with the paging maxPageSearchSize set.
         */
        public Builder setMaxPageSearchSize(Integer maxPageSearchSize) {
            this.maxPageSearchSize = maxPageSearchSize == null ? DEFAULT_MAX_PAGE_SEARCH_SIZE : maxPageSearchSize;
            return this;
        }

        /**
         * Sets the docs per second that transform can use when pulling the data from the source index.
         *
         * This setting throttles transform by issuing queries less often, however processing still happens in
         * batches. A value of 0 disables throttling (default).
         *
         * @param docsPerSecond Integer value
         * @return the {@link Builder} with requestsPerSecond set.
         */
        public Builder setRequestsPerSecond(Float docsPerSecond) {
            this.docsPerSecond = docsPerSecond == null ? DEFAULT_DOCS_PER_SECOND : docsPerSecond;
            return this;
        }

        /**
         * Whether to write the output of a date aggregation as millis since epoch or as formatted string (ISO format).
         *
         * Transforms created before 7.11 write dates as epoch_millis. The new default is ISO string.
         * You can use this setter to configure the old style writing as epoch millis.
         *
         * An explicit `null` resets to default.
         *
         * @param datesAsEpochMillis true if dates should be written as epoch_millis.
         * @return the {@link Builder} with datesAsEpochMilli set.
         */
        public Builder setDatesAsEpochMillis(Boolean datesAsEpochMillis) {
            this.datesAsEpochMillis = datesAsEpochMillis == null ? DEFAULT_DATES_AS_EPOCH_MILLIS : datesAsEpochMillis ? 1 : 0;
            return this;
        }

        /**
         * Whether to write interim results in transform checkpoints.
         *
         * An explicit `null` resets to default.
         *
         * @param interimResults true if interim results should be written.
         * @return the {@link Builder} with interimResults set.
         */
        public Builder setInterimResults(Boolean interimResults) {
            this.interimResults = interimResults == null ? DEFAULT_INTERIM_RESULTS : interimResults ? 1 : 0;
            return this;
        }

        public SettingsConfig build() {
            return new SettingsConfig(maxPageSearchSize, docsPerSecond, datesAsEpochMillis, interimResults);
        }
    }
}
