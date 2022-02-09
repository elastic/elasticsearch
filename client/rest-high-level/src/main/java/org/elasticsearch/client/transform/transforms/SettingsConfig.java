/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SettingsConfig implements ToXContentObject {

    private static final ParseField MAX_PAGE_SEARCH_SIZE = new ParseField("max_page_search_size");
    private static final ParseField DOCS_PER_SECOND = new ParseField("docs_per_second");
    private static final ParseField DATES_AS_EPOCH_MILLIS = new ParseField("dates_as_epoch_millis");
    private static final ParseField ALIGN_CHECKPOINTS = new ParseField("align_checkpoints");
    private static final ParseField USE_PIT = new ParseField("use_point_in_time");
    private static final ParseField DEDUCE_MAPPINGS = new ParseField("deduce_mappings");
    private static final int DEFAULT_MAX_PAGE_SEARCH_SIZE = -1;
    private static final float DEFAULT_DOCS_PER_SECOND = -1F;

    // use an integer as we need to code 4 states: true, false, null (unchanged), default (defined server side)
    private static final int DEFAULT_DATES_AS_EPOCH_MILLIS = -1;

    // use an integer as we need to code 4 states: true, false, null (unchanged), default (defined server side)
    private static final int DEFAULT_ALIGN_CHECKPOINTS = -1;

    // use an integer as we need to code 4 states: true, false, null (unchanged), default (defined server side)
    private static final int DEFAULT_USE_PIT = -1;

    // use an integer as we need to code 4 states: true, false, null (unchanged), default (defined server side)
    private static final int DEFAULT_DEDUCE_MAPPINGS = -1;

    private final Integer maxPageSearchSize;
    private final Float docsPerSecond;
    private final Integer datesAsEpochMillis;
    private final Integer alignCheckpoints;
    private final Integer usePit;
    private final Integer deduceMappings;

    private static final ConstructingObjectParser<SettingsConfig, Void> PARSER = new ConstructingObjectParser<>(
        "settings_config",
        true,
        args -> new SettingsConfig(
            (Integer) args[0],
            (Float) args[1],
            (Integer) args[2],
            (Integer) args[3],
            (Integer) args[4],
            (Integer) args[5]
        )
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
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_ALIGN_CHECKPOINTS : p.booleanValue() ? 1 : 0,
            ALIGN_CHECKPOINTS,
            ValueType.BOOLEAN_OR_NULL
        );
        // this boolean requires 4 possible values: true, false, not_specified, default, therefore using a custom parser
        PARSER.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_USE_PIT : p.booleanValue() ? 1 : 0,
            USE_PIT,
            ValueType.BOOLEAN_OR_NULL
        );
        // this boolean requires 4 possible values: true, false, not_specified, default, therefore using a custom parser
        PARSER.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_DEDUCE_MAPPINGS : p.booleanValue() ? 1 : 0,
            DEDUCE_MAPPINGS,
            ValueType.BOOLEAN_OR_NULL
        );
    }

    public static SettingsConfig fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    SettingsConfig(
        Integer maxPageSearchSize,
        Float docsPerSecond,
        Integer datesAsEpochMillis,
        Integer alignCheckpoints,
        Integer usePit,
        Integer deduceMappings
    ) {
        this.maxPageSearchSize = maxPageSearchSize;
        this.docsPerSecond = docsPerSecond;
        this.datesAsEpochMillis = datesAsEpochMillis;
        this.alignCheckpoints = alignCheckpoints;
        this.usePit = usePit;
        this.deduceMappings = deduceMappings;
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
        if (alignCheckpoints != null) {
            if (alignCheckpoints.equals(DEFAULT_ALIGN_CHECKPOINTS)) {
                builder.field(ALIGN_CHECKPOINTS.getPreferredName(), (Boolean) null);
            } else {
                builder.field(ALIGN_CHECKPOINTS.getPreferredName(), alignCheckpoints > 0 ? true : false);
            }
        }
        if (usePit != null) {
            if (usePit.equals(DEFAULT_USE_PIT)) {
                builder.field(USE_PIT.getPreferredName(), (Boolean) null);
            } else {
                builder.field(USE_PIT.getPreferredName(), usePit > 0 ? true : false);
            }
        }
        if (deduceMappings != null) {
            if (deduceMappings.equals(DEFAULT_DEDUCE_MAPPINGS)) {
                builder.field(DEDUCE_MAPPINGS.getPreferredName(), (Boolean) null);
            } else {
                builder.field(DEDUCE_MAPPINGS.getPreferredName(), deduceMappings > 0 ? true : false);
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

    public Boolean getAlignCheckpoints() {
        return alignCheckpoints != null ? alignCheckpoints > 0 : null;
    }

    public Boolean getUsePit() {
        return usePit != null ? usePit > 0 : null;
    }

    public Boolean getDeduceMappings() {
        return deduceMappings != null ? deduceMappings > 0 : null;
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
            && Objects.equals(alignCheckpoints, that.alignCheckpoints)
            && Objects.equals(usePit, that.usePit)
            && Objects.equals(deduceMappings, that.deduceMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxPageSearchSize, docsPerSecond, datesAsEpochMillis, alignCheckpoints, usePit, deduceMappings);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Integer maxPageSearchSize;
        private Float docsPerSecond;
        private Integer datesAsEpochMillis;
        private Integer alignCheckpoints;
        private Integer usePit;
        private Integer deduceMappings;

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
         * @param documentsPerSecond Integer value
         * @return the {@link Builder} with requestsPerSecond set.
         */
        public Builder setRequestsPerSecond(Float documentsPerSecond) {
            this.docsPerSecond = documentsPerSecond == null ? DEFAULT_DOCS_PER_SECOND : documentsPerSecond;
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
         * Whether to align transform checkpoint ranges with date histogram interval.
         *
         * An explicit `null` resets to default.
         *
         * @param alignCheckpoints true if checkpoint ranges should be aligned with date histogram interval.
         * @return the {@link Builder} with alignCheckpoints set.
         */
        public Builder setAlignCheckpoints(Boolean alignCheckpoints) {
            this.alignCheckpoints = alignCheckpoints == null ? DEFAULT_ALIGN_CHECKPOINTS : alignCheckpoints ? 1 : 0;
            return this;
        }

        /**
         * Whether the point in time API should be used for search.
         * Point in time is a more resource friendly way to query. It is used by default. In case of problems
         * you can disable the point in time API usage with this setting.
         *
         * An explicit `null` resets to default.
         *
         * @param usePit true if the point in time API should be used.
         * @return the {@link Builder} with usePit set.
         */
        public Builder setUsePit(Boolean usePit) {
            this.usePit = usePit == null ? DEFAULT_USE_PIT : usePit ? 1 : 0;
            return this;
        }

        /**
         * Whether the destination index mappings should be deduced from the transform config.
         * It is used per default.
         *
         * An explicit `null` resets to default.
         *
         * @param deduceMappings true if the transform should try deducing mappings from the config.
         * @return the {@link Builder} with deduceMappings set.
         */
        public Builder setDeduceMappings(Boolean deduceMappings) {
            this.deduceMappings = deduceMappings == null ? DEFAULT_DEDUCE_MAPPINGS : deduceMappings ? 1 : 0;
            return this;
        }

        public SettingsConfig build() {
            return new SettingsConfig(maxPageSearchSize, docsPerSecond, datesAsEpochMillis, alignCheckpoints, usePit, deduceMappings);
        }
    }
}
