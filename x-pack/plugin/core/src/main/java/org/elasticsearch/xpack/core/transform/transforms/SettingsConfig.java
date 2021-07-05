/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SettingsConfig implements Writeable, ToXContentObject {
    public static final ConstructingObjectParser<SettingsConfig, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<SettingsConfig, Void> LENIENT_PARSER = createParser(true);

    private static final int DEFAULT_MAX_PAGE_SEARCH_SIZE = -1;
    private static final float DEFAULT_DOCS_PER_SECOND = -1F;
    private static final int DEFAULT_DATES_AS_EPOCH_MILLIS = -1;
    private static final int DEFAULT_INTERIM_RESULTS = -1;

    private static ConstructingObjectParser<SettingsConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<SettingsConfig, Void> parser = new ConstructingObjectParser<>(
            "transform_config_settings",
            lenient,
            args -> new SettingsConfig((Integer) args[0], (Float) args[1], (Integer) args[2], (Integer) args[3])
        );
        parser.declareIntOrNull(optionalConstructorArg(), DEFAULT_MAX_PAGE_SEARCH_SIZE, TransformField.MAX_PAGE_SEARCH_SIZE);
        parser.declareFloatOrNull(optionalConstructorArg(), DEFAULT_DOCS_PER_SECOND, TransformField.DOCS_PER_SECOND);
        // this boolean requires 4 possible values: true, false, not_specified, default, therefore using a custom parser
        parser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_DATES_AS_EPOCH_MILLIS : p.booleanValue() ? 1 : 0,
            TransformField.DATES_AS_EPOCH_MILLIS,
            ValueType.BOOLEAN_OR_NULL
        );
        // this boolean requires 4 possible values: true, false, not_specified, default, therefore using a custom parser
        parser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_INTERIM_RESULTS : p.booleanValue() ? 1 : 0,
            TransformField.INTERIM_RESULTS,
            ValueType.BOOLEAN_OR_NULL
        );
        return parser;
    }

    private final Integer maxPageSearchSize;
    private final Float docsPerSecond;
    private final Integer datesAsEpochMillis;
    private final Integer interimResults;

    public SettingsConfig() {
        this(null, null, (Integer) null, (Integer) null);
    }

    public SettingsConfig(Integer maxPageSearchSize, Float docsPerSecond, Boolean datesAsEpochMillis, Boolean interimResults) {
        this(
            maxPageSearchSize,
            docsPerSecond,
            datesAsEpochMillis == null ? null : datesAsEpochMillis ? 1 : 0,
            interimResults == null ? null : interimResults ? 1 : 0
        );
    }

    public SettingsConfig(Integer maxPageSearchSize, Float docsPerSecond, Integer datesAsEpochMillis, Integer interimResults) {
        this.maxPageSearchSize = maxPageSearchSize;
        this.docsPerSecond = docsPerSecond;
        this.datesAsEpochMillis = datesAsEpochMillis;
        this.interimResults = interimResults;
    }

    public SettingsConfig(final StreamInput in) throws IOException {
        this.maxPageSearchSize = in.readOptionalInt();
        this.docsPerSecond = in.readOptionalFloat();
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            this.datesAsEpochMillis = in.readOptionalInt();
        } else {
            this.datesAsEpochMillis = DEFAULT_DATES_AS_EPOCH_MILLIS;
        }
        if (in.getVersion().onOrAfter(Version.CURRENT)) {  // TODO: 7.15
            this.interimResults = in.readOptionalInt();
        } else {
            this.interimResults = DEFAULT_INTERIM_RESULTS;
        }
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

    public Integer getDatesAsEpochMillisForUpdate() {
        return datesAsEpochMillis;
    }

    public Boolean getInterimResults() {
        return interimResults != null ? interimResults > 0 : null;
    }

    public Integer getInterimResultsForUpdate() {
        return interimResults;
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (maxPageSearchSize != null && (maxPageSearchSize < 10 || maxPageSearchSize > MultiBucketConsumerService.DEFAULT_MAX_BUCKETS)) {
            validationException = addValidationError(
                "settings.max_page_search_size ["
                    + maxPageSearchSize
                    + "] is out of range. The minimum value is 10 and the maximum is "
                    + MultiBucketConsumerService.DEFAULT_MAX_BUCKETS,
                validationException
            );
        }

        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(maxPageSearchSize);
        out.writeOptionalFloat(docsPerSecond);
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            out.writeOptionalInt(datesAsEpochMillis);
        }
        if (out.getVersion().onOrAfter(Version.CURRENT)) {  // TODO: 7.15
            out.writeOptionalInt(interimResults);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // do not write default values
        if (maxPageSearchSize != null && (maxPageSearchSize.equals(DEFAULT_MAX_PAGE_SEARCH_SIZE) == false)) {
            builder.field(TransformField.MAX_PAGE_SEARCH_SIZE.getPreferredName(), maxPageSearchSize);
        }
        if (docsPerSecond != null && (docsPerSecond.equals(DEFAULT_DOCS_PER_SECOND) == false)) {
            builder.field(TransformField.DOCS_PER_SECOND.getPreferredName(), docsPerSecond);
        }
        if (datesAsEpochMillis != null && (datesAsEpochMillis.equals(DEFAULT_DATES_AS_EPOCH_MILLIS) == false)) {
            builder.field(TransformField.DATES_AS_EPOCH_MILLIS.getPreferredName(), datesAsEpochMillis > 0 ? true : false);
        }
        if (interimResults != null && (interimResults.equals(DEFAULT_INTERIM_RESULTS) == false)) {
            builder.field(TransformField.INTERIM_RESULTS.getPreferredName(), interimResults > 0 ? true : false);
        }
        builder.endObject();
        return builder;
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

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static SettingsConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public static class Builder {
        private Integer maxPageSearchSize;
        private Float docsPerSecond;
        private Integer datesAsEpochMillis;
        private Integer interimResults;

        /**
         * Default builder
         */
        public Builder() {}

        /**
         * Builder starting from existing settings as base, for the purpose of partially updating settings.
         *
         * @param base base settings
         */
        public Builder(SettingsConfig base) {
            this.maxPageSearchSize = base.maxPageSearchSize;
            this.docsPerSecond = base.docsPerSecond;
            this.datesAsEpochMillis = base.datesAsEpochMillis;
            this.interimResults = base.interimResults;
        }

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
         * TODO: Comment
         *
         * An explicit `null` resets to default.
         *
         * @param interimResults true if TODO
         * @return the {@link Builder} with interimResults set.
         */
        public Builder setInterimResults(Boolean interimResults) {
            this.interimResults = interimResults == null ? DEFAULT_INTERIM_RESULTS : interimResults ? 1 : 0;
            return this;
        }

        /**
         * Update settings according to given settings config.
         *
         * @param update update settings
         * @return the {@link Builder} with applied updates.
         */
        public Builder update(SettingsConfig update) {
            // if explicit {@code null}s have been set in the update, we do not want to carry the default, but get rid
            // of the setting
            if (update.getDocsPerSecond() != null) {
                this.docsPerSecond = update.getDocsPerSecond().equals(DEFAULT_DOCS_PER_SECOND) ? null : update.getDocsPerSecond();
            }
            if (update.getMaxPageSearchSize() != null) {
                this.maxPageSearchSize = update.getMaxPageSearchSize().equals(DEFAULT_MAX_PAGE_SEARCH_SIZE)
                    ? null
                    : update.getMaxPageSearchSize();
            }
            if (update.getDatesAsEpochMillisForUpdate() != null) {
                this.datesAsEpochMillis = update.getDatesAsEpochMillisForUpdate().equals(DEFAULT_DATES_AS_EPOCH_MILLIS)
                    ? null
                    : update.getDatesAsEpochMillisForUpdate();
            }
            if (update.getInterimResultsForUpdate() != null)  {
                this.interimResults = update.getInterimResultsForUpdate().equals(DEFAULT_INTERIM_RESULTS)
                    ? null
                    : update.getInterimResultsForUpdate();
            }

            return this;
        }

        public SettingsConfig build() {
            return new SettingsConfig(maxPageSearchSize, docsPerSecond, datesAsEpochMillis, interimResults);
        }
    }
}
