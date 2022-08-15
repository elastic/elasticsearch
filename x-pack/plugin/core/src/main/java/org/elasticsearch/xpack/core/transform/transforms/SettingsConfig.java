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
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SettingsConfig implements Writeable, ToXContentObject {
    public static final ConstructingObjectParser<SettingsConfig, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<SettingsConfig, Void> LENIENT_PARSER = createParser(true);

    public static final int MAX_NUM_FAILURE_RETRIES = 100;

    private static final int DEFAULT_MAX_PAGE_SEARCH_SIZE = -1;
    private static final float DEFAULT_DOCS_PER_SECOND = -1F;
    private static final int DEFAULT_DATES_AS_EPOCH_MILLIS = -1;
    private static final int DEFAULT_ALIGN_CHECKPOINTS = -1;
    private static final int DEFAULT_USE_PIT = -1;
    private static final int DEFAULT_DEDUCE_MAPPINGS = -1;
    private static final int DEFAULT_NUM_FAILURE_RETRIES = -2;
    private static final int DEFAULT_UNATTENDED = -1;

    private static ConstructingObjectParser<SettingsConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<SettingsConfig, Void> parser = new ConstructingObjectParser<>(
            "transform_config_settings",
            lenient,
            args -> new SettingsConfig(
                (Integer) args[0],
                (Float) args[1],
                (Integer) args[2],
                (Integer) args[3],
                (Integer) args[4],
                (Integer) args[5],
                (Integer) args[6],
                (Integer) args[7]
            )
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
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_ALIGN_CHECKPOINTS : p.booleanValue() ? 1 : 0,
            TransformField.ALIGN_CHECKPOINTS,
            ValueType.BOOLEAN_OR_NULL
        );
        // this boolean requires 4 possible values: true, false, not_specified, default, therefore using a custom parser
        parser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_USE_PIT : p.booleanValue() ? 1 : 0,
            TransformField.USE_PIT,
            ValueType.BOOLEAN_OR_NULL
        );
        // this boolean requires 4 possible values: true, false, not_specified, default, therefore using a custom parser
        parser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_DEDUCE_MAPPINGS : p.booleanValue() ? 1 : 0,
            TransformField.DEDUCE_MAPPINGS,
            ValueType.BOOLEAN_OR_NULL
        );
        parser.declareIntOrNull(optionalConstructorArg(), DEFAULT_NUM_FAILURE_RETRIES, TransformField.NUM_FAILURE_RETRIES);
        // this boolean requires 4 possible values: true, false, not_specified, default, therefore using a custom parser
        parser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_UNATTENDED : p.booleanValue() ? 1 : 0,
            TransformField.UNATTENDED,
            ValueType.BOOLEAN_OR_NULL
        );
        return parser;
    }

    private final Integer maxPageSearchSize;
    private final Float docsPerSecond;
    private final Integer datesAsEpochMillis;
    private final Integer alignCheckpoints;
    private final Integer usePit;
    private final Integer deduceMappings;
    private final Integer numFailureRetries;
    private final Integer unattended;

    public SettingsConfig() {
        this(null, null, (Integer) null, (Integer) null, (Integer) null, (Integer) null, (Integer) null, (Integer) null);
    }

    public SettingsConfig(
        Integer maxPageSearchSize,
        Float docsPerSecond,
        Boolean datesAsEpochMillis,
        Boolean alignCheckpoints,
        Boolean usePit,
        Boolean deduceMappings,
        Integer numFailureRetries,
        Boolean unattended
    ) {
        this(
            maxPageSearchSize,
            docsPerSecond,
            datesAsEpochMillis == null ? null : datesAsEpochMillis ? 1 : 0,
            alignCheckpoints == null ? null : alignCheckpoints ? 1 : 0,
            usePit == null ? null : usePit ? 1 : 0,
            deduceMappings == null ? null : deduceMappings ? 1 : 0,
            numFailureRetries,
            unattended == null ? null : unattended ? 1 : 0
        );
    }

    SettingsConfig(
        Integer maxPageSearchSize,
        Float docsPerSecond,
        Integer datesAsEpochMillis,
        Integer alignCheckpoints,
        Integer usePit,
        Integer deduceMappings,
        Integer numFailureRetries,
        Integer unattended
    ) {
        this.maxPageSearchSize = maxPageSearchSize;
        this.docsPerSecond = docsPerSecond;
        this.datesAsEpochMillis = datesAsEpochMillis;
        this.alignCheckpoints = alignCheckpoints;
        this.usePit = usePit;
        this.deduceMappings = deduceMappings;
        this.numFailureRetries = numFailureRetries;
        this.unattended = unattended;
    }

    public SettingsConfig(final StreamInput in) throws IOException {
        this.maxPageSearchSize = in.readOptionalInt();
        this.docsPerSecond = in.readOptionalFloat();
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            this.datesAsEpochMillis = in.readOptionalInt();
        } else {
            this.datesAsEpochMillis = DEFAULT_DATES_AS_EPOCH_MILLIS;
        }
        if (in.getVersion().onOrAfter(Version.V_7_15_0)) {
            this.alignCheckpoints = in.readOptionalInt();
        } else {
            this.alignCheckpoints = DEFAULT_ALIGN_CHECKPOINTS;
        }
        if (in.getVersion().onOrAfter(Version.V_7_16_1)) {
            this.usePit = in.readOptionalInt();
        } else {
            this.usePit = DEFAULT_USE_PIT;
        }
        if (in.getVersion().onOrAfter(Version.V_8_1_0)) {
            deduceMappings = in.readOptionalInt();
        } else {
            deduceMappings = DEFAULT_DEDUCE_MAPPINGS;
        }
        if (in.getVersion().onOrAfter(Version.V_8_4_0)) {
            numFailureRetries = in.readOptionalInt();
        } else {
            numFailureRetries = null;
        }
        if (in.getVersion().onOrAfter(Version.V_8_5_0)) {
            unattended = in.readOptionalInt();
        } else {
            unattended = DEFAULT_UNATTENDED;
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

    public Boolean getAlignCheckpoints() {
        return alignCheckpoints != null ? (alignCheckpoints > 0) || (alignCheckpoints == DEFAULT_ALIGN_CHECKPOINTS) : null;
    }

    public Integer getAlignCheckpointsForUpdate() {
        return alignCheckpoints;
    }

    public Boolean getUsePit() {
        return usePit != null ? (usePit > 0) || (usePit == DEFAULT_USE_PIT) : null;
    }

    public Integer getUsePitForUpdate() {
        return usePit;
    }

    public Boolean getDeduceMappings() {
        return deduceMappings != null ? (deduceMappings > 0) || (deduceMappings == DEFAULT_DEDUCE_MAPPINGS) : null;
    }

    public Integer getDeduceMappingsForUpdate() {
        return deduceMappings;
    }

    public Integer getNumFailureRetries() {
        return numFailureRetries != null ? (numFailureRetries == DEFAULT_NUM_FAILURE_RETRIES ? null : numFailureRetries) : null;
    }

    public Integer getNumFailureRetriesForUpdate() {
        return numFailureRetries;
    }

    public Boolean getUnattended() {
        return unattended != null ? (unattended == DEFAULT_UNATTENDED) ? null : (unattended > 0) : null;
    }

    public Integer getUnattendedForUpdate() {
        return unattended;
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
        if (numFailureRetries != null && (numFailureRetries < -1 || numFailureRetries > MAX_NUM_FAILURE_RETRIES)) {
            validationException = addValidationError(
                "settings.num_failure_retries ["
                    + numFailureRetries
                    + "] is out of range. The minimum value is -1 (infinity) and the maximum is "
                    + MAX_NUM_FAILURE_RETRIES,
                validationException
            );
        }

        // disallow setting unattended to true with explicit num failure retries
        if (unattended != null && unattended == 1 && numFailureRetries != null && numFailureRetries > 0) {
            validationException = addValidationError(
                "settings.num_failure_retries ["
                    + numFailureRetries
                    + "] can not be set in unattended mode, unattended retries indefinitely",
                validationException
            );
        }

        return validationException;
    }

    public void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation) {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(maxPageSearchSize);
        out.writeOptionalFloat(docsPerSecond);
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            out.writeOptionalInt(datesAsEpochMillis);
        }
        if (out.getVersion().onOrAfter(Version.V_7_15_0)) {
            out.writeOptionalInt(alignCheckpoints);
        }
        if (out.getVersion().onOrAfter(Version.V_7_16_1)) {
            out.writeOptionalInt(usePit);
        }
        if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
            out.writeOptionalInt(deduceMappings);
        }
        if (out.getVersion().onOrAfter(Version.V_8_4_0)) {
            out.writeOptionalInt(numFailureRetries);
        }
        if (out.getVersion().onOrAfter(Version.V_8_5_0)) {
            out.writeOptionalInt(unattended);
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
        if (alignCheckpoints != null && (alignCheckpoints.equals(DEFAULT_ALIGN_CHECKPOINTS) == false)) {
            builder.field(TransformField.ALIGN_CHECKPOINTS.getPreferredName(), alignCheckpoints > 0 ? true : false);
        }
        if (usePit != null && (usePit.equals(DEFAULT_USE_PIT) == false)) {
            builder.field(TransformField.USE_PIT.getPreferredName(), usePit > 0 ? true : false);
        }
        if (deduceMappings != null && (deduceMappings.equals(DEFAULT_DEDUCE_MAPPINGS) == false)) {
            builder.field(TransformField.DEDUCE_MAPPINGS.getPreferredName(), deduceMappings > 0 ? true : false);
        }
        if (numFailureRetries != null && (numFailureRetries.equals(DEFAULT_NUM_FAILURE_RETRIES) == false)) {
            builder.field(TransformField.NUM_FAILURE_RETRIES.getPreferredName(), numFailureRetries);
        }
        if (unattended != null && (unattended.equals(DEFAULT_UNATTENDED) == false)) {
            builder.field(TransformField.UNATTENDED.getPreferredName(), unattended > 0 ? true : false);
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
            && Objects.equals(alignCheckpoints, that.alignCheckpoints)
            && Objects.equals(usePit, that.usePit)
            && Objects.equals(deduceMappings, that.deduceMappings)
            && Objects.equals(numFailureRetries, that.numFailureRetries)
            && Objects.equals(unattended, that.unattended);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            maxPageSearchSize,
            docsPerSecond,
            datesAsEpochMillis,
            alignCheckpoints,
            usePit,
            deduceMappings,
            numFailureRetries,
            unattended
        );
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
        private Integer alignCheckpoints;
        private Integer usePit;
        private Integer deduceMappings;
        private Integer numFailureRetries;
        private Integer unattended;

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
            this.alignCheckpoints = base.alignCheckpoints;
            this.usePit = base.usePit;
            this.deduceMappings = base.deduceMappings;
            this.numFailureRetries = base.numFailureRetries;
            this.unattended = base.unattended;
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
         * Point in time is a more resource friendly way to query. It is used per default. In case of problems
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

        public Builder setNumFailureRetries(Integer numFailureRetries) {
            this.numFailureRetries = numFailureRetries == null ? DEFAULT_NUM_FAILURE_RETRIES : numFailureRetries;
            return this;
        }

        /**
         * Whether to run the transform in unattended mode.
         * In unattended mode the transform does not immediately fail for errors that are classified
         * as irrecoverable.
         *
         * An explicit `null` resets to default.
         *
         * @param unattended true if this is a unattended transform.
         * @return the {@link Builder} with usePit set.
         */
        public Builder setUnattended(Boolean unattended) {
            this.unattended = unattended == null ? DEFAULT_UNATTENDED : unattended ? 1 : 0;
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
            if (update.getAlignCheckpointsForUpdate() != null) {
                this.alignCheckpoints = update.getAlignCheckpointsForUpdate().equals(DEFAULT_ALIGN_CHECKPOINTS)
                    ? null
                    : update.getAlignCheckpointsForUpdate();
            }
            if (update.getUsePitForUpdate() != null) {
                this.usePit = update.getUsePitForUpdate().equals(DEFAULT_USE_PIT) ? null : update.getUsePitForUpdate();
            }
            if (update.getDeduceMappingsForUpdate() != null) {
                this.deduceMappings = update.getDeduceMappingsForUpdate().equals(DEFAULT_DEDUCE_MAPPINGS)
                    ? null
                    : update.getDeduceMappingsForUpdate();
            }
            if (update.getNumFailureRetriesForUpdate() != null) {
                this.numFailureRetries = update.getNumFailureRetriesForUpdate().equals(DEFAULT_NUM_FAILURE_RETRIES)
                    ? null
                    : update.getNumFailureRetriesForUpdate();
            }
            if (update.getUnattendedForUpdate() != null) {
                this.unattended = update.getUnattendedForUpdate().equals(DEFAULT_UNATTENDED) ? null : update.getUnattendedForUpdate();
            }

            return this;
        }

        public SettingsConfig build() {
            return new SettingsConfig(
                maxPageSearchSize,
                docsPerSecond,
                datesAsEpochMillis,
                alignCheckpoints,
                usePit,
                deduceMappings,
                numFailureRetries,
                unattended
            );
        }
    }
}
