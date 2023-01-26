/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which rolls over the index.
 */
public class RolloverAction implements LifecycleAction {
    public static final String NAME = "rollover";
    public static final String INDEXING_COMPLETE_STEP_NAME = "set-indexing-complete";
    public static final ParseField MAX_SIZE_FIELD = new ParseField("max_size");
    public static final ParseField MAX_PRIMARY_SHARD_SIZE_FIELD = new ParseField("max_primary_shard_size");
    public static final ParseField MAX_DOCS_FIELD = new ParseField("max_docs");
    public static final ParseField MAX_AGE_FIELD = new ParseField("max_age");
    public static final ParseField MAX_PRIMARY_SHARD_DOCS_FIELD = new ParseField("max_primary_shard_docs");
    public static final ParseField MIN_SIZE_FIELD = new ParseField("min_size");
    public static final ParseField MIN_PRIMARY_SHARD_SIZE_FIELD = new ParseField("min_primary_shard_size");
    public static final ParseField MIN_DOCS_FIELD = new ParseField("min_docs");
    public static final ParseField MIN_AGE_FIELD = new ParseField("min_age");
    public static final ParseField MIN_PRIMARY_SHARD_DOCS_FIELD = new ParseField("min_primary_shard_docs");
    public static final String LIFECYCLE_ROLLOVER_ALIAS = "index.lifecycle.rollover_alias";
    public static final Setting<String> LIFECYCLE_ROLLOVER_ALIAS_SETTING = Setting.simpleString(
        LIFECYCLE_ROLLOVER_ALIAS,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    private static final Settings INDEXING_COMPLETE = Settings.builder().put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true).build();

    private static final ConstructingObjectParser<RolloverAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new RolloverAction(
            (ByteSizeValue) a[0],
            (ByteSizeValue) a[1],
            (TimeValue) a[2],
            (Long) a[3],
            (Long) a[4],
            (ByteSizeValue) a[5],
            (ByteSizeValue) a[6],
            (TimeValue) a[7],
            (Long) a[8],
            (Long) a[9]
        )
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SIZE_FIELD.getPreferredName()),
            MAX_SIZE_FIELD,
            ValueType.VALUE
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName()),
            MAX_PRIMARY_SHARD_SIZE_FIELD,
            ValueType.VALUE
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_AGE_FIELD.getPreferredName()),
            MAX_AGE_FIELD,
            ValueType.VALUE
        );
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MAX_DOCS_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MAX_PRIMARY_SHARD_DOCS_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MIN_SIZE_FIELD.getPreferredName()),
            MIN_SIZE_FIELD,
            ValueType.VALUE
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MIN_PRIMARY_SHARD_SIZE_FIELD.getPreferredName()),
            MIN_PRIMARY_SHARD_SIZE_FIELD,
            ValueType.VALUE
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), MIN_AGE_FIELD.getPreferredName()),
            MIN_AGE_FIELD,
            ValueType.VALUE
        );
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MIN_DOCS_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MIN_PRIMARY_SHARD_DOCS_FIELD);
    }

    private final ByteSizeValue maxSize;
    private final ByteSizeValue maxPrimaryShardSize;
    private final Long maxDocs;
    private final TimeValue maxAge;
    private final Long maxPrimaryShardDocs;
    private final ByteSizeValue minSize;
    private final ByteSizeValue minPrimaryShardSize;
    private final Long minDocs;
    private final TimeValue minAge;
    private final Long minPrimaryShardDocs;

    public static RolloverAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RolloverAction(
        @Nullable ByteSizeValue maxSize,
        @Nullable ByteSizeValue maxPrimaryShardSize,
        @Nullable TimeValue maxAge,
        @Nullable Long maxDocs,
        @Nullable Long maxPrimaryShardDocs,
        @Nullable ByteSizeValue minSize,
        @Nullable ByteSizeValue minPrimaryShardSize,
        @Nullable TimeValue minAge,
        @Nullable Long minDocs,
        @Nullable Long minPrimaryShardDocs
    ) {
        if (maxSize == null && maxPrimaryShardSize == null && maxAge == null && maxDocs == null && maxPrimaryShardDocs == null) {
            throw new IllegalArgumentException("At least one max_* rollover condition must be set.");
        }

        this.maxSize = maxSize;
        this.maxPrimaryShardSize = maxPrimaryShardSize;
        this.maxAge = maxAge;
        this.maxDocs = maxDocs;
        this.maxPrimaryShardDocs = maxPrimaryShardDocs;

        this.minSize = minSize;
        this.minPrimaryShardSize = minPrimaryShardSize;
        this.minAge = minAge;
        this.minDocs = minDocs;
        this.minPrimaryShardDocs = minPrimaryShardDocs;
    }

    public RolloverAction(StreamInput in) throws IOException {
        maxSize = in.readOptionalWriteable(ByteSizeValue::readFrom);
        maxPrimaryShardSize = in.readOptionalWriteable(ByteSizeValue::readFrom);
        maxAge = in.readOptionalTimeValue();
        maxDocs = in.readOptionalVLong();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_2_0)) {
            maxPrimaryShardDocs = in.readOptionalVLong();
        } else {
            maxPrimaryShardDocs = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            minSize = in.readOptionalWriteable(ByteSizeValue::readFrom);
            minPrimaryShardSize = in.readOptionalWriteable(ByteSizeValue::readFrom);
            minAge = in.readOptionalTimeValue();
            minDocs = in.readOptionalVLong();
            minPrimaryShardDocs = in.readOptionalVLong();
        } else {
            minSize = null;
            minPrimaryShardSize = null;
            minAge = null;
            minDocs = null;
            minPrimaryShardDocs = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(maxSize);
        out.writeOptionalWriteable(maxPrimaryShardSize);
        out.writeOptionalTimeValue(maxAge);
        out.writeOptionalVLong(maxDocs);
        if (out.getVersion().onOrAfter(Version.V_8_2_0)) {
            out.writeOptionalVLong(maxPrimaryShardDocs);
        }
        if (out.getVersion().onOrAfter(Version.V_8_4_0)) {
            out.writeOptionalWriteable(minSize);
            out.writeOptionalWriteable(minPrimaryShardSize);
            out.writeOptionalTimeValue(minAge);
            out.writeOptionalVLong(minDocs);
            out.writeOptionalVLong(minPrimaryShardDocs);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public ByteSizeValue getMaxSize() {
        return maxSize;
    }

    public ByteSizeValue getMaxPrimaryShardSize() {
        return maxPrimaryShardSize;
    }

    public TimeValue getMaxAge() {
        return maxAge;
    }

    public Long getMaxDocs() {
        return maxDocs;
    }

    public Long getMaxPrimaryShardDocs() {
        return maxPrimaryShardDocs;
    }

    public ByteSizeValue getMinSize() {
        return minSize;
    }

    public ByteSizeValue getMinPrimaryShardSize() {
        return minPrimaryShardSize;
    }

    public TimeValue getMinAge() {
        return minAge;
    }

    public Long getMinDocs() {
        return minDocs;
    }

    public Long getMinPrimaryShardDocs() {
        return minPrimaryShardDocs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (maxSize != null) {
            builder.field(MAX_SIZE_FIELD.getPreferredName(), maxSize.getStringRep());
        }
        if (maxPrimaryShardSize != null) {
            builder.field(MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName(), maxPrimaryShardSize.getStringRep());
        }
        if (maxAge != null) {
            builder.field(MAX_AGE_FIELD.getPreferredName(), maxAge.getStringRep());
        }
        if (maxDocs != null) {
            builder.field(MAX_DOCS_FIELD.getPreferredName(), maxDocs);
        }
        if (maxPrimaryShardDocs != null) {
            builder.field(MAX_PRIMARY_SHARD_DOCS_FIELD.getPreferredName(), maxPrimaryShardDocs);
        }
        if (minSize != null) {
            builder.field(MIN_SIZE_FIELD.getPreferredName(), minSize.getStringRep());
        }
        if (minPrimaryShardSize != null) {
            builder.field(MIN_PRIMARY_SHARD_SIZE_FIELD.getPreferredName(), minPrimaryShardSize.getStringRep());
        }
        if (minAge != null) {
            builder.field(MIN_AGE_FIELD.getPreferredName(), minAge.getStringRep());
        }
        if (minDocs != null) {
            builder.field(MIN_DOCS_FIELD.getPreferredName(), minDocs);
        }
        if (minPrimaryShardDocs != null) {
            builder.field(MIN_PRIMARY_SHARD_DOCS_FIELD.getPreferredName(), minPrimaryShardDocs);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        StepKey waitForRolloverReadyStepKey = new StepKey(phase, NAME, WaitForRolloverReadyStep.NAME);
        StepKey rolloverStepKey = new StepKey(phase, NAME, RolloverStep.NAME);
        StepKey waitForActiveShardsKey = new StepKey(phase, NAME, WaitForActiveShardsStep.NAME);
        StepKey updateDateStepKey = new StepKey(phase, NAME, UpdateRolloverLifecycleDateStep.NAME);
        StepKey setIndexingCompleteStepKey = new StepKey(phase, NAME, INDEXING_COMPLETE_STEP_NAME);

        WaitForRolloverReadyStep waitForRolloverReadyStep = new WaitForRolloverReadyStep(
            waitForRolloverReadyStepKey,
            rolloverStepKey,
            client,
            maxSize,
            maxPrimaryShardSize,
            maxAge,
            maxDocs,
            maxPrimaryShardDocs,
            minSize,
            minPrimaryShardSize,
            minAge,
            minDocs,
            minPrimaryShardDocs
        );
        RolloverStep rolloverStep = new RolloverStep(rolloverStepKey, waitForActiveShardsKey, client);
        WaitForActiveShardsStep waitForActiveShardsStep = new WaitForActiveShardsStep(waitForActiveShardsKey, updateDateStepKey);
        UpdateRolloverLifecycleDateStep updateDateStep = new UpdateRolloverLifecycleDateStep(
            updateDateStepKey,
            setIndexingCompleteStepKey,
            System::currentTimeMillis
        );
        UpdateSettingsStep setIndexingCompleteStep = new UpdateSettingsStep(
            setIndexingCompleteStepKey,
            nextStepKey,
            client,
            INDEXING_COMPLETE
        );
        return Arrays.asList(waitForRolloverReadyStep, rolloverStep, waitForActiveShardsStep, updateDateStep, setIndexingCompleteStep);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            maxSize,
            maxPrimaryShardSize,
            maxAge,
            maxDocs,
            maxPrimaryShardDocs,
            minSize,
            minPrimaryShardSize,
            minAge,
            minDocs,
            minPrimaryShardDocs
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RolloverAction other = (RolloverAction) obj;
        return Objects.equals(maxSize, other.maxSize)
            && Objects.equals(maxPrimaryShardSize, other.maxPrimaryShardSize)
            && Objects.equals(maxAge, other.maxAge)
            && Objects.equals(maxDocs, other.maxDocs)
            && Objects.equals(maxPrimaryShardDocs, other.maxPrimaryShardDocs)
            && Objects.equals(minSize, other.minSize)
            && Objects.equals(minPrimaryShardSize, other.minPrimaryShardSize)
            && Objects.equals(minAge, other.minAge)
            && Objects.equals(minDocs, other.minDocs)
            && Objects.equals(minPrimaryShardDocs, other.minPrimaryShardDocs);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
