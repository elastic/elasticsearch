/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which deletes the index.
 */
public class RolloverAction implements LifecycleAction {
    public static final String NAME = "rollover";
    public static final String INDEXING_COMPLETE_STEP_NAME = "set-indexing-complete";
    public static final ParseField MAX_SIZE_FIELD = new ParseField("max_size");
    public static final ParseField MAX_DOCS_FIELD = new ParseField("max_docs");
    public static final ParseField MAX_AGE_FIELD = new ParseField("max_age");
    public static final String LIFECYCLE_ROLLOVER_ALIAS = "index.lifecycle.rollover_alias";
    public static final Setting<String> LIFECYCLE_ROLLOVER_ALIAS_SETTING = Setting.simpleString(LIFECYCLE_ROLLOVER_ALIAS,
        Setting.Property.Dynamic, Setting.Property.IndexScope);

    private static final ConstructingObjectParser<RolloverAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new RolloverAction((ByteSizeValue) a[0], (TimeValue) a[1], (Long) a[2]));
    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SIZE_FIELD.getPreferredName()), MAX_SIZE_FIELD, ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_AGE_FIELD.getPreferredName()), MAX_AGE_FIELD, ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MAX_DOCS_FIELD);
    }

    private final ByteSizeValue maxSize;
    private final Long maxDocs;
    private final TimeValue maxAge;

    public static RolloverAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RolloverAction(@Nullable ByteSizeValue maxSize, @Nullable TimeValue maxAge, @Nullable Long maxDocs) {
        if (maxSize == null && maxAge == null && maxDocs == null) {
            throw new IllegalArgumentException("At least one rollover condition must be set.");
        }
        this.maxSize = maxSize;
        this.maxAge = maxAge;
        this.maxDocs = maxDocs;
    }

    public RolloverAction(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            maxSize = new ByteSizeValue(in);
        } else {
            maxSize = null;
        }
        maxAge = in.readOptionalTimeValue();
        if (in.readBoolean()) {
            maxDocs = in.readVLong();
        } else {
            maxDocs = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        boolean hasMaxSize = maxSize != null;
        out.writeBoolean(hasMaxSize);
        if (hasMaxSize) {
            maxSize.writeTo(out);
        }
        out.writeOptionalTimeValue(maxAge);
        boolean hasMaxDocs = maxDocs != null;
        out.writeBoolean(hasMaxDocs);
        if (hasMaxDocs) {
            out.writeVLong(maxDocs);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public ByteSizeValue getMaxSize() {
        return maxSize;
    }

    public TimeValue getMaxAge() {
        return maxAge;
    }

    public Long getMaxDocs() {
        return maxDocs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (maxSize != null) {
            builder.field(MAX_SIZE_FIELD.getPreferredName(), maxSize.getStringRep());
        }
        if (maxAge != null) {
            builder.field(MAX_AGE_FIELD.getPreferredName(), maxAge.getStringRep());
        }
        if (maxDocs != null) {
            builder.field(MAX_DOCS_FIELD.getPreferredName(), maxDocs);
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
        Settings indexingComplete = Settings.builder().put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true).build();

        StepKey waitForRolloverReadyStepKey = new StepKey(phase, NAME, WaitForRolloverReadyStep.NAME);
        StepKey rolloverStepKey = new StepKey(phase, NAME, RolloverStep.NAME);
        StepKey updateDateStepKey = new StepKey(phase, NAME, UpdateRolloverLifecycleDateStep.NAME);
        StepKey setIndexingCompleteStepKey = new StepKey(phase, NAME, INDEXING_COMPLETE_STEP_NAME);

        WaitForRolloverReadyStep waitForRolloverReadyStep = new WaitForRolloverReadyStep(waitForRolloverReadyStepKey, rolloverStepKey,
            client, maxSize, maxAge, maxDocs);
        RolloverStep rolloverStep = new RolloverStep(rolloverStepKey, updateDateStepKey, client);
        UpdateRolloverLifecycleDateStep updateDateStep = new UpdateRolloverLifecycleDateStep(updateDateStepKey, setIndexingCompleteStepKey,
            System::currentTimeMillis);
        UpdateSettingsStep setIndexingCompleteStep = new UpdateSettingsStep(setIndexingCompleteStepKey, nextStepKey,
            client, indexingComplete);
        return Arrays.asList(waitForRolloverReadyStep, rolloverStep, updateDateStep, setIndexingCompleteStep);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxSize, maxAge, maxDocs);
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
        return Objects.equals(maxSize, other.maxSize) &&
                Objects.equals(maxAge, other.maxAge) &&
                Objects.equals(maxDocs, other.maxDocs);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
