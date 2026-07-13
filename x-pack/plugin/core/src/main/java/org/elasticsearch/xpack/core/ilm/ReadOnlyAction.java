/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * A {@link LifecycleAction} which sets the index to be read-only.
 */
public class ReadOnlyAction implements LifecycleAction {
    public static final String NAME = "readonly";

    private static final ObjectParser<ReadOnlyAction, Void> PARSER = new ObjectParser<>(NAME, false, ReadOnlyAction::new);

    public static final String INDEXING_COMPLETE_STEP_NAME = "set-indexing-complete";

    private static final Settings INDEXING_COMPLETE = Settings.builder().put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true).build();

    public static ReadOnlyAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ReadOnlyAction() {}

    public ReadOnlyAction(StreamInput in) {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey checkNotWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey waitTimeSeriesEndTimePassesKey = new StepKey(phase, NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, NAME);
        StepKey setIndexingCompleteStepKey = new StepKey(phase, NAME, INDEXING_COMPLETE_STEP_NAME);

        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(
            checkNotWriteIndex,
            waitTimeSeriesEndTimePassesKey
        );
        WaitUntilTimeSeriesEndTimePassesStep waitUntilTimeSeriesEndTimeStep = new WaitUntilTimeSeriesEndTimePassesStep(
            waitTimeSeriesEndTimePassesKey,
            readOnlyKey,
            Instant::now
        );
        ReadOnlyStep readOnlyStep = new ReadOnlyStep(readOnlyKey, setIndexingCompleteStepKey, client, true);

        UpdateSettingsStep setIndexingCompleteStep = new UpdateSettingsStep(
            setIndexingCompleteStepKey,
            nextStepKey,
            client,
            INDEXING_COMPLETE
        );

        return List.of(checkNotWriteIndexStep, waitUntilTimeSeriesEndTimeStep, readOnlyStep, setIndexingCompleteStep);
    }

    @Override
    public int hashCode() {
        return ReadOnlyAction.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
