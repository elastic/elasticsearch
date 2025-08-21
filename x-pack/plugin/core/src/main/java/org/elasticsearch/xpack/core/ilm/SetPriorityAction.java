/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which sets the index's priority. The higher the priority, the faster the recovery.
 */
public class SetPriorityAction implements LifecycleAction {
    public static final String NAME = "set_priority";
    public static final ParseField RECOVERY_PRIORITY_FIELD = new ParseField("priority");

    private static final ConstructingObjectParser<SetPriorityAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new SetPriorityAction((Integer) a[0])
    );

    private static final Settings NULL_PRIORITY_SETTINGS = Settings.builder()
        .putNull(IndexMetadata.INDEX_PRIORITY_SETTING.getKey())
        .build();

    // package private for testing
    final Integer recoveryPriority;

    static {
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.intValue(),
            RECOVERY_PRIORITY_FIELD,
            ObjectParser.ValueType.INT_OR_NULL
        );
    }

    public static SetPriorityAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public SetPriorityAction(@Nullable Integer recoveryPriority) {
        if (recoveryPriority != null && recoveryPriority < 0) {
            throw new IllegalArgumentException("[" + RECOVERY_PRIORITY_FIELD.getPreferredName() + "] must be 0 or greater");
        }
        this.recoveryPriority = recoveryPriority;
    }

    public SetPriorityAction(StreamInput in) throws IOException {
        this(in.readOptionalVInt());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public Integer getRecoveryPriority() {
        return recoveryPriority;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RECOVERY_PRIORITY_FIELD.getPreferredName(), recoveryPriority);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(recoveryPriority);
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey key = new StepKey(phase, NAME, NAME);
        Settings indexPriority = recoveryPriority == null
            ? NULL_PRIORITY_SETTINGS
            : Settings.builder().put(IndexMetadata.INDEX_PRIORITY_SETTING.getKey(), recoveryPriority).build();
        return List.of(new UpdateSettingsStep(key, nextStepKey, client, indexPriority));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SetPriorityAction that = (SetPriorityAction) o;
        return Objects.equals(recoveryPriority, that.recoveryPriority);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recoveryPriority);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
