/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A {@link LifecycleAction} which sets the index's priority. The higher the priority, the faster the recovery.
 */
public class SetPriorityAction implements LifecycleAction {
    public static final String NAME = "set_priority";
    private static final ParseField RECOVERY_PRIORITY_FIELD = new ParseField("priority");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SetPriorityAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> new SetPriorityAction((Integer) a[0]));

    //package private for testing
    final Integer recoveryPriority;

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.intValue()
            , RECOVERY_PRIORITY_FIELD, ObjectParser.ValueType.INT_OR_NULL);
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
        Settings indexPriority = recoveryPriority == null ?
            Settings.builder().putNull(IndexMetadata.INDEX_PRIORITY_SETTING.getKey()).build()
            : Settings.builder().put(IndexMetadata.INDEX_PRIORITY_SETTING.getKey(), recoveryPriority).build();
        return Collections.singletonList(new UpdateSettingsStep(key, nextStepKey, client, indexPriority));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SetPriorityAction that = (SetPriorityAction) o;

        return recoveryPriority != null ? recoveryPriority.equals(that.recoveryPriority) : that.recoveryPriority == null;
    }

    @Override
    public int hashCode() {
        return recoveryPriority != null ? recoveryPriority.hashCode() : 0;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
