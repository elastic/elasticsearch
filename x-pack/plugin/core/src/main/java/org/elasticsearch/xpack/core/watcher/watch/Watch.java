/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.watch;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.trigger.Trigger;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

public class Watch implements ToXContentObject {

    public static final String INCLUDE_STATUS_KEY = "include_status";
    public static final String INDEX = ".watches";

    private final String id;
    private final Trigger trigger;
    private final ExecutableInput<? extends Input, ? extends Input.Result> input;
    private final ExecutableCondition condition;
    @Nullable
    private final ExecutableTransform<? extends Transform, ? extends Transform.Result> transform;
    private final List<ActionWrapper> actions;
    @Nullable
    private final TimeValue throttlePeriod;
    @Nullable
    private final Map<String, Object> metadata;
    private final WatchStatus status;

    private final long sourceSeqNo;
    private final long sourcePrimaryTerm;

    public Watch(
        String id,
        Trigger trigger,
        ExecutableInput<? extends Input, ? extends Input.Result> input,
        ExecutableCondition condition,
        @Nullable ExecutableTransform<? extends Transform, ? extends Transform.Result> transform,
        @Nullable TimeValue throttlePeriod,
        List<ActionWrapper> actions,
        @Nullable Map<String, Object> metadata,
        WatchStatus status,
        long sourceSeqNo,
        long sourcePrimaryTerm
    ) {
        this.id = id;
        this.trigger = trigger;
        this.input = input;
        this.condition = condition;
        this.transform = transform;
        this.actions = actions;
        this.throttlePeriod = throttlePeriod;
        this.metadata = metadata;
        this.status = status;
        this.sourceSeqNo = sourceSeqNo;
        this.sourcePrimaryTerm = sourcePrimaryTerm;
    }

    public String id() {
        return id;
    }

    public Trigger trigger() {
        return trigger;
    }

    public ExecutableInput<? extends Input, ? extends Input.Result> input() {
        return input;
    }

    public ExecutableCondition condition() {
        return condition;
    }

    public ExecutableTransform<? extends Transform, ? extends Transform.Result> transform() {
        return transform;
    }

    public TimeValue throttlePeriod() {
        return throttlePeriod;
    }

    public List<ActionWrapper> actions() {
        return actions;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public WatchStatus status() {
        return status;
    }

    /**
     * The sequence number of the document that was used to create this watch, {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
     * if the watch wasn't read from a document
     ***/
    public long getSourceSeqNo() {
        return sourceSeqNo;
    }

    /**
     * The primary term of the document that was used to create this watch, {@link SequenceNumbers#UNASSIGNED_PRIMARY_TERM}
     * if the watch wasn't read from a document
     ***/
    public long getSourcePrimaryTerm() {
        return sourcePrimaryTerm;
    }

    /**
     * Sets the state of this watch to in/active
     *
     * @return  {@code true} if the status of this watch changed, {@code false} otherwise.
     */
    public boolean setState(boolean active, ZonedDateTime now) {
        return status.setActive(active, now);
    }

    /**
     * Acks this watch.
     *
     * @return  {@code true} if the status of this watch changed, {@code false} otherwise.
     */
    public boolean ack(ZonedDateTime now, String... actionIds) {
        return status.onAck(now, actionIds);
    }

    public boolean acked(String actionId) {
        ActionStatus actionStatus = status.actionStatus(actionId);
        return actionStatus.ackStatus().state() == ActionStatus.AckStatus.State.ACKED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Watch watch = (Watch) o;
        return watch.id.equals(id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(WatchField.TRIGGER.getPreferredName()).startObject().field(trigger.type(), trigger, params).endObject();
        builder.field(WatchField.INPUT.getPreferredName()).startObject().field(input.type(), input, params).endObject();
        builder.field(WatchField.CONDITION.getPreferredName()).startObject().field(condition.type(), condition, params).endObject();
        if (transform != null) {
            builder.field(WatchField.TRANSFORM.getPreferredName()).startObject().field(transform.type(), transform, params).endObject();
        }
        if (throttlePeriod != null) {
            builder.humanReadableField(
                WatchField.THROTTLE_PERIOD.getPreferredName(),
                WatchField.THROTTLE_PERIOD_HUMAN.getPreferredName(),
                throttlePeriod
            );
        }
        builder.startObject(WatchField.ACTIONS.getPreferredName());
        for (ActionWrapper action : actions) {
            builder.field(action.id(), action, params);
        }
        builder.endObject();
        if (metadata != null) {
            builder.field(WatchField.METADATA.getPreferredName(), metadata);
        }
        if (params.paramAsBoolean(INCLUDE_STATUS_KEY, false)) {
            builder.field(WatchField.STATUS.getPreferredName(), status, params);
        }
        builder.endObject();
        return builder;
    }

}
