/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Describes the state of an index with respect to the data stream lifecycle's DLM frozen tier transition:
 * whether it is old enough to be frozen, whether it has been marked for the transition, and the transition's
 * current execution status.
 * <p>
 * {@code eligible} and {@code markedForTransition} are derived from durable cluster state and are consistent
 * regardless of which node answers the request. {@code status}, by contrast, is best-effort: see {@link Status}.
 */
public record ExplainIndexFrozenTransition(boolean eligible, boolean markedForTransition, Status status)
    implements
        Writeable,
        ToXContentObject {

    public static final ParseField ELIGIBLE_FIELD = new ParseField("eligible");
    public static final ParseField MARKED_FOR_TRANSITION_FIELD = new ParseField("marked_for_transition");
    public static final ParseField STATUS_FIELD = new ParseField("status");

    public ExplainIndexFrozenTransition(StreamInput in) throws IOException {
        this(in.readBoolean(), in.readBoolean(), in.readEnum(Status.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(eligible);
        out.writeBoolean(markedForTransition);
        out.writeEnum(status);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ELIGIBLE_FIELD.getPreferredName(), eligible);
        builder.field(MARKED_FOR_TRANSITION_FIELD.getPreferredName(), markedForTransition);
        builder.field(STATUS_FIELD.getPreferredName(), status.toString());
        builder.endObject();
        return builder;
    }

    /**
     * The execution status of an index's DLM frozen tier transition, as tracked by the transition executor.
     * <p>
     * This is best-effort: it reflects only the in-process state of whichever node is currently the elected
     * master, and resets to {@link #NOT_STARTED} across a master failover, even for an index whose transition
     * was genuinely in progress on the previous master.
     */
    public enum Status {
        NOT_STARTED,
        QUEUED,
        RUNNING;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
