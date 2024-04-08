/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.LAZY_ROLLOVER_ADDED;

/**
 * Response object for {@link RolloverRequest} API
 *
 * Note: there is a new class with the same name for the Java HLRC that uses a typeless format.
 * Any changes done to this class should also go to that client class.
 */
public final class RolloverResponse extends ShardsAcknowledgedResponse implements ToXContentObject {

    private static final ParseField NEW_INDEX = new ParseField("new_index");
    private static final ParseField OLD_INDEX = new ParseField("old_index");
    private static final ParseField DRY_RUN = new ParseField("dry_run");
    private static final ParseField ROLLED_OVER = new ParseField("rolled_over");
    private static final ParseField LAZY = new ParseField("lazy");
    private static final ParseField CONDITIONS = new ParseField("conditions");

    private final String oldIndex;
    private final String newIndex;
    private final Map<String, Boolean> conditionStatus;
    private final boolean dryRun;
    private final boolean rolledOver;
    // Needs to be duplicated, because shardsAcknowledged gets (de)serialized as last field whereas
    // in other subclasses of ShardsAcknowledgedResponse this field (de)serialized as first field.
    private final boolean shardsAcknowledged;
    private final boolean lazy;

    RolloverResponse(StreamInput in) throws IOException {
        super(in, false);
        oldIndex = in.readString();
        newIndex = in.readString();
        int conditionSize = in.readVInt();
        conditionStatus = Maps.newMapWithExpectedSize(conditionSize);
        for (int i = 0; i < conditionSize; i++) {
            conditionStatus.put(in.readString(), in.readBoolean());
        }
        dryRun = in.readBoolean();
        rolledOver = in.readBoolean();
        shardsAcknowledged = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(LAZY_ROLLOVER_ADDED)) {
            lazy = in.readBoolean();
        } else {
            lazy = false;
        }
    }

    public RolloverResponse(
        String oldIndex,
        String newIndex,
        Map<String, Boolean> conditionResults,
        boolean dryRun,
        boolean rolledOver,
        boolean acknowledged,
        boolean shardsAcknowledged,
        boolean lazy
    ) {
        super(acknowledged, shardsAcknowledged);
        this.oldIndex = oldIndex;
        this.newIndex = newIndex;
        this.dryRun = dryRun;
        this.rolledOver = rolledOver;
        this.conditionStatus = conditionResults;
        this.shardsAcknowledged = shardsAcknowledged;
        this.lazy = lazy;
    }

    /**
     * Returns the name of the index that the request alias was pointing to
     */
    public String getOldIndex() {
        return oldIndex;
    }

    /**
     * Returns the name of the index that the request alias currently points to
     */
    public String getNewIndex() {
        return newIndex;
    }

    /**
     * Returns the statuses of all the request conditions
     */
    public Map<String, Boolean> getConditionStatus() {
        return conditionStatus;
    }

    /**
     * Returns if the rollover execution was skipped even when conditions were met
     */
    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * Returns true if the rollover was not simulated and the conditions were met
     */
    public boolean isRolledOver() {
        return rolledOver;
    }

    @Override
    public boolean isShardsAcknowledged() {
        return shardsAcknowledged;
    }

    /**
     * Returns true if the rollover has been lazily applied, meaning the target will rollover when the next document will get indexed.
     */
    public boolean isLazy() {
        return lazy;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(oldIndex);
        out.writeString(newIndex);
        out.writeMap(conditionStatus, StreamOutput::writeBoolean);
        out.writeBoolean(dryRun);
        out.writeBoolean(rolledOver);
        out.writeBoolean(shardsAcknowledged);
        if (out.getTransportVersion().onOrAfter(LAZY_ROLLOVER_ADDED)) {
            out.writeBoolean(lazy);
        }
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.field(OLD_INDEX.getPreferredName(), oldIndex);
        builder.field(NEW_INDEX.getPreferredName(), newIndex);
        builder.field(ROLLED_OVER.getPreferredName(), rolledOver);
        builder.field(DRY_RUN.getPreferredName(), dryRun);
        builder.field(LAZY.getPreferredName(), lazy);
        builder.startObject(CONDITIONS.getPreferredName());
        for (Map.Entry<String, Boolean> entry : conditionStatus.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            RolloverResponse that = (RolloverResponse) o;
            return dryRun == that.dryRun
                && rolledOver == that.rolledOver
                && lazy == that.lazy
                && Objects.equals(oldIndex, that.oldIndex)
                && Objects.equals(newIndex, that.newIndex)
                && Objects.equals(conditionStatus, that.conditionStatus);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), oldIndex, newIndex, conditionStatus, dryRun, rolledOver, lazy);
    }
}
