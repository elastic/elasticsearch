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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which deletes the index.
 */
public class DeleteAction implements LifecycleAction {
    public static final String NAME = "delete";

    public static final ParseField DELETE_SEARCHABLE_SNAPSHOT_FIELD = new ParseField("delete_searchable_snapshot");

    public static final DeleteAction WITH_SNAPSHOT_DELETE = new DeleteAction(true);
    public static final DeleteAction NO_SNAPSHOT_DELETE = new DeleteAction(false);

    private static final ConstructingObjectParser<DeleteAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> (a[0] == null || (boolean) a[0]) ? WITH_SNAPSHOT_DELETE : NO_SNAPSHOT_DELETE
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DELETE_SEARCHABLE_SNAPSHOT_FIELD);
    }

    public static DeleteAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final boolean deleteSearchableSnapshot;

    private DeleteAction(boolean deleteSearchableSnapshot) {
        this.deleteSearchableSnapshot = deleteSearchableSnapshot;
    }

    public static DeleteAction readFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? WITH_SNAPSHOT_DELETE : NO_SNAPSHOT_DELETE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(deleteSearchableSnapshot);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DELETE_SEARCHABLE_SNAPSHOT_FIELD.getPreferredName(), deleteSearchableSnapshot);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        Step.StepKey waitForNoFollowerStepKey = new Step.StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        Step.StepKey waitTimeSeriesEndTimePassesKey = new Step.StepKey(phase, NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);
        Step.StepKey deleteStepKey = new Step.StepKey(phase, NAME, DeleteStep.NAME);
        Step.StepKey cleanSnapshotKey = new Step.StepKey(phase, NAME, CleanupSnapshotStep.NAME);

        if (deleteSearchableSnapshot) {
            WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(
                waitForNoFollowerStepKey,
                waitTimeSeriesEndTimePassesKey,
                client
            );
            WaitUntilTimeSeriesEndTimePassesStep waitUntilTimeSeriesEndTimeStep = new WaitUntilTimeSeriesEndTimePassesStep(
                waitTimeSeriesEndTimePassesKey,
                cleanSnapshotKey,
                Instant::now
            );
            CleanupSnapshotStep cleanupSnapshotStep = new CleanupSnapshotStep(cleanSnapshotKey, deleteStepKey, client);
            DeleteStep deleteStep = new DeleteStep(deleteStepKey, nextStepKey, client);
            return List.of(waitForNoFollowersStep, waitUntilTimeSeriesEndTimeStep, cleanupSnapshotStep, deleteStep);
        } else {
            WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(
                waitForNoFollowerStepKey,
                waitTimeSeriesEndTimePassesKey,
                client
            );
            WaitUntilTimeSeriesEndTimePassesStep waitUntilTimeSeriesEndTimeStep = new WaitUntilTimeSeriesEndTimePassesStep(
                waitTimeSeriesEndTimePassesKey,
                deleteStepKey,
                Instant::now
            );
            DeleteStep deleteStep = new DeleteStep(deleteStepKey, nextStepKey, client);
            return List.of(waitForNoFollowersStep, waitUntilTimeSeriesEndTimeStep, deleteStep);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(deleteSearchableSnapshot);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DeleteAction that = (DeleteAction) obj;
        return deleteSearchableSnapshot == that.deleteSearchableSnapshot;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
