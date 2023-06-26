/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Various statistics (timing information etc) about cluster state updates coordinated by this node.
 */
public class ClusterStateUpdateStats implements Writeable, ToXContentFragment {

    private final long unchangedTaskCount;
    private final long publicationSuccessCount;
    private final long publicationFailureCount;

    private final long unchangedComputationElapsedMillis;
    private final long unchangedNotificationElapsedMillis;
    private final long successfulComputationElapsedMillis;
    private final long successfulPublicationElapsedMillis;
    private final long successfulContextConstructionElapsedMillis;
    private final long successfulCommitElapsedMillis;
    private final long successfulCompletionElapsedMillis;
    private final long successfulMasterApplyElapsedMillis;
    private final long successfulNotificationElapsedMillis;

    private final long failedComputationElapsedMillis;
    private final long failedPublicationElapsedMillis;
    private final long failedContextConstructionElapsedMillis;
    private final long failedCommitElapsedMillis;
    private final long failedCompletionElapsedMillis;
    private final long failedMasterApplyElapsedMillis;
    private final long failedNotificationElapsedMillis;

    public ClusterStateUpdateStats(
        long unchangedTaskCount,
        long publicationSuccessCount,
        long publicationFailureCount,
        long unchangedComputationElapsedMillis,
        long unchangedNotificationElapsedMillis,
        long successfulComputationElapsedMillis,
        long successfulPublicationElapsedMillis,
        long successfulContextConstructionElapsedMillis,
        long successfulCommitElapsedMillis,
        long successfulCompletionElapsedMillis,
        long successfulMasterApplyElapsedMillis,
        long successfulNotificationElapsedMillis,
        long failedComputationElapsedMillis,
        long failedPublicationElapsedMillis,
        long failedContextConstructionElapsedMillis,
        long failedCommitElapsedMillis,
        long failedCompletionElapsedMillis,
        long failedMasterApplyElapsedMillis,
        long failedNotificationElapsedMillis
    ) {
        this.unchangedTaskCount = nonNegative(unchangedTaskCount);
        this.publicationSuccessCount = nonNegative(publicationSuccessCount);
        this.publicationFailureCount = nonNegative(publicationFailureCount);
        this.unchangedComputationElapsedMillis = nonNegative(unchangedComputationElapsedMillis);
        this.unchangedNotificationElapsedMillis = nonNegative(unchangedNotificationElapsedMillis);
        this.successfulComputationElapsedMillis = nonNegative(successfulComputationElapsedMillis);
        this.successfulPublicationElapsedMillis = nonNegative(successfulPublicationElapsedMillis);
        this.successfulContextConstructionElapsedMillis = nonNegative(successfulContextConstructionElapsedMillis);
        this.successfulCommitElapsedMillis = nonNegative(successfulCommitElapsedMillis);
        this.successfulCompletionElapsedMillis = nonNegative(successfulCompletionElapsedMillis);
        this.successfulMasterApplyElapsedMillis = nonNegative(successfulMasterApplyElapsedMillis);
        this.successfulNotificationElapsedMillis = nonNegative(successfulNotificationElapsedMillis);
        this.failedComputationElapsedMillis = nonNegative(failedComputationElapsedMillis);
        this.failedPublicationElapsedMillis = nonNegative(failedPublicationElapsedMillis);
        this.failedContextConstructionElapsedMillis = nonNegative(failedContextConstructionElapsedMillis);
        this.failedCommitElapsedMillis = nonNegative(failedCommitElapsedMillis);
        this.failedCompletionElapsedMillis = nonNegative(failedCompletionElapsedMillis);
        this.failedMasterApplyElapsedMillis = nonNegative(failedMasterApplyElapsedMillis);
        this.failedNotificationElapsedMillis = nonNegative(failedNotificationElapsedMillis);
    }

    private static long nonNegative(long v) {
        assert v >= 0 : v;
        return v;
    }

    public ClusterStateUpdateStats(StreamInput in) throws IOException {
        this.unchangedTaskCount = in.readVLong();
        this.publicationSuccessCount = in.readVLong();
        this.publicationFailureCount = in.readVLong();
        this.unchangedComputationElapsedMillis = in.readVLong();
        this.unchangedNotificationElapsedMillis = in.readVLong();
        this.successfulComputationElapsedMillis = in.readVLong();
        this.successfulPublicationElapsedMillis = in.readVLong();
        this.successfulContextConstructionElapsedMillis = in.readVLong();
        this.successfulCommitElapsedMillis = in.readVLong();
        this.successfulCompletionElapsedMillis = in.readVLong();
        this.successfulMasterApplyElapsedMillis = in.readVLong();
        this.successfulNotificationElapsedMillis = in.readVLong();
        this.failedComputationElapsedMillis = in.readVLong();
        this.failedPublicationElapsedMillis = in.readVLong();
        this.failedContextConstructionElapsedMillis = in.readVLong();
        this.failedCommitElapsedMillis = in.readVLong();
        this.failedCompletionElapsedMillis = in.readVLong();
        this.failedMasterApplyElapsedMillis = in.readVLong();
        this.failedNotificationElapsedMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert out.getTransportVersion().onOrAfter(TransportVersion.V_7_16_0) : out.getTransportVersion();
        out.writeVLong(unchangedTaskCount);
        out.writeVLong(publicationSuccessCount);
        out.writeVLong(publicationFailureCount);
        out.writeVLong(unchangedComputationElapsedMillis);
        out.writeVLong(unchangedNotificationElapsedMillis);
        out.writeVLong(successfulComputationElapsedMillis);
        out.writeVLong(successfulPublicationElapsedMillis);
        out.writeVLong(successfulContextConstructionElapsedMillis);
        out.writeVLong(successfulCommitElapsedMillis);
        out.writeVLong(successfulCompletionElapsedMillis);
        out.writeVLong(successfulMasterApplyElapsedMillis);
        out.writeVLong(successfulNotificationElapsedMillis);
        out.writeVLong(failedComputationElapsedMillis);
        out.writeVLong(failedPublicationElapsedMillis);
        out.writeVLong(failedContextConstructionElapsedMillis);
        out.writeVLong(failedCommitElapsedMillis);
        out.writeVLong(failedCompletionElapsedMillis);
        out.writeVLong(failedMasterApplyElapsedMillis);
        out.writeVLong(failedNotificationElapsedMillis);
    }

    public static ClusterStateUpdateStats EMPTY = new ClusterStateUpdateStats(
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L
    );

    public long getUnchangedTaskCount() {
        return unchangedTaskCount;
    }

    public long getPublicationSuccessCount() {
        return publicationSuccessCount;
    }

    public long getPublicationFailureCount() {
        return publicationFailureCount;
    }

    public long getUnchangedComputationElapsedMillis() {
        return unchangedComputationElapsedMillis;
    }

    public long getUnchangedNotificationElapsedMillis() {
        return unchangedNotificationElapsedMillis;
    }

    public long getSuccessfulComputationElapsedMillis() {
        return successfulComputationElapsedMillis;
    }

    public long getSuccessfulPublicationElapsedMillis() {
        return successfulPublicationElapsedMillis;
    }

    public long getSuccessfulContextConstructionElapsedMillis() {
        return successfulContextConstructionElapsedMillis;
    }

    public long getSuccessfulCommitElapsedMillis() {
        return successfulCommitElapsedMillis;
    }

    public long getSuccessfulCompletionElapsedMillis() {
        return successfulCompletionElapsedMillis;
    }

    public long getSuccessfulMasterApplyElapsedMillis() {
        return successfulMasterApplyElapsedMillis;
    }

    public long getSuccessfulNotificationElapsedMillis() {
        return successfulNotificationElapsedMillis;
    }

    public long getFailedComputationElapsedMillis() {
        return failedComputationElapsedMillis;
    }

    public long getFailedPublicationElapsedMillis() {
        return failedPublicationElapsedMillis;
    }

    public long getFailedContextConstructionElapsedMillis() {
        return failedContextConstructionElapsedMillis;
    }

    public long getFailedCommitElapsedMillis() {
        return failedCommitElapsedMillis;
    }

    public long getFailedCompletionElapsedMillis() {
        return failedCompletionElapsedMillis;
    }

    public long getFailedMasterApplyElapsedMillis() {
        return failedMasterApplyElapsedMillis;
    }

    public long getFailedNotificationElapsedMillis() {
        return failedNotificationElapsedMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("cluster_state_update");

        builder.startObject("unchanged");
        builder.field("count", unchangedTaskCount);
        msField(builder, "computation", unchangedComputationElapsedMillis);
        msField(builder, "notification", unchangedNotificationElapsedMillis);
        builder.endObject();

        builder.startObject("success");
        builder.field("count", publicationSuccessCount);
        msField(builder, "computation", successfulComputationElapsedMillis);
        msField(builder, "publication", successfulPublicationElapsedMillis);
        msField(builder, "context_construction", successfulContextConstructionElapsedMillis);
        msField(builder, "commit", successfulCommitElapsedMillis);
        msField(builder, "completion", successfulCompletionElapsedMillis);
        msField(builder, "master_apply", successfulMasterApplyElapsedMillis);
        msField(builder, "notification", successfulNotificationElapsedMillis);
        builder.endObject();

        builder.startObject("failure");
        builder.field("count", publicationFailureCount);
        msField(builder, "computation", failedComputationElapsedMillis);
        msField(builder, "publication", failedPublicationElapsedMillis);
        msField(builder, "context_construction", failedContextConstructionElapsedMillis);
        msField(builder, "commit", failedCommitElapsedMillis);
        msField(builder, "completion", failedCompletionElapsedMillis);
        msField(builder, "master_apply", failedMasterApplyElapsedMillis);
        msField(builder, "notification", failedNotificationElapsedMillis);
        builder.endObject();

        builder.endObject();
        return builder;
    }

    private static void msField(XContentBuilder builder, String name, long millis) throws IOException {
        builder.humanReadableField(name + "_time_millis", name + "_time", TimeValue.timeValueMillis(millis));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateUpdateStats that = (ClusterStateUpdateStats) o;
        return unchangedTaskCount == that.unchangedTaskCount
            && publicationSuccessCount == that.publicationSuccessCount
            && publicationFailureCount == that.publicationFailureCount
            && unchangedComputationElapsedMillis == that.unchangedComputationElapsedMillis
            && unchangedNotificationElapsedMillis == that.unchangedNotificationElapsedMillis
            && successfulComputationElapsedMillis == that.successfulComputationElapsedMillis
            && successfulPublicationElapsedMillis == that.successfulPublicationElapsedMillis
            && successfulContextConstructionElapsedMillis == that.successfulContextConstructionElapsedMillis
            && successfulCommitElapsedMillis == that.successfulCommitElapsedMillis
            && successfulCompletionElapsedMillis == that.successfulCompletionElapsedMillis
            && successfulMasterApplyElapsedMillis == that.successfulMasterApplyElapsedMillis
            && successfulNotificationElapsedMillis == that.successfulNotificationElapsedMillis
            && failedComputationElapsedMillis == that.failedComputationElapsedMillis
            && failedPublicationElapsedMillis == that.failedPublicationElapsedMillis
            && failedContextConstructionElapsedMillis == that.failedContextConstructionElapsedMillis
            && failedCommitElapsedMillis == that.failedCommitElapsedMillis
            && failedCompletionElapsedMillis == that.failedCompletionElapsedMillis
            && failedMasterApplyElapsedMillis == that.failedMasterApplyElapsedMillis
            && failedNotificationElapsedMillis == that.failedNotificationElapsedMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            unchangedTaskCount,
            publicationSuccessCount,
            publicationFailureCount,
            unchangedComputationElapsedMillis,
            unchangedNotificationElapsedMillis,
            successfulComputationElapsedMillis,
            successfulPublicationElapsedMillis,
            successfulContextConstructionElapsedMillis,
            successfulCommitElapsedMillis,
            successfulCompletionElapsedMillis,
            successfulMasterApplyElapsedMillis,
            successfulNotificationElapsedMillis,
            failedComputationElapsedMillis,
            failedPublicationElapsedMillis,
            failedContextConstructionElapsedMillis,
            failedCommitElapsedMillis,
            failedCompletionElapsedMillis,
            failedMasterApplyElapsedMillis,
            failedNotificationElapsedMillis
        );
    }
}
