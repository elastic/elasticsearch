/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to force merging the segments of one or more indices. In order to
 * run a merge on all the indices, pass an empty array or {@code null} for the
 * indices.
 * {@link #maxNumSegments(int)} allows to control the number of segments
 * to force merge down to. Defaults to simply checking if a merge needs
 * to execute, and if so, executes it
 *
 * @see org.elasticsearch.client.internal.IndicesAdminClient#forceMerge(ForceMergeRequest)
 */
public class ForceMergeRequest extends BroadcastRequest<ForceMergeRequest> {

    public static final class Defaults {
        public static final int MAX_NUM_SEGMENTS = -1;
        public static final boolean ONLY_EXPUNGE_DELETES = false;
        public static final boolean FLUSH = true;
    }

    private int maxNumSegments = Defaults.MAX_NUM_SEGMENTS;
    private boolean onlyExpungeDeletes = Defaults.ONLY_EXPUNGE_DELETES;
    private boolean flush = Defaults.FLUSH;
    /**
     * Should this task store its result?
     */
    private boolean shouldStoreResult;

    private static final TransportVersion FORCE_MERGE_UUID_SIMPLE_VERSION = TransportVersions.V_8_0_0;

    /**
     * Force merge UUID to store in the live commit data of a shard under
     * {@link org.elasticsearch.index.engine.Engine#FORCE_MERGE_UUID_KEY} after force merging it.
     */
    private final String forceMergeUUID;

    /**
     * Constructs a merge request over one or more indices.
     *
     * @param indices The indices to merge, no indices passed means all indices will be merged.
     */
    public ForceMergeRequest(String... indices) {
        super(indices);
        forceMergeUUID = UUIDs.randomBase64UUID();
    }

    public ForceMergeRequest(StreamInput in) throws IOException {
        super(in);
        maxNumSegments = in.readInt();
        onlyExpungeDeletes = in.readBoolean();
        flush = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(FORCE_MERGE_UUID_SIMPLE_VERSION)) {
            forceMergeUUID = in.readString();
        } else {
            forceMergeUUID = in.readOptionalString();
            assert forceMergeUUID != null : "optional was just used as a BwC measure";
        }
    }

    /**
     * Will merge the index down to &lt;= maxNumSegments. By default, will cause the merge
     * process to merge down to half the configured number of segments.
     */
    public int maxNumSegments() {
        return maxNumSegments;
    }

    /**
     * Will merge the index down to &lt;= maxNumSegments. By default, will cause the merge
     * process to merge down to half the configured number of segments.
     */
    public ForceMergeRequest maxNumSegments(int maxNumSegments) {
        this.maxNumSegments = maxNumSegments;
        return this;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merging.
     * Defaults to full merging ({@code false}).
     */
    public boolean onlyExpungeDeletes() {
        return onlyExpungeDeletes;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merge.
     * Defaults to full merging ({@code false}).
     */
    public ForceMergeRequest onlyExpungeDeletes(boolean onlyExpungeDeletes) {
        this.onlyExpungeDeletes = onlyExpungeDeletes;
        return this;
    }

    /**
     * Force merge UUID to use when force merging.
     */
    public String forceMergeUUID() {
        return forceMergeUUID;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     */
    public boolean flush() {
        return flush;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     */
    public ForceMergeRequest flush(boolean flush) {
        this.flush = flush;
        return this;
    }

    /**
     * Should this task store its result after it has finished?
     */
    public ForceMergeRequest setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
        return this;
    }

    @Override
    public boolean getShouldStoreResult() {
        return shouldStoreResult;
    }

    @Override
    public String getDescription() {
        return "Force-merge indices "
            + Arrays.toString(indices())
            + ", maxSegments["
            + maxNumSegments
            + "], onlyExpungeDeletes["
            + onlyExpungeDeletes
            + "], flush["
            + flush
            + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(maxNumSegments);
        out.writeBoolean(onlyExpungeDeletes);
        out.writeBoolean(flush);
        if (out.getTransportVersion().onOrAfter(FORCE_MERGE_UUID_SIMPLE_VERSION)) {
            out.writeString(forceMergeUUID);
        } else {
            out.writeOptionalString(forceMergeUUID);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationError = super.validate();
        if (onlyExpungeDeletes && maxNumSegments != Defaults.MAX_NUM_SEGMENTS) {
            validationError = addValidationError(
                "cannot set only_expunge_deletes and max_num_segments at the same time, those two " + "parameters are mutually exclusive",
                validationError
            );
        }
        return validationError;
    }

    @Override
    public String toString() {
        return "ForceMergeRequest{"
            + "maxNumSegments="
            + maxNumSegments
            + ", onlyExpungeDeletes="
            + onlyExpungeDeletes
            + ", flush="
            + flush
            + '}';
    }
}
