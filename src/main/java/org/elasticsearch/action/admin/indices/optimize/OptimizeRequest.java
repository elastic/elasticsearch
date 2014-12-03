/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.optimize;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to optimize one or more indices. In order to optimize on all the indices, pass an empty array or
 * <tt>null</tt> for the indices.
 * <p/>
 * <p>{@link #waitForMerge(boolean)} allows to control if the call will block until the optimize completes and
 * defaults to <tt>true</tt>.
 * <p/>
 * <p>{@link #maxNumSegments(int)} allows to control the number of segments to optimize down to. By default, will
 * cause the optimize process to optimize down to half the configured number of segments.
 *
 * @see org.elasticsearch.client.Requests#optimizeRequest(String...)
 * @see org.elasticsearch.client.IndicesAdminClient#optimize(OptimizeRequest)
 * @see OptimizeResponse
 */
public class OptimizeRequest extends BroadcastOperationRequest<OptimizeRequest> {

    public static final class Defaults {
        public static final boolean WAIT_FOR_MERGE = true;
        public static final int MAX_NUM_SEGMENTS = -1;
        public static final boolean ONLY_EXPUNGE_DELETES = false;
        public static final boolean FLUSH = true;
        public static final boolean UPGRADE = false;
    }

    private boolean waitForMerge = Defaults.WAIT_FOR_MERGE;
    private int maxNumSegments = Defaults.MAX_NUM_SEGMENTS;
    private boolean onlyExpungeDeletes = Defaults.ONLY_EXPUNGE_DELETES;
    private boolean flush = Defaults.FLUSH;
    private boolean upgrade = Defaults.UPGRADE;

    /**
     * Constructs an optimization request over one or more indices.
     *
     * @param indices The indices to optimize, no indices passed means all indices will be optimized.
     */
    public OptimizeRequest(String... indices) {
        super(indices);
    }

    public OptimizeRequest() {

    }

    /**
     * Should the call block until the optimize completes. Defaults to <tt>true</tt>.
     */
    public boolean waitForMerge() {
        return waitForMerge;
    }

    /**
     * Should the call block until the optimize completes. Defaults to <tt>true</tt>.
     */
    public OptimizeRequest waitForMerge(boolean waitForMerge) {
        this.waitForMerge = waitForMerge;
        return this;
    }

    /**
     * Will optimize the index down to <= maxNumSegments. By default, will cause the optimize
     * process to optimize down to half the configured number of segments.
     */
    public int maxNumSegments() {
        return maxNumSegments;
    }

    /**
     * Will optimize the index down to <= maxNumSegments. By default, will cause the optimize
     * process to optimize down to half the configured number of segments.
     */
    public OptimizeRequest maxNumSegments(int maxNumSegments) {
        this.maxNumSegments = maxNumSegments;
        return this;
    }

    /**
     * Should the optimization only expunge deletes from the index, without full optimization.
     * Defaults to full optimization (<tt>false</tt>).
     */
    public boolean onlyExpungeDeletes() {
        return onlyExpungeDeletes;
    }

    /**
     * Should the optimization only expunge deletes from the index, without full optimization.
     * Defaults to full optimization (<tt>false</tt>).
     */
    public OptimizeRequest onlyExpungeDeletes(boolean onlyExpungeDeletes) {
        this.onlyExpungeDeletes = onlyExpungeDeletes;
        return this;
    }

    /**
     * Should flush be performed after the optimization. Defaults to <tt>true</tt>.
     */
    public boolean flush() {
        return flush;
    }

    /**
     * Should flush be performed after the optimization. Defaults to <tt>true</tt>.
     */
    public OptimizeRequest flush(boolean flush) {
        this.flush = flush;
        return this;
    }

    /**
     * @deprecated See {@link #upgrade()}
     */
    @Deprecated
    public boolean force() {
        return upgrade;
    }

    /**
     * @deprecated Use {@link #upgrade(boolean)}.
     */
    @Deprecated
    public OptimizeRequest force(boolean force) {
        this.upgrade = force;
        return this;
    }

    /**
     * Should the merge upgrade all old segments to the current index format.
     * Defaults to <tt>false</tt>.
     */
    public boolean upgrade() {
        return upgrade;
    }

    /**
     * See {@link #upgrade()}
     */
    public OptimizeRequest upgrade(boolean upgrade) {
        this.upgrade = upgrade;
        return this;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        waitForMerge = in.readBoolean();
        maxNumSegments = in.readInt();
        onlyExpungeDeletes = in.readBoolean();
        flush = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_1_1_0)) {
            upgrade = in.readBoolean();
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(waitForMerge);
        out.writeInt(maxNumSegments);
        out.writeBoolean(onlyExpungeDeletes);
        out.writeBoolean(flush);
        if (out.getVersion().onOrAfter(Version.V_1_1_0)) {
            out.writeBoolean(upgrade);
        }
    }
}
