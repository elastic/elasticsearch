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

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to force merging the segments of one or more indices. In order to
 * run a merge on all the indices, pass an empty array or <tt>null</tt> for the
 * indices.
 * {@link #maxNumSegments(int)} allows to control the number of segments
 * to force merge down to. Defaults to simply checking if a merge needs
 * to execute, and if so, executes it
 *
 * @see org.elasticsearch.client.Requests#forceMergeRequest(String...)
 * @see org.elasticsearch.client.IndicesAdminClient#forceMerge(ForceMergeRequest)
 * @see ForceMergeResponse
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
     * Constructs a merge request over one or more indices.
     *
     * @param indices The indices to merge, no indices passed means all indices will be merged.
     */
    public ForceMergeRequest(String... indices) {
        super(indices);
    }

    public ForceMergeRequest() {

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
     * Defaults to full merging (<tt>false</tt>).
     */
    public boolean onlyExpungeDeletes() {
        return onlyExpungeDeletes;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merge.
     * Defaults to full merging (<tt>false</tt>).
     */
    public ForceMergeRequest onlyExpungeDeletes(boolean onlyExpungeDeletes) {
        this.onlyExpungeDeletes = onlyExpungeDeletes;
        return this;
    }

    /**
     * Should flush be performed after the merge. Defaults to <tt>true</tt>.
     */
    public boolean flush() {
        return flush;
    }

    /**
     * Should flush be performed after the merge. Defaults to <tt>true</tt>.
     */
    public ForceMergeRequest flush(boolean flush) {
        this.flush = flush;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        maxNumSegments = in.readInt();
        onlyExpungeDeletes = in.readBoolean();
        flush = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(maxNumSegments);
        out.writeBoolean(onlyExpungeDeletes);
        out.writeBoolean(flush);
    }

    @Override
    public String toString() {
        return "ForceMergeRequest{" +
                "maxNumSegments=" + maxNumSegments +
                ", onlyExpungeDeletes=" + onlyExpungeDeletes +
                ", flush=" + flush +
                '}';
    }
}
