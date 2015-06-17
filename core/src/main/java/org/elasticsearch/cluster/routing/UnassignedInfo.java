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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Holds additional information as to why the shard is in unassigned state.
 */
public class UnassignedInfo implements ToXContent, Writeable<UnassignedInfo> {

    public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern("dateOptionalTime");

    /**
     * Reason why the shard is in unassigned state.
     * <p/>
     * Note, ordering of the enum is important, make sure to add new values
     * at the end and handle version serialization properly.
     */
    public enum Reason {
        /**
         * Unassigned as a result of an API creation of an index.
         */
        INDEX_CREATED,
        /**
         * Unassigned as a result of a full cluster recovery.
         */
        CLUSTER_RECOVERED,
        /**
         * Unassigned as a result of opening a closed index.
         */
        INDEX_REOPENED,
        /**
         * Unassigned as a result of importing a dangling index.
         */
        DANGLING_INDEX_IMPORTED,
        /**
         * Unassigned as a result of restoring into a new index.
         */
        NEW_INDEX_RESTORED,
        /**
         * Unassigned as a result of restoring into a closed index.
         */
        EXISTING_INDEX_RESTORED,
        /**
         * Unassigned as a result of explicit addition of a replica.
         */
        REPLICA_ADDED,
        /**
         * Unassigned as a result of a failed allocation of the shard.
         */
        ALLOCATION_FAILED,
        /**
         * Unassigned as a result of the node hosting it leaving the cluster.
         */
        NODE_LEFT,
        /**
         * Unassigned as a result of explicit cancel reroute command.
         */
        REROUTE_CANCELLED;
    }

    private final Reason reason;
    private final long timestamp;
    private final String details;

    public UnassignedInfo(Reason reason, String details) {
        this(reason, System.currentTimeMillis(), details);
    }

    private UnassignedInfo(Reason reason, long timestamp, String details) {
        this.reason = reason;
        this.timestamp = timestamp;
        this.details = details;
    }

    UnassignedInfo(StreamInput in) throws IOException {
        this.reason = Reason.values()[(int) in.readByte()];
        this.timestamp = in.readLong();
        this.details = in.readOptionalString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) reason.ordinal());
        out.writeLong(timestamp);
        out.writeOptionalString(details);
    }

    public UnassignedInfo readFrom(StreamInput in) throws IOException {
        return new UnassignedInfo(in);
    }

    /**
     * The reason why the shard is unassigned.
     */
    public Reason getReason() {
        return this.reason;
    }

    /**
     * The timestamp in milliseconds since epoch. Note, we use timestamp here since
     * we want to make sure its preserved across node serializations. Extra care need
     * to be made if its used to calculate diff (handle negative values) in case of
     * time drift.
     */
    public long getTimestampInMillis() {
        return this.timestamp;
    }

    /**
     * Returns optional details explaining the reasons.
     */
    @Nullable
    public String getDetails() {
        return this.details;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("unassigned_info[[reason=").append(reason).append("]");
        sb.append(", at[").append(DATE_TIME_FORMATTER.printer().print(timestamp)).append("]");
        if (details != null) {
            sb.append(", details[").append(details).append("]");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("unassigned_info");
        builder.field("reason", reason);
        builder.field("at", DATE_TIME_FORMATTER.printer().print(timestamp));
        if (details != null) {
            builder.field("details", details);
        }
        builder.endObject();
        return builder;
    }
}
