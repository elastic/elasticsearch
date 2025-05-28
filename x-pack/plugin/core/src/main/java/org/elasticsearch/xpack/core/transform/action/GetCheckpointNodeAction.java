/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

public class GetCheckpointNodeAction extends ActionType<GetCheckpointNodeAction.Response> {

    public static final GetCheckpointNodeAction INSTANCE = new GetCheckpointNodeAction();

    // note: this is an index action and requires `view_index_metadata`
    public static final String NAME = GetCheckpointAction.NAME + "[n]";

    private GetCheckpointNodeAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse {
        private final Map<String, long[]> checkpoints;

        public Response(Map<String, long[]> checkpoints) {
            this.checkpoints = checkpoints;
        }

        public Response(StreamInput in) throws IOException {
            this.checkpoints = in.readOrderedMap(StreamInput::readString, StreamInput::readLongArray);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(getCheckpoints(), StreamOutput::writeLongArray);
        }

        public Map<String, long[]> getCheckpoints() {
            return checkpoints;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Response that = (Response) obj;

            return this.checkpoints.size() == that.checkpoints.size()
                && this.checkpoints.entrySet().stream().allMatch(e -> Arrays.equals(e.getValue(), that.checkpoints.get(e.getKey())));
        }

        @Override
        public int hashCode() {
            int hash = 1;

            for (Entry<String, long[]> e : checkpoints.entrySet()) {
                hash = 31 * hash + Objects.hash(e.getKey(), Arrays.hashCode(e.getValue()));
            }

            return hash;
        }
    }

    public static class Request extends LegacyActionRequest implements IndicesRequest {

        private final Set<ShardId> shards;
        private final OriginalIndices originalIndices;
        private final TimeValue timeout;

        public Request(Set<ShardId> shards, OriginalIndices originalIndices, TimeValue timeout) {
            this.shards = shards;
            this.originalIndices = originalIndices;
            this.timeout = timeout;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shards = in.readCollectionAsImmutableSet(ShardId::new);
            this.originalIndices = OriginalIndices.readOriginalIndices(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                this.timeout = in.readOptionalTimeValue();
            } else {
                this.timeout = null;
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(shards);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeOptionalTimeValue(timeout);
            }
        }

        public Set<ShardId> getShards() {
            return shards;
        }

        public OriginalIndices getOriginalIndices() {
            return originalIndices;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request that = (Request) obj;

            return Objects.equals(shards, that.shards)
                && Objects.equals(originalIndices, that.originalIndices)
                && Objects.equals(timeout, that.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shards, originalIndices, timeout);
        }

        @Override
        public String[] indices() {
            return originalIndices.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return originalIndices.indicesOptions();
        }

        @Override
        public CancellableTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(
                id,
                type,
                action,
                format("get_checkpoint_node[%d;%d]", indices() != null ? indices().length : 0, shards != null ? shards.size() : 0),
                parentTaskId,
                headers
            );
        }
    }
}
