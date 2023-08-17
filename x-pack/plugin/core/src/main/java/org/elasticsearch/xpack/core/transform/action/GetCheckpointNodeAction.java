/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public class GetCheckpointNodeAction extends ActionType<GetCheckpointNodeAction.Response> {

    public static final GetCheckpointNodeAction INSTANCE = new GetCheckpointNodeAction();

    // note: this is an index action and requires `view_index_metadata`
    public static final String NAME = GetCheckpointAction.NAME + "[n]";

    private GetCheckpointNodeAction() {
        super(NAME, GetCheckpointNodeAction.Response::new);
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
            out.writeMap(getCheckpoints(), StreamOutput::writeString, StreamOutput::writeLongArray);
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

    public static class Request extends ActionRequest implements IndicesRequest {

        private final Set<ShardId> shards;
        private final OriginalIndices originalIndices;

        public Request(Set<ShardId> shards, OriginalIndices originalIndices) {
            this.shards = shards;
            this.originalIndices = originalIndices;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shards = in.readImmutableSet(ShardId::new);
            this.originalIndices = OriginalIndices.readOriginalIndices(in);
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
        }

        public Set<ShardId> getShards() {
            return shards;
        }

        public OriginalIndices getOriginalIndices() {
            return originalIndices;
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

            return Objects.equals(shards, that.shards) && Objects.equals(originalIndices, that.originalIndices);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shards, originalIndices);
        }

        @Override
        public String[] indices() {
            return originalIndices.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return originalIndices.indicesOptions();
        }

    }
}
