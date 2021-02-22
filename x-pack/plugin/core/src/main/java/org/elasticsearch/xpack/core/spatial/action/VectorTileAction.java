/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.spatial.action;


import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class VectorTileAction extends ActionType<VectorTileAction.Response> {
    public static final VectorTileAction INSTANCE = new VectorTileAction();
    public static final String NAME = "indices:data/read/vectortile";

    private VectorTileAction() {
        super(NAME, VectorTileAction.Response::new);
    }

    public static class Request extends BroadcastRequest<Request> implements ToXContentObject {
        private final String[] index;
        private final String field;
        private final int z;
        private final int x;
        private final int y;

        public Request(String[] index, String field, int z, int x, int y) {
            this.index = index;
            this.field = field;
            this.z = z;
            this.x = x;
            this.y = y;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            index = in.readStringArray();
            field = in.readString();
            z = in.readVInt();
            x = in.readVInt();
            y = in.readVInt();
        }

        @Override
        public String[] indices() {
            return index;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(index);
            out.writeString(field);
            out.writeVInt(z);
            out.writeVInt(x);
            out.writeVInt(y);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("index", index);
            builder.field("field", field);
            builder.field("z", z);
            builder.field("x", x);
            builder.field("y", y);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, field, z, x, y);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(index, other.index)
                && Objects.equals(field, other.field)
                && Objects.equals(z, other.z)
                && Objects.equals(x, other.x)
                && Objects.equals(y, other.y);
        }
    }

    public static class Response extends BroadcastResponse implements Writeable {
        private final byte[] vectorTile;

        public Response(final byte[] vectorTile) {
            this.vectorTile = vectorTile;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            vectorTile = in.readByteArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeByteArray(vectorTile);
        }

        public byte[] getVectorTiles() {
            return vectorTile;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Arrays.equals(vectorTile, response.vectorTile);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(vectorTile);
        }


    }

    public static class ShardRequest extends BroadcastShardRequest {
        private final Request request;

        public ShardRequest(StreamInput in) throws IOException {
            super(in);
            this.request = new Request(in);
        }

        public ShardRequest(ShardId shardId, Request request) {
            super(shardId, request);
            this.request = request;
        }

        public String getGeoField() {
            return request.field;
        }

        public int getZ() {
            return request.z;
        }

        public int getX() {
            return request.x;
        }

        public int getY() {
            return request.y;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class ShardResponse extends BroadcastShardResponse {
        private byte[] vectorTile;

        public ShardResponse(StreamInput in) throws IOException {
            super(in);
            vectorTile = in.readByteArray();
        }

        public ShardResponse(ShardId shardId, byte[] vectorTile) {
            super(shardId);
            this.vectorTile = vectorTile;
        }

        public byte[] getVectorTile() {
            return vectorTile;
        }
    }
}
