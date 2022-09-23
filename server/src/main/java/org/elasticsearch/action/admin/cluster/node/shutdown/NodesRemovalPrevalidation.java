/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class NodesRemovalPrevalidation implements ToXContentObject, Writeable {

    private final Result result;
    private final List<NodeResult> nodes;

    public NodesRemovalPrevalidation(Result result, List<NodeResult> nodes) {
        this.result = result;
        this.nodes = nodes;
    }

    public NodesRemovalPrevalidation(final StreamInput in) throws IOException {
        this.result = Result.readFrom(in);
        this.nodes = in.readList(NodeResult::readFrom);
    }

    public Result getResult() {
        return result;
    }

    public List<NodeResult> getNodes() {
        return nodes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("result", result);
        builder.xContentList("nodes", nodes, params);
        return builder.endObject();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        result.writeTo(out);
        out.writeList(nodes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof NodesRemovalPrevalidation == false) return false;
        NodesRemovalPrevalidation other = (NodesRemovalPrevalidation) o;
        return Objects.equals(result, other.result) && Objects.equals(nodes, other.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(result, nodes);
    }

    public record Result(IsSafe isSafe, String reason) implements ToXContentObject, Writeable {
        public static Result readFrom(final StreamInput in) throws IOException {
            return new Result(IsSafe.readFrom(in), in.readString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("is_safe", isSafe.name());
            builder.field("reason", reason);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(isSafe.isSafe());
            out.writeString(reason);
        }
    }

    public record NodeResult(String name, String Id, String externalId, Result result) implements ToXContentObject, Writeable {
        public static NodeResult readFrom(final StreamInput in) throws IOException {
            return new NodeResult(in.readString(), in.readString(), in.readString(), Result.readFrom(in));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("id", Id);
            builder.field("name", name);
            builder.field("external_id", externalId);
            builder.field("result", result);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(Id);
            out.writeString(externalId);
            result.writeTo(out);
        }
    }

    public enum IsSafe {
        YES("yes"),
        NO("no"),
        UNKNOWN("unknown");

        private final String isSafe;

        IsSafe(String isSafe) {
            this.isSafe = isSafe;
        }

        public String isSafe() {
            return isSafe;
        }

        public static IsSafe readFrom(final StreamInput in) throws IOException {
            return fromString(in.readString());
        }

        public static IsSafe fromString(String s) {
            return switch (s) {
                case "yes" -> YES;
                case "no" -> NO;
                case "unknown" -> UNKNOWN;
                default -> throw new IllegalArgumentException("unexpected IsSafe value [" + s + "]");
            };
        }
    }
}
