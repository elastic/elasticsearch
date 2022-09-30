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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class NodesRemovalPrevalidation implements ToXContentObject, Writeable {

    private static final ParseField RESULT_FIELD = new ParseField("result");
    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<NodesRemovalPrevalidation, Void> PARSER = new ConstructingObjectParser<>(
        "nodes_removal_prevalidation",
        objects -> new NodesRemovalPrevalidation((Result) objects[0], (List<NodeResult>) objects[1])
    );

    static {
        configureParser(PARSER);
    }

    static <T> void configureParser(ConstructingObjectParser<T, Void> parser) {
        parser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Result.fromXContent(p), RESULT_FIELD);
        parser.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> NodeResult.fromXContent(p), NODES_FIELD);
    }

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
    public void writeTo(final StreamOutput out) throws IOException {
        result.writeTo(out);
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("result", result);
        builder.xContentList("nodes", nodes, params);
        return builder.endObject();
    }

    public static NodesRemovalPrevalidation fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
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

    @Override
    public String toString() {
        return "NodesRemovalPrevalidation{" + "result=" + result + ", nodes=" + nodes + '}';
    }

    public record Result(IsSafe isSafe, String reason) implements ToXContentObject, Writeable {

        private static final ParseField IS_SAFE_FIELD = new ParseField("is_safe");
        private static final ParseField REASON_FIELD = new ParseField("reason");

        private static final ConstructingObjectParser<Result, Void> PARSER = new ConstructingObjectParser<>(
            "nodes_removal_prevalidation_result",
            objects -> new Result(IsSafe.fromString((String) objects[0]), (String) objects[1])
        );

        static {
            configureParser(PARSER);
        }

        static <T> void configureParser(ConstructingObjectParser<T, Void> parser) {
            parser.declareString(ConstructingObjectParser.constructorArg(), IS_SAFE_FIELD);
            parser.declareString(ConstructingObjectParser.constructorArg(), REASON_FIELD);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(isSafe.isSafe());
            out.writeString(reason);
        }

        public static Result readFrom(final StreamInput in) throws IOException {
            return new Result(IsSafe.readFrom(in), in.readString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(IS_SAFE_FIELD.getPreferredName(), isSafe.name());
            builder.field(REASON_FIELD.getPreferredName(), reason);
            builder.endObject();
            return builder;
        }

        public static Result fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    public record NodeResult(String name, String Id, String externalId, Result result) implements ToXContentObject, Writeable {

        private static final ParseField NAME_FIELD = new ParseField("name");
        private static final ParseField ID_FIELD = new ParseField("id");
        private static final ParseField EXTERNAL_ID_FIELD = new ParseField("external_id");
        private static final ParseField RESULT_FIELD = new ParseField("result");

        private static final ConstructingObjectParser<NodeResult, Void> PARSER = new ConstructingObjectParser<>(
            "nodes_removal_prevalidation_node_result",
            objects -> new NodeResult((String) objects[0], (String) objects[1], (String) objects[2], (Result) objects[3])
        );

        static {
            configureParser(PARSER);
        }

        static <T> void configureParser(ConstructingObjectParser<T, Void> parser) {
            parser.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
            parser.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
            parser.declareString(ConstructingObjectParser.constructorArg(), EXTERNAL_ID_FIELD);
            parser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Result.fromXContent(p), RESULT_FIELD);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(Id);
            out.writeString(externalId);
            result.writeTo(out);
        }

        public static NodeResult readFrom(final StreamInput in) throws IOException {
            return new NodeResult(in.readString(), in.readString(), in.readString(), Result.readFrom(in));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", name);
            builder.field("id", Id);
            builder.field("external_id", externalId);
            builder.field("result", result);
            builder.endObject();
            return builder;
        }

        public static NodeResult fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
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
            return switch (s.toLowerCase(Locale.ROOT)) {
                case "yes" -> YES;
                case "no" -> NO;
                case "unknown" -> UNKNOWN;
                default -> throw new IllegalArgumentException("unexpected IsSafe value [" + s + "]");
            };
        }
    }
}
