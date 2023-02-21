/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public record NodesRemovalPrevalidation(boolean isSafe, String message, List<NodeResult> nodes) implements ToXContentObject, Writeable {

    private static final ParseField IS_SAFE_FIELD = new ParseField("is_safe");
    private static final ParseField MESSAGE_FIELD = new ParseField("message");
    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<NodesRemovalPrevalidation, Void> PARSER = new ConstructingObjectParser<>(
        "nodes_removal_prevalidation",
        objects -> new NodesRemovalPrevalidation((boolean) objects[0], (String) objects[1], (List<NodeResult>) objects[2])
    );

    static {
        configureParser(PARSER);
    }

    static <T> void configureParser(ConstructingObjectParser<T, Void> parser) {
        parser.declareBoolean(ConstructingObjectParser.constructorArg(), IS_SAFE_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), MESSAGE_FIELD);
        parser.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> NodeResult.fromXContent(p), NODES_FIELD);
    }

    public static NodesRemovalPrevalidation readFrom(final StreamInput in) throws IOException {
        return new NodesRemovalPrevalidation(in.readBoolean(), in.readString(), in.readList(NodeResult::readFrom));
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeBoolean(isSafe);
        out.writeString(message);
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(IS_SAFE_FIELD.getPreferredName(), isSafe);
        builder.field(MESSAGE_FIELD.getPreferredName(), message);
        builder.xContentList(NODES_FIELD.getPreferredName(), nodes, params);
        return builder.endObject();
    }

    public static NodesRemovalPrevalidation fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    // Prevalidation response for one node including its result
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
            builder.field(NAME_FIELD.getPreferredName(), name);
            builder.field(ID_FIELD.getPreferredName(), Id);
            builder.field(EXTERNAL_ID_FIELD.getPreferredName(), externalId);
            builder.field(RESULT_FIELD.getPreferredName(), result);
            builder.endObject();
            return builder;
        }

        public static NodeResult fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     *  The prevalidation result of a node.
     * @param reason is nullable only for BWC between 8.6 and 8.7. In a fully-upgraded 8.7, it should always be non-null.
     */
    public record Result(boolean isSafe, @Nullable Reason reason, String message) implements ToXContentObject, Writeable {

        private static final ParseField IS_SAFE_FIELD = new ParseField("is_safe");
        private static final ParseField REASON_FIELD = new ParseField("reason");
        private static final ParseField MESSAGE_FIELD = new ParseField("message");

        private static final ConstructingObjectParser<Result, Void> PARSER = new ConstructingObjectParser<>(
            "nodes_removal_prevalidation_result",
            objects -> new Result((boolean) objects[0], Reason.fromString((String) objects[1]), (String) objects[2])
        );

        static {
            configureParser(PARSER);
        }

        static <T> void configureParser(ConstructingObjectParser<T, Void> parser) {
            parser.declareBoolean(ConstructingObjectParser.constructorArg(), IS_SAFE_FIELD);
            parser.declareString(ConstructingObjectParser.constructorArg(), REASON_FIELD);
            parser.declareString(ConstructingObjectParser.constructorArg(), MESSAGE_FIELD);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(isSafe);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                reason.writeTo(out);
            }
            out.writeString(message);
        }

        public static Result readFrom(final StreamInput in) throws IOException {
            if (in.getTransportVersion().before(TransportVersion.V_8_7_0)) {
                return new Result(in.readBoolean(), null, in.readString());
            }
            return new Result(in.readBoolean(), Reason.readFrom(in), in.readString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(IS_SAFE_FIELD.getPreferredName(), isSafe);
            if (reason != null) {
                builder.field(REASON_FIELD.getPreferredName(), reason.reason);
            }
            builder.field(MESSAGE_FIELD.getPreferredName(), message);
            builder.endObject();
            return builder;
        }

        public static Result fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    public enum Reason implements Writeable {
        NO_PROBLEMS("no_problems"),
        NO_RED_SHARDS_ON_NODE("no_red_shards_on_node"),
        NO_RED_SHARDS_EXCEPT_SEARCHABLE_SNAPSHOTS("no_red_shards_except_searchable_snapshots"),
        RED_SHARDS_ON_NODE("red_shards_on_node"),
        UNABLE_TO_VERIFY("unable_to_verify_red_shards");

        private final String reason;

        Reason(String reason) {
            this.reason = reason;
        }

        public String reason() {
            return reason;
        }

        public static Reason readFrom(final StreamInput in) throws IOException {
            return fromString(in.readString());
        }

        public static Reason fromString(String s) {
            for (Reason r : values()) {
                if (s.equalsIgnoreCase(r.reason)) {
                    return r;
                }
            }
            throw new IllegalArgumentException("unexpected Reason value [" + s + "]");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(reason);
        }
    }
}
