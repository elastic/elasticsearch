/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

public class ReindexDataStreamAction extends ActionType<ReindexDataStreamAction.ReindexDataStreamResponse> {
    public static final FeatureFlag REINDEX_DATA_STREAM_FEATURE_FLAG = new FeatureFlag("reindex_data_stream");

    public static final ReindexDataStreamAction INSTANCE = new ReindexDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/reindex";

    public ReindexDataStreamAction() {
        super(NAME);
    }

    public static class ReindexDataStreamResponse extends ActionResponse implements ToXContentObject {
        private final String taskId;

        public ReindexDataStreamResponse(String taskId) {
            super();
            this.taskId = taskId;
        }

        public ReindexDataStreamResponse(StreamInput in) throws IOException {
            super(in);
            this.taskId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(taskId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("task", getTaskId());
            builder.endObject();
            return builder;
        }

        public String getTaskId() {
            return taskId;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(taskId);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ReindexDataStreamResponse && taskId.equals(((ReindexDataStreamResponse) other).taskId);
        }

    }

    public static class ReindexDataStreamRequest extends ActionRequest implements IndicesRequest, ToXContent {
        private final String mode;
        private final String sourceDataStream;

        public ReindexDataStreamRequest(String mode, String sourceDataStream) {
            this.mode = mode;
            this.sourceDataStream = sourceDataStream;
        }

        public ReindexDataStreamRequest(StreamInput in) throws IOException {
            super(in);
            this.mode = in.readString();
            this.sourceDataStream = in.readString();
        }

        private static final ConstructingObjectParser<ReindexDataStreamRequest, Predicate<NodeFeature>> PARSER =
            new ConstructingObjectParser<>("migration_reindex", new Function<Object[], ReindexDataStreamRequest>() {
                @Override
                public ReindexDataStreamRequest apply(Object[] objects) {
                    String mode = (String) objects[0];
                    String source = (String) objects[1];
                    return new ReindexDataStreamRequest(mode, source);
                }
            });

        private static final ConstructingObjectParser<String, Void> SOURCE_PARSER = new ConstructingObjectParser<>(
            "source",
            false,
            (a, id) -> (String) a[0]
        );

        static {
            SOURCE_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("index"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("mode"));
            PARSER.declareObject(
                ConstructingObjectParser.constructorArg(),
                (parser, id) -> SOURCE_PARSER.apply(parser, null),
                new ParseField("source")
            );
        }

        public static ReindexDataStreamRequest fromXContent(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(mode);
            out.writeString(sourceDataStream);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean getShouldStoreResult() {
            return true; // do not wait_for_completion
        }

        public String getSourceDataStream() {
            return sourceDataStream;
        }

        public String getMode() {
            return mode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode, sourceDataStream);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ReindexDataStreamRequest otherRequest
                && mode.equals(otherRequest.mode)
                && sourceDataStream.equals(otherRequest.sourceDataStream);
        }

        @Override
        public String[] indices() {
            return new String[] { sourceDataStream };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        /*
         * This only exists for the sake of testing the xcontent parser
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("mode", mode);
            builder.startObject("source");
            builder.field("index", sourceDataStream);
            builder.endObject();
            return builder;
        }
    }
}
