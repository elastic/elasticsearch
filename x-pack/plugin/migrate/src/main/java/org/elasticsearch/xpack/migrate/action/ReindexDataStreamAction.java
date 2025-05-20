/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public class ReindexDataStreamAction extends ActionType<AcknowledgedResponse> {
    public static final String TASK_ID_PREFIX = "reindex-data-stream-";

    public static final ReindexDataStreamAction INSTANCE = new ReindexDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/reindex";
    public static final ParseField MODE_FIELD = new ParseField("mode");
    public static final ParseField SOURCE_FIELD = new ParseField("source");
    public static final ParseField INDEX_FIELD = new ParseField("index");

    public ReindexDataStreamAction() {
        super(NAME);
    }

    public enum Mode {
        UPGRADE
    }

    public static class ReindexDataStreamRequest extends LegacyActionRequest implements IndicesRequest, ToXContent {
        private final Mode mode;
        private final String sourceDataStream;

        public ReindexDataStreamRequest(Mode mode, String sourceDataStream) {
            this.mode = mode;
            this.sourceDataStream = sourceDataStream;
        }

        public ReindexDataStreamRequest(StreamInput in) throws IOException {
            super(in);
            this.mode = Mode.valueOf(in.readString());
            this.sourceDataStream = in.readString();
        }

        private static final ConstructingObjectParser<ReindexDataStreamRequest, Predicate<NodeFeature>> PARSER =
            new ConstructingObjectParser<>("migration_reindex", objects -> {
                Mode mode = Mode.valueOf(((String) objects[0]).toUpperCase(Locale.ROOT));
                String source = (String) objects[1];
                return new ReindexDataStreamRequest(mode, source);
            });

        private static final ConstructingObjectParser<String, Void> SOURCE_PARSER = new ConstructingObjectParser<>(
            SOURCE_FIELD.getPreferredName(),
            false,
            (a, id) -> (String) a[0]
        );

        static {
            SOURCE_PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), MODE_FIELD);
            PARSER.declareObject(
                ConstructingObjectParser.constructorArg(),
                (parser, id) -> SOURCE_PARSER.apply(parser, null),
                SOURCE_FIELD
            );
        }

        public static ReindexDataStreamRequest fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(mode.name());
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

        public Mode getMode() {
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
            builder.field(MODE_FIELD.getPreferredName(), mode);
            builder.startObject(SOURCE_FIELD.getPreferredName());
            builder.field(INDEX_FIELD.getPreferredName(), sourceDataStream);
            builder.endObject();
            return builder;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "reindexing data stream " + sourceDataStream;
        }
    }
}
