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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class CreateIndexFromSourceAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "indices:admin/index/create_from_source";

    public static final ActionType<AcknowledgedResponse> INSTANCE = new CreateIndexFromSourceAction();

    private CreateIndexFromSourceAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest implements IndicesRequest, ToXContent {
        private final String sourceIndex;
        private final String destIndex;
        private Settings settingsOverride;
        private Map<String, Object> mappingsOverride;
        private boolean removeIndexBlocks;
        private static final ParseField SETTINGS_OVERRIDE_FIELD = new ParseField("settings_override");
        private static final ParseField MAPPINGS_OVERRIDE_FIELD = new ParseField("mappings_override");
        private static final ParseField REMOVE_INDEX_BLOCKS_FIELD = new ParseField("remove_index_blocks");
        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>("create_index_from_source_request");

        static {
            PARSER.declareField(
                (parser, request, context) -> request.settingsOverride(Settings.fromXContent(parser)),
                SETTINGS_OVERRIDE_FIELD,
                ObjectParser.ValueType.OBJECT
            );
            PARSER.declareField(
                (parser, request, context) -> request.mappingsOverride(Map.of("_doc", parser.map())),
                MAPPINGS_OVERRIDE_FIELD,
                ObjectParser.ValueType.OBJECT
            );
            PARSER.declareField(
                (parser, request, context) -> request.removeIndexBlocks(parser.booleanValue()),
                REMOVE_INDEX_BLOCKS_FIELD,
                ObjectParser.ValueType.BOOLEAN
            );
        }

        public Request(String sourceIndex, String destIndex) {
            this(sourceIndex, destIndex, Settings.EMPTY, Map.of(), true);
        }

        public Request(
            String sourceIndex,
            String destIndex,
            Settings settingsOverride,
            Map<String, Object> mappingsOverride,
            boolean removeIndexBlocks
        ) {
            Objects.requireNonNull(settingsOverride);
            Objects.requireNonNull(mappingsOverride);
            this.sourceIndex = sourceIndex;
            this.destIndex = destIndex;
            this.settingsOverride = settingsOverride;
            this.mappingsOverride = mappingsOverride;
            this.removeIndexBlocks = removeIndexBlocks;
        }

        @SuppressWarnings("unchecked")
        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
            this.destIndex = in.readString();
            this.settingsOverride = Settings.readSettingsFromStream(in);
            this.mappingsOverride = (Map<String, Object>) in.readGenericValue();
            this.removeIndexBlocks = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeString(destIndex);
            settingsOverride.writeTo(out);
            out.writeGenericValue(mappingsOverride);
            out.writeBoolean(removeIndexBlocks);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String sourceIndex() {
            return sourceIndex;
        }

        public String destIndex() {
            return destIndex;
        }

        public Settings settingsOverride() {
            return settingsOverride;
        }

        public Map<String, Object> mappingsOverride() {
            return mappingsOverride;
        }

        public boolean removeIndexBlocks() {
            return removeIndexBlocks;
        }

        public void settingsOverride(Settings settingsOverride) {
            this.settingsOverride = settingsOverride;
        }

        public void mappingsOverride(Map<String, Object> mappingsOverride) {
            this.mappingsOverride = mappingsOverride;
        }

        public void removeIndexBlocks(boolean removeIndexBlocks) {
            this.removeIndexBlocks = removeIndexBlocks;
        }

        public void fromXContent(XContentParser parser) throws IOException {
            PARSER.parse(parser, this, null);
        }

        /*
         * This only exists for the sake of testing the xcontent parser
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (mappingsOverride.containsKey("_doc")) {
                builder.field(MAPPINGS_OVERRIDE_FIELD.getPreferredName(), mappingsOverride.get("_doc"));
            }

            if (settingsOverride.isEmpty() == false) {
                builder.startObject(SETTINGS_OVERRIDE_FIELD.getPreferredName());
                settingsOverride.toXContent(builder, params);
                builder.endObject();
            }
            builder.field(REMOVE_INDEX_BLOCKS_FIELD.getPreferredName(), removeIndexBlocks);

            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(sourceIndex, request.sourceIndex)
                && Objects.equals(destIndex, request.destIndex)
                && Objects.equals(settingsOverride, request.settingsOverride)
                && Objects.equals(mappingsOverride, request.mappingsOverride)
                && removeIndexBlocks == request.removeIndexBlocks;

        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, destIndex, settingsOverride, mappingsOverride, removeIndexBlocks);
        }

        @Override
        public String[] indices() {
            return new String[] { sourceIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "creating index " + destIndex + " from " + sourceIndex;
        }
    }
}
