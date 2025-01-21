/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.migrate.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
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

    public static class Request extends ActionRequest implements IndicesRequest, ToXContent {
        private final String sourceIndex;
        private final String destIndex;
        private Settings settingsOverride = Settings.EMPTY;
        private Map<String, Object> mappingsOverride = Map.of();
        private static final ParseField SETTINGS_OVERRIDE_FIELD = new ParseField("settings_override");
        private static final ParseField MAPPINGS_OVERRIDE_FIELD = new ParseField("mappings_override");
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
        }

        public Request(String sourceIndex, String destIndex) {
            this(sourceIndex, destIndex, Settings.EMPTY, Map.of());
        }

        public Request(String sourceIndex, String destIndex, Settings settingsOverride, Map<String, Object> mappingsOverride) {
            Objects.requireNonNull(settingsOverride);
            Objects.requireNonNull(mappingsOverride);
            this.sourceIndex = sourceIndex;
            this.destIndex = destIndex;
            this.settingsOverride = settingsOverride;
            this.mappingsOverride = mappingsOverride;
        }

        @SuppressWarnings("unchecked")
        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
            this.destIndex = in.readString();
            this.settingsOverride = Settings.readSettingsFromStream(in);
            this.mappingsOverride = (Map<String, Object>) in.readGenericValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeString(destIndex);
            settingsOverride.writeTo(out);
            out.writeGenericValue(mappingsOverride);
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

        public void settingsOverride(Settings settingsOverride) {
            this.settingsOverride = settingsOverride;
        }

        public void mappingsOverride(Map<String, Object> mappingsOverride) {
            this.mappingsOverride = mappingsOverride;
        }

        public void fromXContent(XContentParser parser) throws IOException {
            PARSER.parse(parser, this, null);
        }

        /*
         * This only exists for the sake of testing the xcontent parser
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            if (mappingsOverride.containsKey("_doc")) {
                builder.field(MAPPINGS_OVERRIDE_FIELD.getPreferredName(), mappingsOverride.get("_doc"));
            }

            if (settingsOverride.isEmpty() == false) {
                builder.startObject(SETTINGS_OVERRIDE_FIELD.getPreferredName());
                settingsOverride.toXContent(builder, params);
                builder.endObject();
            }

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
                && Objects.equals(mappingsOverride, request.mappingsOverride);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, destIndex, settingsOverride, mappingsOverride);
        }

        @Override
        public String[] indices() {
            return new String[] { sourceIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }
}
