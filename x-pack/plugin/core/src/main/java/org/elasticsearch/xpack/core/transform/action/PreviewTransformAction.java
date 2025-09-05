/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class PreviewTransformAction extends ActionType<PreviewTransformAction.Response> {

    public static final PreviewTransformAction INSTANCE = new PreviewTransformAction();
    public static final String NAME = "cluster:admin/transform/preview";

    public static final String DUMMY_DEST_INDEX_FOR_PREVIEW = "unused-transform-preview-index";

    private PreviewTransformAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private final TransformConfig config;

        public Request(TransformConfig config, TimeValue timeout) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, timeout);
            this.config = config;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new TransformConfig(in);
        }

        public static Request fromXContent(final XContentParser parser, TimeValue timeout) throws IOException {
            Map<String, Object> content = parser.map();
            // dest.index is not required for _preview, so we just supply our own
            Map<String, String> tempDestination = new HashMap<>();
            tempDestination.put(DestConfig.INDEX.getPreferredName(), DUMMY_DEST_INDEX_FOR_PREVIEW);
            // Users can still provide just dest.pipeline to preview what their data would look like given the pipeline ID
            Object providedDestination = content.get(TransformField.DESTINATION.getPreferredName());
            if (providedDestination instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, String> providedDestinationAsMap = (Map<String, String>) providedDestination;
                tempDestination.putAll(providedDestinationAsMap);
            }
            content.put(TransformField.DESTINATION.getPreferredName(), tempDestination);
            content.putIfAbsent(TransformField.ID.getPreferredName(), "transform-preview");
            try (
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(content);
                XContentParser newParser = XContentHelper.createParserNotCompressed(
                    LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG.withRegistry(parser.getXContentRegistry()),
                    BytesReference.bytes(xContentBuilder),
                    XContentType.JSON
                )
            ) {
                return new Request(TransformConfig.fromXContent(newParser, null, false), timeout);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            validationException = config.validate(validationException);
            validationException = SourceDestValidator.validateRequest(
                validationException,
                config.getDestination() != null ? config.getDestination().getIndex() : null
            );

            return validationException;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return this.config.toXContent(builder, params);
        }

        public TransformConfig getConfig() {
            return config;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.config.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(config, other.config);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("preview_transform[%s]", config.getId()), parentTaskId, headers);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField PREVIEW = new ParseField("preview");
        public static final ParseField GENERATED_DEST_INDEX_SETTINGS = new ParseField("generated_dest_index");

        private final List<Map<String, Object>> docs;
        private final TransformDestIndexSettings generatedDestIndexSettings;

        public Response(List<Map<String, Object>> docs, TransformDestIndexSettings generatedDestIndexSettings) {
            this.docs = docs;
            this.generatedDestIndexSettings = generatedDestIndexSettings;
        }

        public Response(StreamInput in) throws IOException {
            int size = in.readInt();
            this.docs = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                this.docs.add(in.readGenericMap());
            }
            this.generatedDestIndexSettings = new TransformDestIndexSettings(in);
        }

        public List<Map<String, Object>> getDocs() {
            return docs;
        }

        public TransformDestIndexSettings getGeneratedDestIndexSettings() {
            return generatedDestIndexSettings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(docs.size());
            for (Map<String, Object> doc : docs) {
                out.writeGenericMap(doc);
            }
            generatedDestIndexSettings.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PREVIEW.getPreferredName(), docs);
            builder.field(GENERATED_DEST_INDEX_SETTINGS.getPreferredName(), generatedDestIndexSettings);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }

            Response other = (Response) obj;
            return Objects.equals(other.docs, docs) && Objects.equals(other.generatedDestIndexSettings, generatedDestIndexSettings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(docs, generatedDestIndexSettings);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }
}
