/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PreviewTransformAction extends ActionType<PreviewTransformAction.Response> {

    public static final PreviewTransformAction INSTANCE = new PreviewTransformAction();
    public static final String NAME = "cluster:admin/transform/preview";

    private PreviewTransformAction() {
        super(NAME, PreviewTransformAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private final TransformConfig config;

        public Request(TransformConfig config) {
            this.config = config;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new TransformConfig(in);
        }

        public static Request fromXContent(final XContentParser parser) throws IOException {
            Map<String, Object> content = parser.map();
            // dest.index and ID are not required for Preview, so we just supply our own
            Map<String, String> tempDestination = new HashMap<>();
            tempDestination.put(DestConfig.INDEX.getPreferredName(), "unused-transform-preview-index");
            // Users can still provide just dest.pipeline to preview what their data would look like given the pipeline ID
            Object providedDestination = content.get(TransformField.DESTINATION.getPreferredName());
            if (providedDestination instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, String> destMap = (Map<String, String>) providedDestination;
                String pipeline = destMap.get(DestConfig.PIPELINE.getPreferredName());
                if (pipeline != null) {
                    tempDestination.put(DestConfig.PIPELINE.getPreferredName(), pipeline);
                }
            }
            content.put(TransformField.DESTINATION.getPreferredName(), tempDestination);
            content.put(TransformField.ID.getPreferredName(), "transform-preview");
            try (
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(content);
                XContentParser newParser = XContentType.JSON.xContent()
                    .createParser(
                        parser.getXContentRegistry(),
                        LoggingDeprecationHandler.INSTANCE,
                        BytesReference.bytes(xContentBuilder).streamInput()
                    )
            ) {
                return new Request(TransformConfig.fromXContent(newParser, "transform-preview", false));
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (config.getPivotConfig() != null) {
                for (String failure : config.getPivotConfig().aggFieldValidation()) {
                    validationException = addValidationError(failure, validationException);
                }
            }

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
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private List<Map<String, Object>> docs;
        private Map<String, Object> mappings;
        public static ParseField PREVIEW = new ParseField("preview");
        public static ParseField MAPPINGS = new ParseField("mappings");

        static final ObjectParser<Response, Void> PARSER = new ObjectParser<>("data_frame_transform_preview", Response::new);
        static {
            PARSER.declareObjectArray(Response::setDocs, (p, c) -> p.mapOrdered(), PREVIEW);
            PARSER.declareObject(Response::setMappings, (p, c) -> p.mapOrdered(), MAPPINGS);
        }

        public Response() {}

        public Response(StreamInput in) throws IOException {
            int size = in.readInt();
            this.docs = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                this.docs.add(in.readMap());
            }
            if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
                Map<String, Object> objectMap = in.readMap();
                this.mappings = objectMap == null ? null : Map.copyOf(objectMap);
            }
        }

        public Response(List<Map<String, Object>> docs) {
            this.docs = new ArrayList<>(docs);
        }

        public void setDocs(List<Map<String, Object>> docs) {
            this.docs = new ArrayList<>(docs);
        }

        public void setMappings(Map<String, Object> mappings) {
            this.mappings = Map.copyOf(mappings);
        }

        /**
         * This takes the a {@code Map<String, String>} of the type "fieldname: fieldtype" and transforms it into the
         * typical mapping format.
         *
         * Example:
         *
         * input:
         * {"field1.subField1": "long", "field2": "keyword"}
         *
         * output:
         * {
         *     "properties": {
         *         "field1.subField1": {
         *             "type": "long"
         *         },
         *         "field2": {
         *             "type": "keyword"
         *         }
         *     }
         * }
         * @param mappings A Map of the form {"fieldName": "fieldType"}
         */
        public void setMappingsFromStringMap(Map<String, String> mappings) {
            Map<String, Object> fieldMappings = new HashMap<>();
            mappings.forEach((k, v) -> fieldMappings.put(k, Map.of("type", v)));
            this.mappings = Map.of("properties", fieldMappings);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(docs.size());
            for (Map<String, Object> doc : docs) {
                out.writeMapWithConsistentOrder(doc);
            }
            if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
                out.writeMap(mappings);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PREVIEW.getPreferredName(), docs);
            if (mappings != null) {
                builder.field(MAPPINGS.getPreferredName(), mappings);
            }
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
            return Objects.equals(other.docs, docs) && Objects.equals(other.mappings, mappings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(docs, mappings);
        }

        public static Response fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
