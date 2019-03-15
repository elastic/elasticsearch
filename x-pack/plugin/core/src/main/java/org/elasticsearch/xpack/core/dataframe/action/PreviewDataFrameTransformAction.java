/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
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
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PreviewDataFrameTransformAction extends Action<PreviewDataFrameTransformAction.Response> {

    public static final PreviewDataFrameTransformAction INSTANCE = new PreviewDataFrameTransformAction();
    public static final String NAME = "cluster:admin/data_frame/preview";

    private PreviewDataFrameTransformAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private DataFrameTransformConfig config;

        public Request(DataFrameTransformConfig config) {
            this.setConfig(config);
        }

        public Request() { }

        public static Request fromXContent(final XContentParser parser) throws IOException {
            Map<String, Object> content = parser.map();
            // Destination and ID are not required for Preview, so we just supply our own
            content.put(DataFrameField.DESTINATION.getPreferredName(), "unused-transform-preview-index");
            try(XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(content);
                XContentParser newParser = XContentType.JSON
                    .xContent()
                    .createParser(parser.getXContentRegistry(),
                        LoggingDeprecationHandler.INSTANCE,
                        BytesReference.bytes(xContentBuilder).streamInput())) {
                return new Request(DataFrameTransformConfig.fromXContent(newParser, "transform-preview", false));
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return this.config.toXContent(builder, params);
        }

        public DataFrameTransformConfig getConfig() {
            return config;
        }

        public void setConfig(DataFrameTransformConfig config) {
            this.config = config;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.config = new DataFrameTransformConfig(in);
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

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, PreviewDataFrameTransformAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private List<Map<String, Object>> docs;
        public static ParseField PREVIEW = new ParseField("preview");

        static ObjectParser<Response, Void> PARSER = new ObjectParser<>("data_frame_transform_preview", Response::new);
        static {
            PARSER.declareObjectArray(Response::setDocs, (p, c) -> p.mapOrdered(), PREVIEW);
        }
        public Response() {}

        public Response(StreamInput in) throws IOException {
            int size = in.readInt();
            this.docs = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                this.docs.add(in.readMap());
            }
        }

        public Response(List<Map<String, Object>> docs) {
            this.docs = new ArrayList<>(docs);
        }

        public void setDocs(List<Map<String, Object>> docs) {
            this.docs = new ArrayList<>(docs);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int size = in.readInt();
            this.docs = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                this.docs.add(in.readMap());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(docs.size());
            for (Map<String, Object> doc : docs) {
                out.writeMapWithConsistentOrder(doc);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PREVIEW.getPreferredName(), docs);
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
            return Objects.equals(other.docs, docs);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(docs);
        }

        public static Response fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
