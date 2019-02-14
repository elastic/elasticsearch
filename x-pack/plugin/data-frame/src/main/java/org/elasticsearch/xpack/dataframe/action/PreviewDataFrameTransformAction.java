/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

        public Request() {

        }

        public static Request fromXContent(final XContentParser parser) throws IOException {
            return new Request(DataFrameTransformConfig.fromXContent(parser, null, false));
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
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
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
                out.writeMap(doc);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("data_frame_preview", docs);
            builder.endObject();
            return builder;
        }
    }
}
