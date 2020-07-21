/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class MlInfoAction extends ActionType<MlInfoAction.Response> {

    public static final MlInfoAction INSTANCE = new MlInfoAction();
    public static final String NAME = "cluster:monitor/xpack/ml/info/get";

    private MlInfoAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        public Request() {
            super();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private Map<String, Object> info;

        public Response(Map<String, Object> info) {
            this.info = info;
        }

        public Response() {
            this.info = Collections.emptyMap();
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            info = in.readMap();
        }

        public Map<String, Object> getInfo() {
            return info;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(info);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.map(info);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(info);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(info, other.info);
        }
    }
}
