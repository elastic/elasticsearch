/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GetAutoFollowPatternAction extends ActionType<GetAutoFollowPatternAction.Response> {

    public static final String NAME = "cluster:admin/xpack/ccr/auto_follow_pattern/get";
    public static final GetAutoFollowPatternAction INSTANCE = new GetAutoFollowPatternAction();

    private GetAutoFollowPatternAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private String name;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readOptionalString();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(name, request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Map<String, AutoFollowPattern> autoFollowPatterns;

        public Response(Map<String, AutoFollowPattern> autoFollowPatterns) {
            this.autoFollowPatterns = autoFollowPatterns;
        }

        public Map<String, AutoFollowPattern> getAutoFollowPatterns() {
            return autoFollowPatterns;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            autoFollowPatterns = in.readMap(AutoFollowPattern::readFrom);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(autoFollowPatterns, StreamOutput::writeWriteable);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.startArray("patterns");
                for (Map.Entry<String, AutoFollowPattern> entry : autoFollowPatterns.entrySet()) {
                    builder.startObject();
                    {
                        builder.field("name", entry.getKey());
                        builder.startObject("pattern");
                        {
                            entry.getValue().toXContent(builder, params);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(autoFollowPatterns, response.autoFollowPatterns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(autoFollowPatterns);
        }
    }

}
