/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class PutFilterAction extends ActionType<PutFilterAction.Response> {

    public static final PutFilterAction INSTANCE = new PutFilterAction();
    public static final String NAME = "cluster:admin/xpack/ml/filters/put";

    private PutFilterAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest implements ToXContentObject {

        public static Request parseRequest(String filterId, XContentParser parser) {
            MlFilter.Builder filter = MlFilter.STRICT_PARSER.apply(parser, null);
            if (filter.getId() == null) {
                filter.setId(filterId);
            } else if (Strings.isNullOrEmpty(filterId) == false && filterId.equals(filter.getId()) == false) {
                // If we have both URI and body filter ID, they must be identical
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, MlFilter.ID.getPreferredName(), filter.getId(), filterId)
                );
            }
            return new Request(filter.build());
        }

        private final MlFilter filter;

        public Request(StreamInput in) throws IOException {
            super(in);
            filter = new MlFilter(in);
        }

        public Request(MlFilter filter) {
            this.filter = ExceptionsHelper.requireNonNull(filter, "filter");
        }

        public MlFilter getFilter() {
            return this.filter;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            filter.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            filter.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filter);
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
            return Objects.equals(filter, other.filter);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final MlFilter filter;

        Response(StreamInput in) throws IOException {
            super(in);
            filter = new MlFilter(in);
        }

        public Response(MlFilter filter) {
            this.filter = filter;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            filter.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return filter.toXContent(builder, params);
        }

        public MlFilter getFilter() {
            return filter;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filter);
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
            return Objects.equals(filter, other.filter);
        }
    }
}
