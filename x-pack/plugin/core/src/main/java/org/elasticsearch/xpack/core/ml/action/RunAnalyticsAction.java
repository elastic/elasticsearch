/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class RunAnalyticsAction extends Action<AcknowledgedResponse> {

    public static final RunAnalyticsAction INSTANCE = new RunAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/dataframe/run";

    private RunAnalyticsAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private String index;

        public Request(String index) {
            this.index = index;
        }

        public Request(StreamInput in) throws IOException {
            readFrom(in);
        }

        public Request() {
        }

        public String getIndex() {
            return index;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("index", index);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            RunAnalyticsAction.Request other = (RunAnalyticsAction.Request) obj;
            return Objects.equals(index, other.index);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse> {

        RequestBuilder(ElasticsearchClient client, RunAnalyticsAction action) {
            super(client, action, new Request());
        }
    }

}
