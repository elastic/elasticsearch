/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.action;


import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class GetRollupIndexCapsAction extends Action<GetRollupIndexCapsAction.Response> {

    public static final GetRollupIndexCapsAction INSTANCE = new GetRollupIndexCapsAction();
    public static final String NAME = "indices:data/xpack/rollup/get_index/caps";
    public static final ParseField CONFIG = new ParseField("config");
    public static final ParseField STATUS = new ParseField("status");
    private static final ParseField INDICES_OPTIONS = new ParseField("indices_options");

    private GetRollupIndexCapsAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContent {
        private String indexPattern;
        private IndicesOptions options;

        public Request(String indexPattern) {
            this(indexPattern, IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED);
        }

        public Request(String indexPattern, IndicesOptions options) {
            if (Strings.isNullOrEmpty(indexPattern) || indexPattern.equals("*")) {
                this.indexPattern = MetaData.ALL;
            } else {
                this.indexPattern = indexPattern;
            }
            this.options = options;
        }

        public Request() {}

        public String getIndexPattern() {
            return indexPattern;
        }

        public IndicesOptions getOptions() {
            return options;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.indexPattern = in.readString();
            this.options = IndicesOptions.readIndicesOptions(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(indexPattern);
            options.writeIndicesOptions(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(RollupField.ID.getPreferredName(), indexPattern);
            builder.field(INDICES_OPTIONS.getPreferredName(), options);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexPattern, options);
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
            return Objects.equals(indexPattern, other.indexPattern)
                && Objects.equals(options, other.options);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, GetRollupIndexCapsAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements Writeable, ToXContentObject {

        private Map<String, RollableIndexCaps> jobs = Collections.emptyMap();

        public Response() {

        }

        public Response(Map<String, RollableIndexCaps> jobs) {
            this.jobs = Objects.requireNonNull(jobs);
        }

        Response(StreamInput in) throws IOException {
            jobs = in.readMap(StreamInput::readString, RollableIndexCaps::new);
        }

        public Map<String, RollableIndexCaps> getJobs() {
            return jobs;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(jobs, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (Map.Entry<String, RollableIndexCaps> entry : jobs.entrySet()) {
                entry.getValue().toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobs);
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
            return Objects.equals(jobs, other.jobs);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
