/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.action;


import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class GetRollupCapsAction extends Action<GetRollupCapsAction.Response> {

    public static final GetRollupCapsAction INSTANCE = new GetRollupCapsAction();
    public static final String NAME = "cluster:monitor/xpack/rollup/get/caps";
    public static final ParseField CONFIG = new ParseField("config");
    public static final ParseField STATUS = new ParseField("status");

    private GetRollupCapsAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContent {
        private String indexPattern;
        private KeyOrder key = KeyOrder.INDEX_PATTERN;

        public Request(String indexPattern, String key) {
            if (Strings.isNullOrEmpty(indexPattern) || indexPattern.equals("*")) {
                this.indexPattern = MetaData.ALL;
            } else {
                this.indexPattern = indexPattern;
            }
            this.key = KeyOrder.fromString(key);
        }

        public Request() {}

        public String getIndexPattern() {
            return indexPattern;
        }

        public KeyOrder getKey() {
            return key;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.indexPattern = in.readString();
            if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
                this.key = KeyOrder.fromStream(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(indexPattern);
            if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
                key.writeTo(out);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(RollupField.ID.getPreferredName(), indexPattern);
            builder.field(KeyOrder.KEY.getPreferredName(), key);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexPattern, key);
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
                && Objects.equals(key, other.key);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, GetRollupCapsAction action) {
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

    public enum KeyOrder implements Writeable {
        INDEX_PATTERN, ROLLUP_INDEX;

        public static final ParseField KEY = new ParseField("key");

        public static KeyOrder fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }

        public static KeyOrder fromStream(StreamInput in) throws IOException {
            return in.readEnum(KeyOrder.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            KeyOrder state = this;
            out.writeEnum(state);
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

}
