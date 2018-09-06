/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class GetLifecycleAction extends Action<GetLifecycleAction.Response> {
    public static final GetLifecycleAction INSTANCE = new GetLifecycleAction();
    public static final String NAME = "cluster:admin/ilm/get";

    protected GetLifecycleAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private Map<LifecyclePolicy, Tuple<Long, String>> policiesToMetadata;

        public Response() {
        }

        public Response(Map<LifecyclePolicy, Tuple<Long, String>> policiesToMetadata) {
            this.policiesToMetadata = policiesToMetadata;
        }

        public Map<LifecyclePolicy, Tuple<Long, String>> getPoliciesToMetadata() {
            return policiesToMetadata;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (Map.Entry<LifecyclePolicy, Tuple<Long, String>> entry : policiesToMetadata.entrySet()) {
                builder.startObject(entry.getKey().getName());
                builder.field("version", entry.getValue().v1());
                builder.field("creation_date", entry.getValue().v2());
                builder.field("policy", entry.getKey());
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            policiesToMetadata = in.readMap(LifecyclePolicy::new, (inn) -> new Tuple<>(inn.readVLong(), inn.readString()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(policiesToMetadata, (o, policy) -> policy.writeTo(o), (o, tuple) -> {
                o.writeVLong(tuple.v1());
                o.writeString(tuple.v2());
            });
        }

        @Override
        public int hashCode() {
            return Objects.hash(policiesToMetadata);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(policiesToMetadata, other.policiesToMetadata);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

    }

    public static class Request extends AcknowledgedRequest<Request> {
        private String[] policyNames;

        public Request(String... policyNames) {
            if (policyNames == null) {
                throw new IllegalArgumentException("ids cannot be null");
            }
            this.policyNames = policyNames;
        }

        public Request() {
            policyNames = Strings.EMPTY_ARRAY;
        }

        public String[] getPolicyNames() {
            return policyNames;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            policyNames = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(policyNames);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(policyNames);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Arrays.equals(policyNames, other.policyNames);
        }

    }

}
