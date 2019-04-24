/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ListEnrichPolicyAction extends Action<ListEnrichPolicyAction.Response> {

    public static final ListEnrichPolicyAction INSTANCE = new ListEnrichPolicyAction();
    public static final String NAME = "cluster:admin/xpack/enrich/list";

    protected ListEnrichPolicyAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }
    public static class Request extends MasterNodeRequest<ListEnrichPolicyAction.Request> {

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Map<String, EnrichPolicy> policyMap;

        public Response(Map<String, EnrichPolicy> policyMap) {
            this.policyMap = Objects.requireNonNull(policyMap, "policy cannot be null");
        }

        public Response(StreamInput in) throws IOException {
            policyMap = in.readMap(StreamInput::readString, EnrichPolicy::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(policyMap, StreamOutput::writeString, (o, value) -> value.writeTo(out));

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.startArray("policies");
                {
                    for (Map.Entry<String, EnrichPolicy> entry : policyMap.entrySet()) {
                        builder.startObject();
                        {
                            entry.getValue().toXContent(builder, params);
                        }
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();

            return builder;
        }

        public Map<String, EnrichPolicy> getPolicyMap() {
            return policyMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return policyMap.equals(response.policyMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policyMap);
        }
    }
}
