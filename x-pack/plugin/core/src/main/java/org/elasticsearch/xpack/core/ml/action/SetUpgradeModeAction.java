/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SetUpgradeModeAction extends Action<AcknowledgedResponse> {

    public static final SetUpgradeModeAction INSTANCE = new SetUpgradeModeAction();
    public static final String NAME = "cluster:admin/xpack/ml/upgrade_mode";

    private SetUpgradeModeAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return AcknowledgedResponse::new;
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private boolean enabled;

        private static final ParseField ENABLED = new ParseField("enabled");
        public static final ConstructingObjectParser<Request, Void> PARSER =
            new ConstructingObjectParser<>(NAME, a -> new Request((Boolean)a[0]));

        static {
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED);
        }

        public Request(boolean enabled) {
            this.enabled = enabled;
        }

        public Request(StreamInput in) throws IOException {
            readFrom(in);
        }

        public Request() {
        }

        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.enabled = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(enabled);
        }

        @Override
        public int hashCode() {
            return Objects.hash(enabled);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(enabled, other.enabled);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ENABLED.getPreferredName(), enabled);
            builder.endObject();
            return builder;
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse> {

        RequestBuilder(ElasticsearchClient client, SetUpgradeModeAction action) {
            super(client, action, new Request());
        }
    }

}
