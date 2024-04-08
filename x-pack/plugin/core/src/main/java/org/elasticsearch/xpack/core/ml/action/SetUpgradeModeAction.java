/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SetUpgradeModeAction extends ActionType<AcknowledgedResponse> {

    public static final SetUpgradeModeAction INSTANCE = new SetUpgradeModeAction();
    public static final String NAME = "cluster:admin/xpack/ml/upgrade_mode";

    private SetUpgradeModeAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private final boolean enabled;

        private static final ParseField ENABLED = new ParseField("enabled");
        public static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            a -> new Request((Boolean) a[0])
        );

        static {
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED);
        }

        public Request(boolean enabled) {
            this.enabled = enabled;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.enabled = in.readBoolean();
        }

        public boolean isEnabled() {
            return enabled;
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
}
