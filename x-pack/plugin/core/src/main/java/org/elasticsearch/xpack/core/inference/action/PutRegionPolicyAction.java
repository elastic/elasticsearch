/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;

import java.io.IOException;
import java.util.Objects;

public class PutRegionPolicyAction extends ActionType<RegionPolicyResponse> {

    public static final PutRegionPolicyAction INSTANCE = new PutRegionPolicyAction();
    public static final String NAME = "cluster:admin/xpack/inference/region_policy/put";

    public PutRegionPolicyAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static final ParseField REGION_POLICY_FIELD = new ParseField("region_policy");

        public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, Builder::new);

        static {
            PARSER.declareObject(Builder::setRegionPolicy, RegionPolicy.STRICT_PARSER, REGION_POLICY_FIELD);
        }

        public static Request parseRequest(XContentParser parser, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            Builder builder = PARSER.apply(parser, null);
            builder.setMasterNodeTimeout(masterNodeTimeout);
            builder.setAckTimeout(ackTimeout);
            return builder.build();
        }

        private final RegionPolicy regionPolicy;

        public Request(RegionPolicy regionPolicy, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
            this.regionPolicy = Objects.requireNonNull(regionPolicy);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            regionPolicy = new RegionPolicy(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            regionPolicy.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(REGION_POLICY_FIELD.getPreferredName(), regionPolicy);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(regionPolicy, request.regionPolicy);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(regionPolicy);
        }

        public RegionPolicy regionPolicy() {
            return regionPolicy;
        }

        private static class Builder {
            private RegionPolicy regionPolicy;
            private TimeValue masterNodeTimeout;
            private TimeValue ackTimeout;

            private void setRegionPolicy(RegionPolicy regionPolicy) {
                this.regionPolicy = regionPolicy;
            }

            private void setMasterNodeTimeout(TimeValue masterNodeTimeout) {
                this.masterNodeTimeout = masterNodeTimeout;
            }

            private void setAckTimeout(TimeValue ackTimeout) {
                this.ackTimeout = ackTimeout;
            }

            private Request build() {
                return new Request(regionPolicy, masterNodeTimeout, ackTimeout);
            }
        }
    }
}
