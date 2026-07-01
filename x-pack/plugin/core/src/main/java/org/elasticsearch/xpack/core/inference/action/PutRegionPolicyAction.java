/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PutRegionPolicyAction extends ActionType<RegionPolicyResponse> {

    public static final PutRegionPolicyAction INSTANCE = new PutRegionPolicyAction();
    public static final String NAME = "cluster:admin/xpack/inference/region_policy/put";

    public PutRegionPolicyAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField REGION_POLICY_FIELD = new ParseField("region_policy");

        public static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "put_region_policy_request",
            false,
            args -> new Request((RegionPolicy) args[0])
        );

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), RegionPolicy.STRICT_PARSER, REGION_POLICY_FIELD);
        }

        public static Request parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final RegionPolicy regionPolicy;

        public Request(RegionPolicy regionPolicy) {
            this.regionPolicy = Objects.requireNonNull(regionPolicy);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            regionPolicy = new RegionPolicy(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException exception = null;
            exception = validateNonNullListIsNotEmpty(regionPolicy.allowedRegions(), RegionPolicy.ALLOWED_REGIONS_FIELD, exception);
            exception = validateNonNullListIsNotEmpty(regionPolicy.allowedGeos(), RegionPolicy.ALLOWED_GEOS_FIELD, exception);
            return exception;
        }

        private static ActionRequestValidationException validateNonNullListIsNotEmpty(
            List<?> list,
            ParseField regionPolicyField,
            ActionRequestValidationException exception
        ) {
            if (list != null && list.isEmpty()) {
                return ValidateActions.addValidationError(
                    Strings.format(
                        "[%s.%s] must not be empty",
                        REGION_POLICY_FIELD.getPreferredName(),
                        regionPolicyField.getPreferredName()
                    ),
                    exception
                );
            }
            return exception;
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
    }
}
