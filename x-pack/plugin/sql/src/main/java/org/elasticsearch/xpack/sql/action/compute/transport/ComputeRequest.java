/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.transport;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;
import org.elasticsearch.xpack.sql.action.compute.planner.PlanNode;

import java.io.IOException;

public class ComputeRequest extends ActionRequest implements IndicesRequest, ToXContentObject {

    private final PlanNode plan;

    public ComputeRequest(StreamInput in) {
        throw new UnsupportedOperationException();
    }

    public ComputeRequest(PlanNode plan) {
        super();
        this.plan = plan;
    }

    public static final ParseField PLAN_FIELD = new ParseField("plan");

    static final ConstructingObjectParser<ComputeRequest, Void> PARSER = new ConstructingObjectParser<>(
        "compute_request",
        args -> new ComputeRequest((PlanNode) args[0])
    );

    static {
        PARSER.declareNamedObject(ConstructingObjectParser.constructorArg(), (p, c, n) -> p.namedObject(PlanNode.class, n, c), PLAN_FIELD);
    }

    public static ComputeRequest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public PlanNode plan() {
        return plan;
    }

    @Override
    public String[] indices() {
        return plan.getIndices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.LENIENT_EXPAND_OPEN;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        NamedXContentObjectHelper.writeNamedObject(builder, params, PLAN_FIELD.getPreferredName(), plan);
        builder.endObject();
        return builder;
    }
}
