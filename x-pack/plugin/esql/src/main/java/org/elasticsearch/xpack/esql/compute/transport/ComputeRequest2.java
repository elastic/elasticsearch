/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.compute.transport;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.esql.plan.logical.EsQuery;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

@Experimental
public class ComputeRequest2 extends ActionRequest implements IndicesRequest {

    private final LogicalPlan plan;

    public ComputeRequest2(StreamInput in) {
        throw new UnsupportedOperationException();
    }

    public ComputeRequest2(LogicalPlan plan) {
        super();
        this.plan = plan;
    }

    public static final ParseField PLAN_FIELD = new ParseField("plan");

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public LogicalPlan plan() {
        return plan;
    }

    @Override
    public String[] indices() {
        return new String[] { ((EsQuery) plan.collect(l -> l instanceof EsQuery).get(0)).index().name() };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.LENIENT_EXPAND_OPEN;
    }
}
