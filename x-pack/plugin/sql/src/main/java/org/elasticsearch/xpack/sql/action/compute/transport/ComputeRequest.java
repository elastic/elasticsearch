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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.sql.action.compute.data.Page;
import org.elasticsearch.xpack.sql.action.compute.planner.PlanNode;

import java.util.function.Consumer;

public class ComputeRequest extends ActionRequest implements IndicesRequest {

    private final PlanNode plan;
    private final Consumer<Page> pageConsumer; // quick hack to stream responses back

    public ComputeRequest(StreamInput in) {
        throw new UnsupportedOperationException();
    }

    public ComputeRequest(PlanNode plan, Consumer<Page> pageConsumer) {
        super();
        this.plan = plan;
        this.pageConsumer = pageConsumer;
    }

    public static ComputeRequest fromXContent(XContentParser parser) {

        return new ComputeRequest(null);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public PlanNode plan() {
        return plan;
    }

    public Consumer<Page> getPageConsumer() {
        return pageConsumer;
    }

    @Override
    public String[] indices() {
        return plan.getIndices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.LENIENT_EXPAND_OPEN;
    }
}
