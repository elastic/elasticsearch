/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.function.BiConsumer;

/**
 * Interface implemented by expressions or plans that require validation post logical optimization,
 * when the plan and references have been not just resolved but also replaced.
 * The interface is similar to {@link PostOptimizationVerificationAware}, but focused on the tree structure
 */
public interface PostOptimizationPlanVerificationAware {

    /**
     * Allows the implementer to return a consumer that will perform self-validation in the context of the tree structure the implementer
     * is part of. This usually involves checking the type and configuration of the children or that of the parent.
     */
    BiConsumer<LogicalPlan, Failures> postOptimizationVerification();
}
