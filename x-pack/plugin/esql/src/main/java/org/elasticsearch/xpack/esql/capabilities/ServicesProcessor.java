/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Hook for performing plan transformations based on (async) call to other services.
 *
 * @param <C> context to be passed to the execute method
 * @param <R> execution result
 */
public interface ServicesProcessor<C, R> {

    /**
     * Callback before the execution of the processor.
     *
     * @param plan current logical plan
     * @return information that will be passed to the execute method
     */
    C beforeExecution(LogicalPlan plan);

    /**
     * Execute async calls to other Elasticsearch services.
     *
     * @param services services available for invocation
     * @param c context passed from #beforeExecution
     * @param listener callback to be called when the execution is done, passing any potential result to #postExecution
     */
    void execute(Services services, C c, ActionListener<R> listener);

    /**
     * Callback after the execution of the processor.
     * @param plan
     * @param r result of the execution
     * @return the logical plan potentially transformed by the processor
     */
    default LogicalPlan afterExecution(LogicalPlan plan, R r) {
        return plan;
    }
}
