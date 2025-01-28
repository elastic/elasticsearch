/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper.preprocessor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;

/**
 * Interface for a LogicalPlan processing rule occurring after the optimization, but before mapping to a physical plan.
 * This step occurs on the coordinator. The rule may use services provided to the transport action and thus can resolve indices, rewrite
 * queries, perform substitutions, etc.
 * Note that the LogicalPlan following the rules' changes will not undergo another logical optimization round. The changes these rules
 * should apply are only those that require access to services that need to be performed asynchronously.
 */
public interface MappingPreProcessor {

    /**
     * Process a logical plan making use of the available services and provide the updated plan to the provided listener.
     * @param plan the logical plan to process
     * @param services the services available from the transport action
     * @param listener the listener to notify when processing is complete
     */
    void preprocess(LogicalPlan plan, TransportActionServices services, ActionListener<LogicalPlan> listener);

    interface MappingPreProcessorSupplier {
        MappingPreProcessor mappingPreProcessor();
    }
}
