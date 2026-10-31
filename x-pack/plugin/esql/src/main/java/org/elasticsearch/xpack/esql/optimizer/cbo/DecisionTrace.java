/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import java.util.List;

/** A compact explanation of one decision made by the cost-based planner. */
public record DecisionTrace(String node, String chosen, List<String> alternatives, String reason) {
    public DecisionTrace {
        alternatives = List.copyOf(alternatives);
    }
}
