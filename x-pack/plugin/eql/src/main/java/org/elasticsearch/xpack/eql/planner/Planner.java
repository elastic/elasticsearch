/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

public class Planner {

    public PhysicalPlan plan(LogicalPlan plan) {
        throw new UnsupportedOperationException();
    }
}
