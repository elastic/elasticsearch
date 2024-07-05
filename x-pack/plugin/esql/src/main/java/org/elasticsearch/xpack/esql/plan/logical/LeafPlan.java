/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Collections;
import java.util.List;

public abstract class LeafPlan extends LogicalPlan {

    protected LeafPlan(Source source) {
        super(source, Collections.emptyList());
    }

    @Override
    public final LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }
}
