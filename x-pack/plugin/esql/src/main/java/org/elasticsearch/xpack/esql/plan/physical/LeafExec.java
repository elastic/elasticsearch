/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Collections;
import java.util.List;

public abstract class LeafExec extends PhysicalPlan {

    protected LeafExec(Source source) {
        super(source, Collections.emptyList());
    }

    @Override
    public final LeafExec replaceChildren(List<PhysicalPlan> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }
}
