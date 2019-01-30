/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.xpack.sql.tree.Source;

abstract class LeafExec extends PhysicalPlan {
    LeafExec(Source source) {
        super(source, Collections.emptyList());
    }

    @Override
    public final LeafExec replaceChildren(List<PhysicalPlan> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }
}
