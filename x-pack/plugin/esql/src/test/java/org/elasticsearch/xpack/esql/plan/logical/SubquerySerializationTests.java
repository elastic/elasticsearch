/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class SubquerySerializationTests extends AbstractLogicalPlanSerializationTests<Subquery> {
    @Override
    protected Subquery createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        return new Subquery(source, child);
    }

    @Override
    protected Subquery mutateInstance(Subquery instance) throws IOException {
        LogicalPlan child = instance.child();
        child = randomValueOtherThan(child, () -> randomChild(0));
        return new Subquery(instance.source(), child);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
