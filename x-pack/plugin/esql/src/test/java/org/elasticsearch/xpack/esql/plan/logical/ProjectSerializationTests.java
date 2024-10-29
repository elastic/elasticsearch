/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public class ProjectSerializationTests extends AbstractLogicalPlanSerializationTests<Project> {
    @Override
    protected Project createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        List<? extends NamedExpression> projections = randomFieldAttributes(0, 10, false);
        return new Project(source, child, projections);
    }

    @Override
    protected Project mutateInstance(Project instance) throws IOException {
        LogicalPlan child = instance.child();
        List<? extends NamedExpression> projections = instance.projections();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            projections = randomValueOtherThan(projections, () -> randomFieldAttributes(0, 10, false));
        }
        return new Project(instance.source(), child, projections);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
