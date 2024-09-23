/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public class ProjectExecSerializationTests extends AbstractPhysicalPlanSerializationTests<ProjectExec> {
    public static ProjectExec randomProjectExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        List<? extends NamedExpression> projections = randomFieldAttributes(0, 10, false);
        return new ProjectExec(source, child, projections);
    }

    @Override
    protected ProjectExec createTestInstance() {
        return randomProjectExec(0);
    }

    @Override
    protected ProjectExec mutateInstance(ProjectExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        List<? extends NamedExpression> projections = instance.projections();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            projections = randomValueOtherThan(projections, () -> randomFieldAttributes(0, 10, false));
        }
        return new ProjectExec(instance.source(), child, projections);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
