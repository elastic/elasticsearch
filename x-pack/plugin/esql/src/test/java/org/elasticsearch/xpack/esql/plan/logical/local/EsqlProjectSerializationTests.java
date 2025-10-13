/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;

public class EsqlProjectSerializationTests extends AbstractLogicalPlanSerializationTests<EsqlProject> {
    @Override
    protected EsqlProject createTestInstance() {
        LogicalPlan child = randomChild(0);
        List<Attribute> projections = randomFieldAttributes(1, 10, false);
        return new EsqlProject(randomSource(), child, projections);
    }

    @Override
    protected EsqlProject mutateInstance(EsqlProject instance) throws IOException {
        LogicalPlan child = instance.child();
        List<? extends NamedExpression> projections = instance.projections();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            projections = randomValueOtherThan(projections, () -> randomFieldAttributes(1, 10, false));
        }
        return new EsqlProject(instance.source(), child, projections);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
