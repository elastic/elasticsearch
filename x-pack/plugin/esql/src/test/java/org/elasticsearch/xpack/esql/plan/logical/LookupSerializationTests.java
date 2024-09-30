/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationSerializationTests;

import java.io.IOException;
import java.util.List;

public class LookupSerializationTests extends AbstractLogicalPlanSerializationTests<Lookup> {
    public static Lookup randomLookup(int depth) {
        Source source = randomSource();
        LogicalPlan child = randomChild(depth);
        Expression tableName = AbstractExpressionSerializationTests.randomChild();
        List<Attribute> matchFields = randomFieldAttributes(1, 10, false);
        LocalRelation localRelation = randomBoolean() ? null : LocalRelationSerializationTests.randomLocalRelation();
        return new Lookup(source, child, tableName, matchFields, localRelation);
    }

    @Override
    protected Lookup createTestInstance() {
        return randomLookup(0);
    }

    @Override
    protected Lookup mutateInstance(Lookup instance) throws IOException {
        Source source = instance.source();
        LogicalPlan child = instance.child();
        Expression tableName = instance.tableName();
        List<Attribute> matchFields = instance.matchFields();
        LocalRelation localRelation = instance.localRelation();
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> tableName = randomValueOtherThan(tableName, AbstractExpressionSerializationTests::randomChild);
            case 2 -> matchFields = randomValueOtherThan(matchFields, () -> randomFieldAttributes(1, 10, false));
            case 3 -> localRelation = randomValueOtherThan(
                localRelation,
                () -> randomBoolean() ? null : LocalRelationSerializationTests.randomLocalRelation()
            );
        }
        return new Lookup(source, child, tableName, matchFields, localRelation);
    }
}
