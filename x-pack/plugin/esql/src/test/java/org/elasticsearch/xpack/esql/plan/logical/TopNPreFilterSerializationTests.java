/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.expression.function.FieldAttributeTestUtils.createFieldAttribute;

public class TopNPreFilterSerializationTests extends AbstractLogicalPlanSerializationTests<TopNPreFilter> {

    @Override
    protected TopNPreFilter createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(between(0, 2));
        Attribute key = createFieldAttribute(0, false);
        Expression limit = new Literal(randomSource(), between(1, 1000), DataType.INTEGER);
        return new TopNPreFilter(source, child, key, limit, randomBoolean(), randomBoolean());
    }

    @Override
    protected TopNPreFilter mutateInstance(TopNPreFilter instance) throws IOException {
        LogicalPlan child = instance.child();
        Attribute key = instance.key();
        Expression limit = instance.limit();
        boolean asc = instance.asc();
        boolean nullsFirst = instance.nullsFirst();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> key = randomValueOtherThan(key, () -> createFieldAttribute(0, false));
            case 2 -> limit = randomValueOtherThan(limit, () -> new Literal(randomSource(), between(1, 1000), DataType.INTEGER));
            case 3 -> asc = asc == false;
            case 4 -> nullsFirst = nullsFirst == false;
            default -> throw new UnsupportedOperationException();
        }
        return new TopNPreFilter(instance.source(), child, key, limit, asc, nullsFirst);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
