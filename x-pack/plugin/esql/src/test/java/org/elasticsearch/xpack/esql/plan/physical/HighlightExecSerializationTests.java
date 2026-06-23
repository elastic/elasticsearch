/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;

public class HighlightExecSerializationTests extends AbstractPhysicalPlanSerializationTests<HighlightExec> {

    @Override
    protected HighlightExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        List<NamedExpression> fields = randomFields();
        return new HighlightExec(
            source,
            child,
            Highlight.DEFAULT_PREFIX,
            randomQuery(),
            fields,
            randomNonNullOptions(),
            generatedFor(fields)
        );
    }

    @Override
    protected HighlightExec mutateInstance(HighlightExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression query = instance.query();
        List<NamedExpression> fields = instance.fields();
        MapExpression options = instance.options();

        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> query = randomValueOtherThan(query, HighlightExecSerializationTests::randomQuery);
            case 2 -> fields = randomValueOtherThan(fields, HighlightExecSerializationTests::randomFields);
            case 3 -> options = randomValueOtherThan(options, HighlightExecSerializationTests::randomOptions);
        }
        return new HighlightExec(instance.source(), child, Highlight.DEFAULT_PREFIX, query, fields, options, generatedFor(fields));
    }

    private static List<NamedExpression> randomFields() {
        return randomList(1, 5, () -> randomReferenceAttribute(false));
    }

    private static List<Attribute> generatedFor(List<NamedExpression> fields) {
        return Highlight.generatedAttributesFor(Source.EMPTY, Highlight.DEFAULT_PREFIX, fields);
    }

    // The query is nullable on the plan node (the bare form has no explicit query), so cover both cases.
    private static Expression randomQuery() {
        return randomBoolean() ? null : Literal.keyword(Source.EMPTY, randomIdentifier());
    }

    private static MapExpression randomOptions() {
        if (randomBoolean()) {
            return null;
        }
        return randomNonNullOptions();
    }

    private static MapExpression randomNonNullOptions() {
        List<Expression> entries = List.of(
            Literal.keyword(Source.EMPTY, Highlight.NUMBER_OF_FRAGMENTS),
            new Literal(Source.EMPTY, randomIntBetween(1, 10), DataType.INTEGER)
        );
        return new MapExpression(Source.EMPTY, entries);
    }
}
