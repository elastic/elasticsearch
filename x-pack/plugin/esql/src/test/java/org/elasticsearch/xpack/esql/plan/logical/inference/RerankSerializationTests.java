/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AliasTests;
import org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class RerankSerializationTests extends AbstractLogicalPlanSerializationTests<Rerank> {
    @Override
    protected Rerank createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        return new Rerank(
            source,
            child,
            string(randomIdentifier()),
            randomRowLimit(),
            string(randomIdentifier()),
            randomFields(),
            scoreAttribute(),
            randomTimeout()
        );
    }

    @Override
    protected Rerank mutateInstance(Rerank instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression inferenceId = instance.inferenceId();
        Expression rowLimit = instance.rowLimit();
        Expression queryText = instance.queryText();
        List<Alias> fields = instance.rerankFields();
        TimeValue timeout = instance.timeout();

        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, () -> string(RerankSerializationTests.randomIdentifier()));
            case 2 -> rowLimit = randomValueOtherThan(rowLimit, this::randomRowLimit);
            case 3 -> queryText = randomValueOtherThan(queryText, () -> string(RerankSerializationTests.randomIdentifier()));
            case 4 -> fields = randomValueOtherThan(fields, this::randomFields);
            case 5 -> timeout = randomValueOtherThan(timeout, this::randomTimeout);
        }
        return new Rerank(instance.source(), child, inferenceId, rowLimit, queryText, fields, instance.scoreAttribute(), timeout);
    }

    private TimeValue randomTimeout() {
        return randomBoolean() ? null : TimeValue.timeValueMillis(randomLongBetween(1, 300_000));
    }

    private List<Alias> randomFields() {
        return randomList(0, 10, AliasTests::randomAlias);
    }

    private Literal string(String value) {
        return Literal.keyword(EMPTY, value);
    }

    private Expression randomRowLimit() {
        return new Literal(Source.EMPTY, randomIntBetween(1, 100), DataType.INTEGER);
    }

    private Attribute scoreAttribute() {
        return new MetadataAttribute(EMPTY, MetadataAttribute.SCORE, DataType.DOUBLE, randomBoolean());
    }
}
