/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AliasTests;
import org.elasticsearch.xpack.esql.plan.physical.AbstractPhysicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class RerankExecSerializationTests extends AbstractPhysicalPlanSerializationTests<RerankExec> {
    @Override
    protected RerankExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        return new RerankExec(source, child, string(randomIdentifier()), string(randomIdentifier()), randomFields(), scoreAttribute());
    }

    @Override
    protected RerankExec mutateInstance(RerankExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression inferenceId = instance.inferenceId();
        Expression queryText = instance.queryText();
        List<Alias> fields = instance.rerankFields();

        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, () -> string(RerankExecSerializationTests.randomIdentifier()));
            case 2 -> queryText = randomValueOtherThan(queryText, () -> string(RerankExecSerializationTests.randomIdentifier()));
            case 3 -> fields = randomValueOtherThan(fields, this::randomFields);
        }
        return new RerankExec(instance.source(), child, inferenceId, queryText, fields, scoreAttribute());
    }

    private List<Alias> randomFields() {
        return randomList(0, 10, AliasTests::randomAlias);
    }

    static Literal string(String value) {
        return new Literal(EMPTY, value, DataType.KEYWORD);
    }

    private Attribute scoreAttribute() {
        return new MetadataAttribute(EMPTY, MetadataAttribute.SCORE, DataType.DOUBLE, randomBoolean());
    }
}
