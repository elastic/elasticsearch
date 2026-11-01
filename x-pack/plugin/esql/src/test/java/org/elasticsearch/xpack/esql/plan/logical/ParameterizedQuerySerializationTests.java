/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.FieldAttributeTestUtils.createFieldAttribute;

public class ParameterizedQuerySerializationTests extends AbstractLogicalPlanSerializationTests<ParameterizedQuery> {

    public static ParameterizedQuery randomParameterizedQuery() {
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        List<MatchConfig> matchFields = randomList(1, 3, ParameterizedQuerySerializationTests::randomMatchConfig);
        Expression joinOnConditions = randomBoolean() ? null : createFieldAttribute(3, false);
        return new ParameterizedQuery(randomSource(), output, matchFields, joinOnConditions);
    }

    private static MatchConfig randomMatchConfig() {
        return new MatchConfig(randomAlphaOfLength(5), between(0, 10), randomFrom(DataType.KEYWORD, DataType.INTEGER, DataType.LONG));
    }

    @Override
    protected ParameterizedQuery createTestInstance() {
        return randomParameterizedQuery();
    }

    @Override
    protected ParameterizedQuery mutateInstance(ParameterizedQuery instance) throws IOException {
        List<Attribute> output = instance.output();
        List<MatchConfig> matchFields = instance.matchFields();
        Expression joinOnConditions = instance.joinOnConditions();
        switch (between(0, 2)) {
            case 0 -> output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
            case 1 -> matchFields = randomValueOtherThan(
                matchFields,
                () -> randomList(1, 3, ParameterizedQuerySerializationTests::randomMatchConfig)
            );
            case 2 -> joinOnConditions = randomValueOtherThan(
                joinOnConditions,
                () -> randomBoolean() ? null : createFieldAttribute(3, false)
            );
            default -> throw new IllegalArgumentException();
        }
        return new ParameterizedQuery(instance.source(), output, matchFields, joinOnConditions);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
