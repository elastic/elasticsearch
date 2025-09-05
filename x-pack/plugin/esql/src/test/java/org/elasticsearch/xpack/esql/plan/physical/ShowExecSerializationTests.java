/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;

public class ShowExecSerializationTests extends AbstractPhysicalPlanSerializationTests<ShowExec> {
    public static ShowExec randomShowExec() {
        Source source = randomSource();
        List<Attribute> attributes = randomList(1, 10, () -> ReferenceAttributeTests.randomReferenceAttribute(true));
        List<List<Object>> values = randomValues(attributes);
        return new ShowExec(source, attributes, values);
    }

    private static List<List<Object>> randomValues(List<Attribute> attributes) {
        int size = between(0, 1000);
        List<List<Object>> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            List<Object> row = new ArrayList<>(attributes.size());
            for (Attribute a : attributes) {
                row.add(randomLiteral(a.dataType()).value());
            }
            result.add(row);
        }
        return result;
    }

    @Override
    protected ShowExec createTestInstance() {
        return randomShowExec();
    }

    @Override
    protected ShowExec mutateInstance(ShowExec instance) throws IOException {
        List<Attribute> attributes = instance.output();
        List<List<Object>> values = instance.values();
        if (randomBoolean()) {
            attributes = randomValueOtherThan(
                attributes,
                () -> randomList(1, 10, () -> ReferenceAttributeTests.randomReferenceAttribute(true))
            );
        }
        List<Attribute> finalAttributes = attributes;
        values = randomValueOtherThan(values, () -> randomValues(finalAttributes));
        return new ShowExec(instance.source(), attributes, values);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
