/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.List;

public class DissectSerializationTests extends AbstractLogicalPlanSerializationTests<Dissect> {
    @Override
    protected Dissect createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        Expression input = FieldAttributeTests.createFieldAttribute(1, false);
        Dissect.Parser parser = randomParser();
        List<Attribute> extracted = randomFieldAttributes(0, 4, false);
        return new Dissect(source, child, input, parser, extracted);
    }

    public static Dissect.Parser randomParser() {
        String suffix = randomAlphaOfLength(5);
        String pattern = "%{b} %{c}" + suffix;
        return new Dissect.Parser(pattern, ",", new DissectParser(pattern, ","));
    }

    @Override
    protected Dissect mutateInstance(Dissect instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression input = instance.input();
        Dissect.Parser parser = randomParser();
        List<Attribute> extracted = randomFieldAttributes(0, 4, false);
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> input = randomValueOtherThan(input, () -> FieldAttributeTests.createFieldAttribute(1, false));
            case 2 -> parser = randomValueOtherThan(parser, DissectSerializationTests::randomParser);
            case 3 -> extracted = randomValueOtherThan(extracted, () -> randomFieldAttributes(0, 4, false));
        }
        return new Dissect(instance.source(), child, input, parser, extracted);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
