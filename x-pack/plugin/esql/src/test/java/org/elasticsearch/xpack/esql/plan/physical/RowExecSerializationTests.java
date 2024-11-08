/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.LiteralTests;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;

import java.io.IOException;
import java.util.List;

public class RowExecSerializationTests extends AbstractPhysicalPlanSerializationTests<RowExec> {
    public static RowExec randomRowExec() {
        Source source = randomSource();
        List<Alias> fields = randomList(1, 10, RowExecSerializationTests::randomAlias);
        return new RowExec(source, fields);
    }

    private static Alias randomAlias() {
        Source source = SourceTests.randomSource();
        String name = randomAlphaOfLength(5);
        Expression child = LiteralTests.randomLiteral();
        boolean synthetic = randomBoolean();
        return new Alias(source, name, child, new NameId(), synthetic);
    }

    @Override
    protected RowExec createTestInstance() {
        return randomRowExec();
    }

    @Override
    protected RowExec mutateInstance(RowExec instance) throws IOException {
        List<Alias> fields = instance.fields();
        fields = randomValueOtherThan(fields, () -> randomList(1, 10, RowExecSerializationTests::randomAlias));
        return new RowExec(instance.source(), fields);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
