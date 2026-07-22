/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MvSingleValueOrNullSerializationTests extends AbstractExpressionSerializationTests<MvSingleValueOrNull> {
    @Override
    protected MvSingleValueOrNull createTestInstance() {
        List<Source> warningSources = randomList(1, 5, AbstractNodeSerializationTests::randomSource);
        return new MvSingleValueOrNull(randomSource(), randomChild(), warningSources);
    }

    @Override
    protected MvSingleValueOrNull mutateInstance(MvSingleValueOrNull instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        List<Source> warningSources = new ArrayList<>(instance.warningSources());
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            if (warningSources.size() == 1) {
                warningSources.add(randomSource());
            } else {
                warningSources.remove(randomInt(warningSources.size() - 1));
            }
        }
        return new MvSingleValueOrNull(source, field, warningSources);
    }
}
