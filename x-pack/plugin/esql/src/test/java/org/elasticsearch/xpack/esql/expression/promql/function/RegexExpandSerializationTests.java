/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class RegexExpandSerializationTests extends AbstractExpressionSerializationTests<RegexExpand> {
    @Override
    protected RegexExpand createTestInstance() {
        return new RegexExpand(randomSource(), randomChild(), randomChild(), randomChild());
    }

    @Override
    protected RegexExpand mutateInstance(RegexExpand instance) throws IOException {
        Source source = instance.source();
        Expression src = instance.children().get(0);
        Expression regex = instance.children().get(1);
        Expression replacement = instance.children().get(2);
        switch (between(0, 2)) {
            case 0 -> src = randomValueOtherThan(src, AbstractExpressionSerializationTests::randomChild);
            case 1 -> regex = randomValueOtherThan(regex, AbstractExpressionSerializationTests::randomChild);
            case 2 -> replacement = randomValueOtherThan(replacement, AbstractExpressionSerializationTests::randomChild);
        }
        return new RegexExpand(source, src, regex, replacement);
    }
}
