/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class WindowWithPartialSerializationTests extends AbstractExpressionSerializationTests<WindowWithPartial> {
    @Override
    protected WindowWithPartial createTestInstance() {
        return new WindowWithPartial(randomSource(), randomChild(), randomPartialFilter());
    }

    @Override
    protected WindowWithPartial mutateInstance(WindowWithPartial instance) throws IOException {
        Source source = randomSource();
        Expression window = instance.window();
        WindowFilter partialFilter = instance.partialFilter();
        switch (between(0, 1)) {
            case 0 -> window = randomValueOtherThan(window, AbstractExpressionSerializationTests::randomChild);
            case 1 -> partialFilter = randomValueOtherThan(partialFilter, WindowWithPartialSerializationTests::randomPartialFilter);
        }
        return new WindowWithPartial(source, window, partialFilter);
    }

    private static WindowFilter randomPartialFilter() {
        return new WindowFilter(randomSource(), randomChild(), randomChild(), randomChild());
    }
}
