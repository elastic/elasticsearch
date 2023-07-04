/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvDedupeTests extends AbstractMultivalueFunctionTestCase {
    @Override
    protected Expression build(Source source, Expression field) {
        return new MvDedupe(source, field);
    }

    @Override
    protected DataType[] supportedTypes() {
        return representable();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Matcher<Object> resultMatcherForInput(List<?> input, DataType dataType) {
        if (input == null) {
            return nullValue();
        }
        Set<Object> values = input.stream().collect(Collectors.toSet());
        return switch (values.size()) {
            case 0 -> nullValue();
            case 1 -> equalTo(values.iterator().next());
            default -> (Matcher<Object>) (Matcher<?>) containsInAnyOrder(values.stream().map(Matchers::equalTo).toArray(Matcher[]::new));
        };
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "MvDedupe[field=Attribute[channel=0]]";
    }
}
