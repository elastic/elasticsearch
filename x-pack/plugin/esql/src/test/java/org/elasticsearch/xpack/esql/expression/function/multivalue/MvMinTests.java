/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.multivalue;

import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MvMinTests extends AbstractMultivalueFunctionTestCase {
    @Override
    protected DataType[] supportedTypes() {
        return new DataType[] { DataTypes.INTEGER };
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvMin(source, field);
    }

    @Override
    protected Matcher<Object> resultMatcherForInput(List<?> input) {
        if (input.get(0) instanceof Integer) {
            return equalTo(input.stream().mapToInt(o -> (Integer) o).min().getAsInt());
        }
        throw new UnsupportedOperationException();
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "MvMin[field=Attribute[channel=0]]";
    }
}
