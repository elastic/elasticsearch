/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.hamcrest.Matcher;

import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvMinTests extends AbstractMultivalueFunctionTestCase {
    @Override
    protected Expression build(Source source, Expression field) {
        return new MvMin(source, field);
    }

    @Override
    protected DataType[] supportedTypes() {
        return representable();
    }

    @Override
    protected Matcher<Object> resultMatcherForInput(List<?> input, DataType dataType) {
        if (input == null) {
            return nullValue();
        }
        return switch (LocalExecutionPlanner.toElementType(EsqlDataTypes.fromJava(input.get(0)))) {
            case BOOLEAN -> equalTo(input.stream().mapToInt(o -> (Boolean) o ? 1 : 0).min().getAsInt() == 1);
            case BYTES_REF -> equalTo(input.stream().map(o -> (BytesRef) o).min(Comparator.naturalOrder()).get());
            case DOUBLE -> equalTo(input.stream().mapToDouble(o -> (Double) o).min().getAsDouble());
            case INT -> equalTo(input.stream().mapToInt(o -> (Integer) o).min().getAsInt());
            case LONG -> equalTo(input.stream().mapToLong(o -> (Long) o).min().getAsLong());
            default -> throw new UnsupportedOperationException("unsupported type " + input);
        };
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "MvMin[field=Attribute[channel=0]]";
    }
}
