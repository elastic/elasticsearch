/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;

public class ToStringSerializationTests extends AbstractUnaryScalarSerializationTests<ToString> {
    @Override
    protected ToString create(Source source, Expression child) {
        return new ToString(source, child, configuration());
    }

    /** The ordinary conversion expression preserves its opaque metadata input type on the wire. */
    public void testTimeSeriesField() throws IOException {
        ToString expression = create(randomSource(), field("metadata", DataType.TIMESERIES_DATA_TYPE));
        ToString copy = copyInstance(expression);
        assertEquals(expression, copy);
        assertEquals(DataType.TIMESERIES_DATA_TYPE, copy.field().dataType());
        assertEquals(DataType.KEYWORD, copy.dataType());
        assertTrue(copy.typeResolved().resolved());
    }
}
