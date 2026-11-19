/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

import static org.hamcrest.Matchers.nullValue;

/**
 * {@link RoundTo#dataType()} caches its result, so it has to give the same answer every time it's called.
 */
public class RoundToDataTypeTests extends ESTestCase {
    /**
     * {@code unsigned_long} isn't supported, so there is no result type. This used to cache the field's type before finding
     * that out, and return it from the second call on.
     */
    public void testUnsupportedPointHasNoTypeOnEveryCall() {
        RoundTo roundTo = new RoundTo(
            Source.EMPTY,
            new ReferenceAttribute(Source.EMPTY, "field", DataType.LONG),
            List.of(new Literal(Source.EMPTY, 1L, DataType.UNSIGNED_LONG))
        );
        assertThat(roundTo.dataType(), nullValue());
        assertThat(roundTo.dataType(), nullValue());
    }
}
