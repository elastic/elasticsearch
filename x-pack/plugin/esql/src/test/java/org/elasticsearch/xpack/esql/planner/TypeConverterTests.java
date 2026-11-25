/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLongBase;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@link TypeConverter} runs the conversions of a union-typed field while the field is loaded, where the only thing available
 * is the block of the loaded field.
 */
public class TypeConverterTests extends ESTestCase {
    private static final FieldAttribute KEYWORD_FIELD = field("value", DataType.KEYWORD);

    /**
     * The base of {@code TO_LONG(value, 16)} is a constant, so it's evaluated without reading the loaded block, and as many times
     * as the loaded block has positions.
     */
    public void testConstantBase() {
        ToLongBase conversion = new ToLongBase(
            Source.EMPTY,
            KEYWORD_FIELD,
            new ToInteger(Source.EMPTY, new Literal(Source.EMPTY, 16, DataType.INTEGER))
        );
        DriverContext driverContext = driverContext();
        // The converter is built first: if building it fails, there's no block left to release.
        try (var converter = new TypeConverter(conversion).build(driverContext)) {
            // The converter takes ownership of the loaded block, so it's not closed here.
            BytesRefBlock input = driverContext.blockFactory()
                .newBytesRefBlockBuilder(3)
                .appendBytesRef(new BytesRef("ff"))
                .appendBytesRef(new BytesRef("10"))
                .appendBytesRef(new BytesRef("7fffffff"))
                .build();
            try (LongBlock result = (LongBlock) converter.convert(input)) {
                assertThat(result.getPositionCount(), equalTo(3));
                assertThat(result.getLong(0), equalTo(255L));
                assertThat(result.getLong(1), equalTo(16L));
                assertThat(result.getLong(2), equalTo(2147483647L));
            }
        }
    }

    /**
     * The analyzer doesn't build conversions of other fields, but if one got here its base would be evaluated from the loaded
     * block, silently returning wrong results, so it fails.
     */
    public void testBaseComputedFromAnotherFieldFails() {
        ToLongBase conversion = new ToLongBase(Source.EMPTY, KEYWORD_FIELD, new ToInteger(Source.EMPTY, field("base", DataType.INTEGER)));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new TypeConverter(conversion));
        assertThat(e.getMessage(), containsString("can't be evaluated while loading a field"));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, type, Collections.emptyMap(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static DriverContext driverContext() {
        return EvalMapperTests.driverContext();
    }
}
