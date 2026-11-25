/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * What a conversion function does with an input it can't convert: it resolves and is {@code null}, instead of failing, unless the
 * input type decides the result or the function needs it to be a constant.
 */
public class LenientConversionTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    private static Literal ip() {
        return new Literal(SOURCE, EsqlDataTypeConverter.stringToIP("1.2.3.4"), DataType.IP);
    }

    private static Literal bool() {
        return Literal.TRUE;
    }

    private static Literal keyword() {
        return Literal.keyword(SOURCE, "1");
    }

    private static Literal integer() {
        return Literal.integer(SOURCE, 1);
    }

    private static void assertFoldsToNull(Expression conversion) {
        assertTrue("expected resolved [" + conversion + "] but got " + conversion.typeResolved(), conversion.typeResolved().resolved());
        Expression lowered = conversion instanceof SurrogateExpression surrogate ? surrogate.surrogate() : conversion;
        assertThat(lowered.foldable(), is(true));
        assertThat(lowered.fold(FoldContext.small()), nullValue());
    }

    public void testInputThatDoesNotConvertIsNull() {
        assertFoldsToNull(new ToLong(SOURCE, ip()));
        assertFoldsToNull(new ToInteger(SOURCE, ip()));
        assertFoldsToNull(new ToDouble(SOURCE, ip()));
        assertFoldsToNull(new ToIp(SOURCE, bool(), null));
        assertFoldsToNull(new ToRadians(SOURCE, bool()));
        assertFoldsToNull(new ToDegrees(SOURCE, bool()));
        assertFoldsToNull(new ToBoolean(SOURCE, ip()));
        assertFoldsToNull(new ToLongSurrogate(SOURCE, ip(), null));
        assertFoldsToNull(new ToIntegerSurrogate(SOURCE, ip(), null));
    }

    public void testConvertsInput() {
        assertThat(new ToLong(SOURCE, ip()).convertsInput(), is(false));
        assertThat(new ToLong(SOURCE, keyword()).convertsInput(), is(true));
        assertThat(new ToLong(SOURCE, Literal.NULL).convertsInput(), is(true));
        assertThat(new ToLong(SOURCE, integer()).convertsInput(), is(true));
        assertThat(new ToIp(SOURCE, bool(), null).convertsInput(), is(false));
        assertThat(new ToIp(SOURCE, keyword(), null).convertsInput(), is(true));
    }

    /**
     * A column of an unsupported type that isn't a field, like the ones {@code ENRICH} returns, is a value that can't be converted. A
     * field is unsupported because of its mapping, and the analyzer decides what to do with it.
     */
    public void testUnsupportedInput() {
        assertFoldsToNullWhenResolved(new ToLong(SOURCE, new ReferenceAttribute(SOURCE, "x", DataType.UNSUPPORTED)));
        EsField unsupportedField = new EsField("x", DataType.UNSUPPORTED, Map.of(), false, EsField.TimeSeriesFieldType.NONE);
        Expression field = new ToLong(SOURCE, new FieldAttribute(SOURCE, "x", unsupportedField));
        assertThat(field.typeResolved().resolved(), is(false));
    }

    private static void assertFoldsToNullWhenResolved(Expression conversion) {
        assertTrue("expected resolved [" + conversion + "] but got " + conversion.typeResolved(), conversion.typeResolved().resolved());
        assertThat(((ConvertFunction) conversion).convertsInput(), is(false));
    }

    /**
     * The conversions that can't tell what to return for an input they don't convert.
     */
    public void testStrictConversions() {
        List<ConvertFunction> strict = List.of(
            new ToGauge(SOURCE, keyword()),
            new ToCounter(SOURCE, keyword()),
            new ToDatePeriod(SOURCE, integer()),
            new ToTimeDuration(SOURCE, integer()),
            new ToLongSurrogate(SOURCE, integer(), integer()),
            new ToIntegerSurrogate(SOURCE, integer(), integer())
        );
        for (ConvertFunction function : strict) {
            assertThat(function + " is lenient", function.lenientOnInputType(), is(false));
        }
    }

    public void testLenientConversions() {
        List<ConvertFunction> lenient = List.of(
            new ToLong(SOURCE, ip()),
            new ToIp(SOURCE, bool(), null),
            new ToDouble(SOURCE, bool()),
            new ToLongSurrogate(SOURCE, ip(), null),
            new ToIntegerSurrogate(SOURCE, ip(), null)
        );
        for (ConvertFunction function : lenient) {
            assertThat(function + " is strict", function.lenientOnInputType(), is(true));
        }
    }

    /**
     * Conversions keep the strict resolution available: it's the error a node that doesn't support lenient conversions would return.
     */
    public void testStrictTypeResolutionIsTheOldError() {
        List<ConvertFunction> invalid = List.of(
            new ToLong(SOURCE, ip()),
            new ToLongSurrogate(SOURCE, ip(), null),
            new ToIntegerSurrogate(SOURCE, ip(), null),
            new ToIp(SOURCE, bool(), null)
        );
        for (ConvertFunction function : invalid) {
            Expression.TypeResolution strict = function.strictTypeResolution();
            assertThat(function + " resolves strictly", strict.resolved(), is(false));
            assertThat(strict.message(), containsString("found value ["));
        }
    }
}
