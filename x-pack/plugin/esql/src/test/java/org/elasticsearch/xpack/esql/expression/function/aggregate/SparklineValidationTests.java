/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIFTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.hamcrest.Matchers.equalTo;

public class SparklineValidationTests extends ESTestCase {

    public void testFieldNotAggregateFunction() {
        Sparkline s = sparkline(
            new Literal(Source.synthetic(""), 1.0, DataType.DOUBLE),
            validKey(),
            validBuckets(),
            validFrom(),
            validTo()
        );
        assertTrue(s.typeResolved().unresolved());
        assertThat(s.typeResolved().message(), equalTo("first argument of [] must be an aggregate function, found value [] type [double]"));
    }

    public void testFieldWrongType() {
        Sparkline s = sparkline(
            new Max(Source.synthetic(""), field("f", DataType.KEYWORD)),
            validKey(),
            validBuckets(),
            validFrom(),
            validTo()
        );
        assertInvalidType(s, FIRST.name().toLowerCase(Locale.ROOT), "integer or long or double", "keyword");
    }

    public void testKeyWrongType() {
        Sparkline s = sparkline(
            validAggField(),
            new Literal(Source.synthetic(""), 1L, DataType.LONG),
            validBuckets(),
            validFrom(),
            validTo()
        );
        assertInvalidType(s, SECOND.name().toLowerCase(Locale.ROOT), "date", "long");
    }

    public void testBucketsNotWholeNumber() {
        Sparkline s = sparkline(
            validAggField(),
            validKey(),
            new Literal(Source.synthetic(""), 1.5, DataType.DOUBLE),
            validFrom(),
            validTo()
        );
        assertInvalidType(s, THIRD.name().toLowerCase(Locale.ROOT), "integer", "double");
    }

    public void testBucketsNotFoldable() {
        Sparkline s = sparkline(validAggField(), validKey(), field("buckets", DataType.INTEGER), validFrom(), validTo());
        assertNotFoldable(s, THIRD.name().toLowerCase(Locale.ROOT), "buckets");
    }

    public void testFromWrongType() {
        Sparkline s = sparkline(
            validAggField(),
            validKey(),
            validBuckets(),
            new Literal(Source.synthetic(""), 1, DataType.INTEGER),
            validTo()
        );
        assertTrue(s.typeResolved().unresolved());
        assertInvalidType(s, FOURTH.name().toLowerCase(Locale.ROOT), "date or keyword or text", "integer");
    }

    public void testFromNotFoldable() {
        Sparkline s = sparkline(validAggField(), validKey(), validBuckets(), field("from", DataType.DATETIME), validTo());
        assertNotFoldable(s, FOURTH.name().toLowerCase(Locale.ROOT), "from");
    }

    public void testToWrongType() {
        Sparkline s = sparkline(
            validAggField(),
            validKey(),
            validBuckets(),
            validFrom(),
            new Literal(Source.synthetic(""), 1, DataType.INTEGER)
        );
        assertInvalidType(s, FIFTH.name().toLowerCase(Locale.ROOT), "date or keyword or text", "integer");
    }

    public void testToNotFoldable() {
        Sparkline s = sparkline(validAggField(), validKey(), validBuckets(), validFrom(), field("to", DataType.DATETIME));
        assertNotFoldable(s, FIFTH.name().toLowerCase(Locale.ROOT), "to");
    }

    private void assertInvalidType(Sparkline s, String position, String expected, String actual) {
        assertTrue(s.typeResolved().unresolved());
        assertThat(
            s.typeResolved().message(),
            equalTo(position + " argument of [] must be [" + expected + "], found value [] type [" + actual + "]")
        );
    }

    private void assertNotFoldable(Sparkline s, String position, String actual) {
        assertTrue(s.typeResolved().unresolved());
        assertThat(s.typeResolved().message(), equalTo(position + " argument of [] must be a constant, received [" + actual + "]"));
    }

    public void testNullField() {
        Sparkline s = sparkline(new Literal(Source.synthetic(""), null, DataType.NULL), validKey(), validBuckets(), validFrom(), validTo());
        assertNullError(s, FIRST.name().toLowerCase(Locale.ROOT));
    }

    public void testNullKey() {
        Sparkline s = sparkline(
            validAggField(),
            new Literal(Source.synthetic(""), null, DataType.NULL),
            validBuckets(),
            validFrom(),
            validTo()
        );
        assertNullError(s, SECOND.name().toLowerCase(Locale.ROOT));
    }

    public void testNullBuckets() {
        Sparkline s = sparkline(
            validAggField(),
            validKey(),
            new Literal(Source.synthetic(""), null, DataType.NULL),
            validFrom(),
            validTo()
        );
        assertNullError(s, THIRD.name().toLowerCase(Locale.ROOT));
    }

    public void testNullFrom() {
        Sparkline s = sparkline(
            validAggField(),
            validKey(),
            validBuckets(),
            new Literal(Source.synthetic(""), null, DataType.NULL),
            validTo()
        );
        assertNullError(s, FOURTH.name().toLowerCase(Locale.ROOT));
    }

    public void testNullTo() {
        Sparkline s = sparkline(
            validAggField(),
            validKey(),
            validBuckets(),
            validFrom(),
            new Literal(Source.synthetic(""), null, DataType.NULL)
        );
        assertNullError(s, FIFTH.name().toLowerCase(Locale.ROOT));
    }

    private void assertNullError(Sparkline s, String position) {
        assertTrue(s.typeResolved().unresolved());
        assertThat(s.typeResolved().message(), equalTo(position + " argument of [] cannot be null, received []"));
    }

    private AggregateFunction validAggField() {
        return new Avg(Source.synthetic(""), field("f", DataType.DOUBLE));
    }

    private Literal validKey() {
        return new Literal(Source.synthetic(""), 1_000_000L, DataType.DATETIME);
    }

    private Literal validBuckets() {
        return new Literal(Source.synthetic(""), 10, DataType.INTEGER);
    }

    private Literal validFrom() {
        return new Literal(Source.synthetic(""), 0L, DataType.DATETIME);
    }

    private Literal validTo() {
        return new Literal(Source.synthetic(""), 2_000_000L, DataType.DATETIME);
    }

    private static Sparkline sparkline(Expression field, Expression key, Expression buckets, Expression from, Expression to) {
        return new Sparkline(Source.synthetic(""), field, key, buckets, from, to);
    }
}
