/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.Decay.DecayFunction;

import java.time.Duration;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Invalid constant arguments of {@link Decay}. Folding goes through {@link Decay#toEvaluator}, the same path
 * a {@code ROW} query or a data node takes.
 */
public class DecayParameterTests extends ESTestCase {

    public void testNodePropertiesWithoutOptions() {
        Decay decay = new Decay(Source.EMPTY, integer(5), integer(0), integer(10), null);
        List<Object> properties = decay.nodeProperties();
        assertThat(properties, hasSize(4));
        assertThat(properties.get(3), nullValue());
        assertThat(decay.nodeString(), equalTo("DECAY(5[INTEGER],0[INTEGER],10[INTEGER])"));
    }

    public void testTypeIsCaseInsensitive() {
        assertThat(DecayFunction.fromBytesRef(new BytesRef("GAUSS")), equalTo(DecayFunction.GAUSSIAN));
        assertThat(DecayFunction.fromBytesRef(new BytesRef("Exp")), equalTo(DecayFunction.EXPONENTIAL));
    }

    public void testUnknownTypeIsRejected() {
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> DecayFunction.fromBytesRef(new BytesRef("gaussian"))
        );
        assertThat(e.getMessage(), equalTo("unknown decay type [gaussian], expected one of [gauss, linear, exp]"));
    }

    public void testDecayOfOne() {
        assertFoldError(
            "[decay] option must be in the range (0, 1), found [1.0]",
            integer(5),
            integer(10),
            integer(10),
            options("decay", dbl(1.0))
        );
    }

    public void testDecayOfZero() {
        assertFoldError(
            "[decay] option must be in the range (0, 1), found [0.0]",
            integer(10),
            integer(10),
            integer(10),
            options("decay", dbl(0.0), "type", Literal.keyword(Source.EMPTY, "exp"))
        );
    }

    public void testDecayNaN() {
        assertFoldError(
            "[decay] option must be in the range (0, 1), found [NaN]",
            integer(5),
            integer(10),
            integer(10),
            options("decay", dbl(Double.NaN))
        );
    }

    public void testNegativeScale() {
        assertFoldError("[scale] must be a finite number greater than 0, found [-10.0]", integer(5), integer(0), integer(-10), null);
    }

    public void testZeroScale() {
        assertFoldError("[scale] must be a finite number greater than 0, found [0.0]", integer(10), integer(10), integer(0), null);
    }

    public void testInfiniteScale() {
        assertFoldError(
            "[scale] must be a finite number greater than 0, found [Infinity]",
            dbl(1.0),
            dbl(0.0),
            dbl(Double.POSITIVE_INFINITY),
            null
        );
    }

    public void testNegativeOffset() {
        assertFoldError(
            "[offset] option must be a finite number greater than or equal to 0, found [-3.0]",
            integer(5),
            integer(0),
            integer(10),
            options("offset", integer(-3))
        );
    }

    public void testZeroGeoScale() {
        assertFoldError(
            "[scale] must be a finite number greater than 0, found [0km]",
            geoPoint("POINT (1 1)"),
            geoPoint("POINT (1 1)"),
            Literal.keyword(Source.EMPTY, "0km"),
            null
        );
    }

    public void testInvalidGeoScale() {
        assertFoldError(
            "[scale] is not a valid distance: [abc]",
            geoPoint("POINT (1 1)"),
            geoPoint("POINT (0 0)"),
            Literal.keyword(Source.EMPTY, "abc"),
            null
        );
    }

    public void testInvalidGeoOffset() {
        assertFoldError(
            "[offset] is not a valid distance: [abc]",
            geoPoint("POINT (1 1)"),
            geoPoint("POINT (0 0)"),
            Literal.keyword(Source.EMPTY, "100km"),
            options("offset", Literal.keyword(Source.EMPTY, "abc"))
        );
    }

    public void testNegativeGeoOffset() {
        assertFoldError(
            "[offset] option must be a finite number greater than or equal to 0, found [-5km]",
            geoPoint("POINT (1 1)"),
            geoPoint("POINT (0 0)"),
            Literal.keyword(Source.EMPTY, "100km"),
            options("offset", Literal.keyword(Source.EMPTY, "-5km"))
        );
    }

    public void testDateNanosScaleTooLarge() {
        assertFoldError(
            "[scale] is too large for a [date_nanos] value: [PT3000000H]",
            new Literal(Source.EMPTY, 1_000L, DataType.DATE_NANOS),
            new Literal(Source.EMPTY, 0L, DataType.DATE_NANOS),
            Literal.timeDuration(Source.EMPTY, Duration.ofHours(3_000_000)),
            null
        );
    }

    public void testSubMillisecondScaleOnDatetime() {
        assertFoldError(
            "[scale] must be a finite number greater than 0, found [0ms]",
            new Literal(Source.EMPTY, 1_000L, DataType.DATETIME),
            new Literal(Source.EMPTY, 0L, DataType.DATETIME),
            Literal.timeDuration(Source.EMPTY, Duration.ofNanos(100)),
            null
        );
    }

    public void testDatetimeOriginBeforeEpochOnDateNanosValue() {
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> fold(
                new Literal(Source.EMPTY, 1_000L, DataType.DATE_NANOS),
                new Literal(Source.EMPTY, -1_000L, DataType.DATETIME),
                Literal.timeDuration(Source.EMPTY, Duration.ofHours(1)),
                null
            )
        );
        assertThat(e.getMessage(), containsString("[origin] cannot be converted to [date_nanos]"));
    }

    private static void assertFoldError(String message, Expression value, Expression origin, Expression scale, Expression options) {
        InvalidArgumentException e = expectThrows(InvalidArgumentException.class, () -> fold(value, origin, scale, options));
        assertThat(e.getMessage(), equalTo(message));
    }

    private static Object fold(Expression value, Expression origin, Expression scale, Expression options) {
        return new Decay(Source.EMPTY, value, origin, scale, options).fold(FoldContext.small());
    }

    private static Literal integer(int value) {
        return Literal.integer(Source.EMPTY, value);
    }

    private static Literal dbl(double value) {
        return Literal.fromDouble(Source.EMPTY, value);
    }

    private static Literal geoPoint(String wkt) {
        return new Literal(Source.EMPTY, GEO.wktToWkb(wkt), DataType.GEO_POINT);
    }

    private static MapExpression options(Object... keysAndValues) {
        Expression[] entries = new Expression[keysAndValues.length];
        for (int i = 0; i < keysAndValues.length; i += 2) {
            entries[i] = Literal.keyword(Source.EMPTY, (String) keysAndValues[i]);
            entries[i + 1] = (Expression) keysAndValues[i + 1];
        }
        return new MapExpression(Source.EMPTY, List.of(entries));
    }
}
