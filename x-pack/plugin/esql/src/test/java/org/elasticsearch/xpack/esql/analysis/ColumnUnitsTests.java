/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToExponentialHistogram;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToTDigest;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.unit.Unit;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ColumnUnitsTests extends ESTestCase {

    private final Map<NameId, Unit> units = new HashMap<>();

    private ReferenceAttribute column(String name, String unit) {
        ReferenceAttribute attribute = new ReferenceAttribute(Source.EMPTY, null, name, DataType.LONG);
        if (unit != null) {
            units.put(attribute.id(), Unit.parse(unit));
        }
        return attribute;
    }

    private static Expression div(Expression left, Expression right) {
        return new Div(Source.EMPTY, left, right, DataType.LONG);
    }

    private static Expression mul(Expression left, Expression right) {
        return new Mul(Source.EMPTY, left, right, false);
    }

    private static Literal number(int value) {
        return Literal.integer(Source.EMPTY, value);
    }

    private String unitOf(Expression e) {
        Unit unit = ColumnUnits.unitOf(e, units);
        return unit == null ? null : unit.format();
    }

    public void testExactTimeConversions() {
        assertThat(unitOf(div(column("d", "nanos"), number(1_000_000))), equalTo("ms"));
        assertThat(unitOf(div(column("d", "ns"), number(1_000))), equalTo("micros"));
        assertThat(unitOf(div(column("d", "ms"), number(1_000))), equalTo("s"));
        assertThat(unitOf(div(column("d", "ms"), number(60_000))), equalTo("min"));
        assertThat(unitOf(mul(column("d", "s"), number(1_000))), equalTo("ms"));
        assertThat(unitOf(mul(number(1_000), column("d", "s"))), equalTo("ms"));
    }

    public void testExactSizeConversions() {
        assertThat(unitOf(div(column("b", "bytes"), number(1024))), equalTo("KiB"));
        assertThat(unitOf(div(column("b", "bytes"), number(1_048_576))), equalTo("MiB"));
        assertThat(unitOf(div(column("b", "bytes"), number(1_000))), equalTo("kB"));
        assertThat(unitOf(div(column("b", "By"), number(1_000_000))), equalTo("MB"));
        assertThat(unitOf(div(column("b", "MiBy"), number(1024))), equalTo("GiB"));
        assertThat(unitOf(mul(column("b", "GB"), number(1_000))), equalTo("MB"));
        assertThat(unitOf(mul(column("b", "KiB"), number(1024))), equalTo("bytes"));
        assertThat(unitOf(mul(column("b", "bytes"), number(8))), equalTo("bits"));
        assertThat(unitOf(div(column("b", "bit"), number(8))), equalTo("bytes"));
        assertThat(unitOf(div(column("b", "bits"), number(8_000))), equalTo("kB"));
        assertThat(unitOf(mul(column("r", "bytes/s"), number(8))), equalTo("bits/s"));
        assertThat("the numerator rescales", unitOf(div(column("r", "bytes/s"), number(1024))), equalTo("KiB/s"));
        assertThat(unitOf(div(column("r", "ms/s"), number(1_000))), equalTo("1"));
        assertThat("decimal and binary prefixes do not mix", unitOf(div(column("b", "MB"), number(1024))), equalTo("MB/1024"));
    }

    public void testOtherScalingStaysExplicit() {
        assertThat(unitOf(mul(column("b", "bytes"), number(3))), equalTo("bytes*3"));
        assertThat(unitOf(div(column("d", "ms"), number(7))), equalTo("ms/7"));
        assertThat(unitOf(div(column("d", "ms"), number(1))), equalTo("ms"));
    }

    public void testRatios() {
        ReferenceAttribute time = column("time", "ms");
        assertThat("time per operation", unitOf(div(time, column("ops", "1"))), equalTo("ms"));
        assertThat("same unit is dimensionless", unitOf(div(time, column("other", "ms"))), equalTo("1"));
        assertThat(unitOf(div(column("bytes", "bytes"), time)), equalTo("bytes/ms"));
        assertThat(unitOf(mul(time, column("ops", "1"))), equalTo("ms"));
    }

    public void testUnitsCancel() {
        ReferenceAttribute timeRate = column("time_rate", "ms/s");
        ReferenceAttribute queryRate = column("query_rate", "1/s");
        assertThat("latency from two rates", unitOf(div(timeRate, queryRate)), equalTo("ms"));
        assertThat(unitOf(mul(column("rate", "bytes/s"), column("t", "s"))), equalTo("bytes"));
        assertThat(unitOf(div(column("a", "bytes/s"), column("b", "packets/s"))), equalTo("bytes/packets"));
        assertThat(unitOf(mul(column("x", "s"), column("y", "s"))), equalTo("s^2"));
        assertThat(unitOf(mul(column("x", "s^2"), column("y", "bytes/s"))), equalTo("s*bytes"));
        assertThat("scaled units are opaque", unitOf(div(div(column("b", "ms"), number(7)), column("c", "ms"))), equalTo("ms/7/ms"));
    }

    public void testVocabulary() {
        assertThat("OpenTelemetry bytes", unitOf(div(column("a", "By"), column("b", "bytes"))), equalTo("1"));
        assertThat("UCUM annotation", unitOf(div(column("a", "{packets}"), column("b", "s"))), equalTo("packets/s"));
        assertThat(unitOf(div(column("a", "ns"), number(1_000_000))), equalTo("ms"));
        assertThat("exponents", unitOf(div(column("a", "m/s^2"), column("b", "1/s"))), equalTo("m/s"));
    }

    public void testCount() {
        assertThat(unitOf(new Count(Source.EMPTY, column("x", "ms"))), equalTo("1"));
    }

    public void testHistogramConversions() {
        assertThat(unitOf(new ToTDigest(Source.EMPTY, column("h", "ms"))), equalTo("ms"));
        assertThat(unitOf(new ToExponentialHistogram(Source.EMPTY, column("h", "ms"))), equalTo("ms"));
    }

    public void testUnknownUnits() {
        assertThat(unitOf(column("plain", null)), nullValue());
        assertThat(unitOf(div(column("plain", null), number(1_000))), nullValue());
        assertThat(unitOf(div(column("a", "ms"), column("b", null))), nullValue());
        assertThat(unitOf(number(3)), nullValue());
    }
}
