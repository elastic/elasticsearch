/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.hamcrest.Matchers.containsString;

/** Tests the type contract of the internal time-series metadata functions that PromQL translation builds. */
public class TimeSeriesFunctionTests extends ESTestCase {
    private static final Expression METADATA = new ReferenceAttribute(Source.EMPTY, "metadata", DataType.TIMESERIES_DATA_TYPE);

    /** The functions are PromQL-internal: ES|QL cannot name them. */
    public void testNotRegistered() {
        var registry = new EsqlFunctionRegistry();
        for (String name : List.of("ts_get", "ts_set", "ts_unset", "ts_hash", "ts_to_string")) {
            assertFalse(name, registry.functionExists(name));
        }
    }

    public void testMetadataTypes() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.ENABLE_TIMESERIES_DATA_TYPE.isEnabled());
        var metadata = Literal.NULL;
        var label = label("pod");
        var set = new TimeSeriesSet(Source.EMPTY, metadata, label, label("a"));
        var unset = new TimeSeriesUnset(Source.EMPTY, set, List.of(label));
        assertEquals(DataType.TIMESERIES_DATA_TYPE, set.dataType());
        assertEquals(DataType.TIMESERIES_DATA_TYPE, unset.dataType());
        assertTrue(set.typeResolved().resolved());
        assertTrue(unset.typeResolved().resolved());
        assertEquals(DataType.KEYWORD, new TimeSeriesGet(Source.EMPTY, unset, label).dataType());
        assertEquals(DataType.TSID_DATA_TYPE, new TimeSeriesHash(Source.EMPTY, unset, List.of()).dataType());
        assertEquals(DataType.KEYWORD, new ToString(Source.EMPTY, unset, TEST_CFG).dataType());
    }

    public void testRejectsUntypedMetadata() {
        var untyped = label("{}");
        for (Expression expression : List.of(
            new TimeSeriesGet(Source.EMPTY, untyped, label("pod")),
            new TimeSeriesSet(Source.EMPTY, untyped, label("pod"), label("a")),
            new TimeSeriesUnset(Source.EMPTY, untyped, List.of(label("pod"))),
            new TimeSeriesHash(Source.EMPTY, untyped, List.of())
        )) {
            assertThat(expression.typeResolved().message(), containsString("_timeseries"));
        }
    }

    public void testSupportedLabelValues() {
        for (Expression value : List.of(
            label("value"),
            Literal.TRUE,
            Literal.integer(Source.EMPTY, 1),
            Literal.fromLong(Source.EMPTY, 1L),
            Literal.fromDouble(Source.EMPTY, 1.5),
            Literal.NULL
        )) {
            assertTrue(new TimeSeriesSet(Source.EMPTY, METADATA, label("label"), value).typeResolved().resolved());
            assertTrue(new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("label"), value)).typeResolved().resolved());
        }
    }

    public void testRejectsUnsupportedLabelValues() {
        var nested = new TimeSeriesSet(Source.EMPTY, Literal.NULL, label("nested"), label("value"));
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("label"), nested);
        assertThat(set.typeResolved().message(), containsString("must be [string, boolean, integer, long, unsigned_long, double or ip]"));
        var hash = new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("label"), nested));
        assertThat(hash.typeResolved().message(), containsString("must be [string, boolean, integer, long, unsigned_long, double or ip]"));
    }

    public void testRejectsInvalidLabelArguments() {
        for (Expression expression : List.of(
            new TimeSeriesGet(Source.EMPTY, METADATA, Literal.integer(Source.EMPTY, 42)),
            new TimeSeriesGet(Source.EMPTY, METADATA, label("")),
            new TimeSeriesSet(Source.EMPTY, METADATA, Literal.NULL, label("a")),
            new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"))),
            new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), label("a"), label("pod"), label("b")))
        )) {
            assertTrue(expression.toString(), expression.typeResolved().unresolved());
        }
        var column = new ReferenceAttribute(Source.EMPTY, "cluster", DataType.KEYWORD);
        assertThat(new TimeSeriesGet(Source.EMPTY, METADATA, column).typeResolved().message(), containsString("must be a constant"));
    }

    private static Literal label(String name) {
        return Literal.keyword(Source.EMPTY, name);
    }
}
