/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesMetadataSupportTests extends ESTestCase {

    public void testSplitsCoverEveryStoredForm() {
        assertThat(TimeSeriesMetadataSupport.splits("pod"), equalTo(List.of(List.of("pod"))));
        assertThat(TimeSeriesMetadataSupport.splits("labels.pod"), containsInAnyOrder(List.of("labels.pod"), List.of("labels", "pod")));
        assertThat(
            TimeSeriesMetadataSupport.splits("a.b.c"),
            containsInAnyOrder(List.of("a.b.c"), List.of("a", "b.c"), List.of("a.b", "c"), List.of("a", "b", "c"))
        );
    }

    public void testUnsetRemovesEveryStoredForm() throws IOException {
        assertUnset(
            "{\"resource\":{\"attributes.host\":{\"name\":\"h\"}},\"resource.attributes\":{\"host.name\":\"h\"},\"x\":1}",
            List.of("resource.attributes.host.name"),
            "{\"x\":1}"
        );
    }

    public void testUnsetPrunesEmptiedParentsOnly() throws IOException {
        assertUnset("{\"a\":{\"b\":\"1\",\"c\":\"2\"}}", List.of("a.b"), "{\"a\":{\"c\":\"2\"}}");
        assertUnset("{\"a\":{\"b\":{\"c\":\"1\"}},\"d\":\"2\"}", List.of("a.b.c"), "{\"d\":\"2\"}");
        assertUnset("{\"a\":{\"b\":null}}", List.of("a.b"), "{}");
        // an object that was already empty is not something the removal emptied
        assertUnset("{\"a\":{},\"b\":\"1\"}", List.of("b"), "{\"a\":{}}");
    }

    public void testUnsetLeavesMissingDimensionsAlone() throws IOException {
        assertUnset("{\"a\":\"1\"}", List.of("b"), "{\"a\":\"1\"}");
        // a path through a value that isn't an object is missing too
        assertUnset("{\"a\":\"1\"}", List.of("a.b"), "{\"a\":\"1\"}");
    }

    public void testWritesTheCanonicalForm() throws IOException {
        assertUnset(
            "{\"b\":[{\"d\":1,\"c\":2}],\"a\":{\"f\":1,\"e\":2}}",
            List.of(),
            "{\"a\":{\"e\":2,\"f\":1},\"b\":[{\"c\":2,\"d\":1}]}"
        );
    }

    public void testRejectsAnythingButOneObject() {
        assertThat(rejected("[1]"), containsString("expected a JSON object"));
        assertThat(rejected("{} {}"), containsString("trailing content after JSON object"));
        assertThat(rejected("{\"a\":"), containsString("invalid JSON object"));
    }

    public void testDimensionsMustBeConstantStrings() {
        Expression timeseries = new ReferenceAttribute(Source.EMPTY, "_timeseries", DataType.KEYWORD);
        assertTrue(unset(timeseries, List.of(Literal.keyword(Source.EMPTY, "pod"))).typeResolved().resolved());
        assertTrue(unset(timeseries, List.of()).typeResolved().resolved());
        Expression column = new ReferenceAttribute(Source.EMPTY, "pod", DataType.KEYWORD);
        assertThat(unset(timeseries, List.of(column)).typeResolved().message(), containsString("must be a constant"));
        Expression none = new Literal(Source.EMPTY, null, DataType.KEYWORD);
        assertThat(unset(timeseries, List.of(none)).typeResolved().message(), containsString("cannot be null"));
        Expression number = new Literal(Source.EMPTY, 1, DataType.INTEGER);
        assertThat(unset(number, List.of()).typeResolved().message(), containsString("must be [string]"));
    }

    private static TimeSeriesUnset unset(Expression timeseries, List<Expression> dimensions) {
        return new TimeSeriesUnset(Source.EMPTY, timeseries, dimensions);
    }

    private static void assertUnset(String value, List<String> dimensions, String expected) throws IOException {
        BytesRef result = TimeSeriesMetadataSupport.unset(dimensions).apply(new BytesRef(value));
        assertThat(result.utf8ToString(), equalTo(expected));
    }

    private static String rejected(String value) {
        return expectThrows(IllegalArgumentException.class, () -> TimeSeriesMetadataSupport.unset(List.of()).apply(new BytesRef(value)))
            .getMessage();
    }
}
