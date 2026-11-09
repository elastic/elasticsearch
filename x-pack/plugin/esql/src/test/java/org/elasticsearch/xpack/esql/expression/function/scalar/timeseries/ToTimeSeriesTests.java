/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

/**
 * The keyword {@code _timeseries} source becomes the canonical label container: labels named as PromQL names them, sorted
 * by name, each with its non-empty values in source order. Equal label sets get equal bytes, so the result can key joins
 * and groups.
 */
public class ToTimeSeriesTests extends AbstractScalarFunctionTestCase {
    public ToTimeSeriesTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(convert("sorted by name", "{\"b\":\"2\",\"a\":\"1\"}", "{\"a\":\"1\",\"b\":\"2\"}"));
        suppliers.add(
            convert(
                "nested fields named as labels",
                "{\"labels\":{\"cluster\":\"prod\",\"region\":[\"eu\",\"us\"]},\"attributes\":{\"cpu\":\"0\"}}",
                "{\"attributes.cpu\":\"0\",\"cluster\":\"prod\",\"region\":[\"eu\",\"us\"]}"
            )
        );
        suppliers.add(
            convert(
                "null and empty values left out",
                "{\"a\":\"1\",\"empty\":\"\",\"null\":null,\"none\":[null,\"\"],\"values\":[\"w\",\"\",\"e\",null,\"w\"]}",
                "{\"a\":\"1\",\"values\":[\"w\",\"e\",\"w\"]}"
            )
        );
        suppliers.add(convert("no labels", "{\"empty\":\"\",\"none\":[]}", "{}"));
        suppliers.add(convert("one value as a string", "{\"a\":[\"1\"]}", "{\"a\":\"1\"}"));
        suppliers.add(convert("scalars as text", "{\"t\":true,\"n\":5}", "{\"n\":\"5\",\"t\":\"true\"}"));
        suppliers.add(convert("unescaped spelling", "{ \"\\u0061\": \"\\u0031\" }", "{\"a\":\"1\"}"));
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static TestCaseSupplier convert(String name, String source, String expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(source), DataType.KEYWORD, "source")),
                "ToTimeSeriesFromKeywordEvaluator[in=Attribute[channel=0]]",
                DataType.TSMD_DATA_TYPE,
                equalTo(new BytesRef(expected))
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToTimeSeries(source, args.getFirst());
    }

    /** Only the keyword source converts: other values are not a series' dimensions. */
    public void testSourceMustBeKeyword() {
        for (DataType type : List.of(DataType.TSMD_DATA_TYPE, DataType.INTEGER, DataType.BOOLEAN)) {
            assertTrue(type.toString(), new ToTimeSeries(Source.EMPTY, field("source", type)).typeResolved().unresolved());
        }
    }

    /** A source that is not a set of labels, or whose fields name one label twice, fails rather than merging series. */
    public void testInvalidSourceFails() {
        for (String invalid : List.of("not JSON", "[]", "{\"pod\":[{}]}", "{} {}", "{\"labels\":{\"pod\":\"a\"},\"pod\":\"b\"}")) {
            expectThrows(IllegalArgumentException.class, () -> convert(new BytesRef(invalid)));
        }
    }

    private void convert(BytesRef source) {
        try (
            ExpressionEvaluator eval = evaluator(new ToTimeSeries(Source.EMPTY, field("source", DataType.KEYWORD))).get(driverContext());
            Block block = eval.eval(row(List.of(source)))
        ) {
            fail("expected a failure, got " + toJavaObject(block, 0));
        }
    }
}
