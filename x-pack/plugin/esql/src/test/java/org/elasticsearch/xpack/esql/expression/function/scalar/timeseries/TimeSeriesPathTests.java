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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesPathTests extends AbstractScalarFunctionTestCase {
    public TimeSeriesPathTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType labelType : DataType.stringTypes()) {
            suppliers.add(read("single value", labelType, "{\"job\":\"api\",\"pod\":\"a\"}", "pod", equalTo(new BytesRef("a"))));
            suppliers.add(read("absent label", labelType, "{\"job\":\"api\"}", "pod", nullValue()));
            suppliers.add(read("empty label container", labelType, "{}", "pod", nullValue()));
            suppliers.add(
                read(
                    "multiple values",
                    labelType,
                    "{\"pod\":[\"b\",\"a\",\"b\"]}",
                    "pod",
                    equalTo(List.of(new BytesRef("b"), new BytesRef("a"), new BytesRef("b")))
                )
            );
            suppliers.add(
                read(
                    "escaped name and value",
                    labelType,
                    "{\"label\\\"\\\\\":\"value\\\"\\\\\\nλ😀\"}",
                    "label\"\\",
                    equalTo(new BytesRef("value\"\\\nλ😀"))
                )
            );
        }
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier read(String name, DataType labelType, String labelContainer, String label, Matcher<?> expected) {
        return new TestCaseSupplier(name, List.of(DataType.TSMD_DATA_TYPE, labelType), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef(labelContainer), DataType.TSMD_DATA_TYPE, "labelContainer"),
                new TestCaseSupplier.TypedData(new BytesRef(label), labelType, "label").forceLiteral()
            );
            String evaluator = "TimeSeriesEvaluator[operation=GET, names=[" + label + "], labelContainer=Attribute[channel=0]]";
            return new TestCaseSupplier.TestCase(data, evaluator, DataType.KEYWORD, expected);
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesPath(source, args.get(0), args.get(1));
    }

    /** A label name must be a non-empty constant string, so the evaluator folds it once. */
    public void testLabelMustBeNonEmptyConstantString() {
        Expression labelContainer = field("labelContainer", DataType.TSMD_DATA_TYPE);
        for (Expression label : List.of(
            Literal.integer(Source.EMPTY, 42),
            Literal.keyword(Source.EMPTY, ""),
            Literal.NULL,
            field("label", DataType.KEYWORD)
        )) {
            assertTrue(label.toString(), new TimeSeriesPath(Source.EMPTY, labelContainer, label).typeResolved().unresolved());
        }
    }

    /** Only a typed label container can be read: its keyword form is not one. */
    public void testLabelContainerMustBeTyped() {
        var path = new TimeSeriesPath(Source.EMPTY, field("labelContainer", DataType.KEYWORD), Literal.keyword(Source.EMPTY, "pod"));
        assertThat(path.typeResolved().message(), containsString("_timeseries"));
    }

    /** A label container that is not a JSON object fails rather than reading as if the label were absent. */
    public void testMalformedLabelContainerFails() {
        for (String malformed : List.of("not JSON", "[]")) {
            expectThrows(IllegalArgumentException.class, () -> read(new BytesRef(malformed)));
        }
    }

    /** A label container is one value, even when the labels inside it hold several. */
    public void testMultivaluedLabelContainerFails() {
        var e = expectThrows(IllegalArgumentException.class, () -> read(List.of(new BytesRef("{}"), new BytesRef("{}"))));
        assertThat(e.getMessage(), containsString("must be single-valued"));
    }

    private void read(Object labelContainer) {
        var path = new TimeSeriesPath(Source.EMPTY, field("labelContainer", DataType.TSMD_DATA_TYPE), Literal.keyword(Source.EMPTY, "pod"));
        try (
            ExpressionEvaluator eval = evaluator(path).get(driverContext());
            Block block = eval.eval(row(Collections.singletonList(labelContainer)))
        ) {
            fail("expected a failure, got " + toJavaObject(block, 0));
        }
    }
}
