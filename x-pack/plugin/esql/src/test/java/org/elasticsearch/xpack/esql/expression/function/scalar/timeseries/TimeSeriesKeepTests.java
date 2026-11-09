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
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesKeepTests extends AbstractScalarFunctionTestCase {
    public TimeSeriesKeepTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @BeforeClass
    public static void requireTimeSeriesType() {
        // Every case takes a _timeseries container, which release builds can't serialize while it is under construction.
        assumeTrue("_timeseries is under construction", DataType.TSMD_DATA_TYPE.supportedVersion().supportedLocally());
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        String labels = "{\"job\":\"api\",\"pod\":\"a\"}";
        suppliers.add(keep("one label", labels, List.of("pod"), "{\"pod\":\"a\"}"));
        suppliers.add(keep("repeated and absent labels", labels, List.of("pod", "pod", "missing"), "{\"pod\":\"a\"}"));
        suppliers.add(keep("absent label", labels, List.of("missing"), "{}"));
        suppliers.add(keep("every label", labels, List.of("pod", "job"), labels));
        suppliers.add(keep("empty label container", "{}", List.of("pod"), "{}"));
        suppliers.add(keep("multiple values", "{\"pod\":[\"b\",\"a\"],\"single\":\"one\"}", List.of("pod"), "{\"pod\":[\"b\",\"a\"]}"));
        suppliers.add(
            keep(
                "escaped name and value",
                "{\"job\":\"api\",\"label\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\"}",
                List.of("label\"\\"),
                "{\"label\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\"}"
            )
        );
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier keep(String name, String labelContainer, List<String> labels, String expected) {
        List<DataType> types = new ArrayList<>();
        types.add(DataType.TSMD_DATA_TYPE);
        labels.forEach(label -> types.add(DataType.KEYWORD));
        return new TestCaseSupplier(name, types, () -> {
            List<TestCaseSupplier.TypedData> data = new ArrayList<>();
            data.add(new TestCaseSupplier.TypedData(new BytesRef(labelContainer), DataType.TSMD_DATA_TYPE, "labelContainer"));
            labels.forEach(label -> data.add(new TestCaseSupplier.TypedData(new BytesRef(label), DataType.KEYWORD, label).forceLiteral()));
            String evaluator = "TimeSeriesEvaluator[operation=KEEP, names=" + labels + ", labelContainer=Attribute[channel=0]]";
            return new TestCaseSupplier.TestCase(data, evaluator, DataType.TSMD_DATA_TYPE, equalTo(new BytesRef(expected)));
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesKeep(source, args.getFirst(), args.subList(1, args.size()));
    }

    /** A missing label container is the empty one, so keeping labels of it leaves it empty rather than null. */
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(new BytesRef("{}"));
    }

    /** Label names must be non-empty constant strings, so the evaluator folds them once. */
    public void testLabelsMustBeNonEmptyConstantStrings() {
        Expression labelContainer = field("labelContainer", DataType.TSMD_DATA_TYPE);
        for (Expression label : List.of(
            Literal.integer(Source.EMPTY, 42),
            Literal.keyword(Source.EMPTY, ""),
            Literal.NULL,
            field("label", DataType.KEYWORD)
        )) {
            var transform = new TimeSeriesKeep(Source.EMPTY, labelContainer, List.of(Literal.keyword(Source.EMPTY, "pod"), label));
            assertTrue(label.toString(), transform.typeResolved().unresolved());
        }
    }

    /** Only a typed label container can be rewritten: its keyword form is not one. */
    public void testLabelContainerMustBeTyped() {
        var transform = new TimeSeriesKeep(
            Source.EMPTY,
            field("labelContainer", DataType.KEYWORD),
            List.of(Literal.keyword(Source.EMPTY, "pod"))
        );
        assertThat(transform.typeResolved().message(), containsString("_timeseries"));
    }

    /** A label container that is not one JSON object fails rather than dropping series' labels. */
    public void testMalformedLabelContainerFails() {
        for (String malformed : List.of("not JSON", "[]", "{} {}")) {
            expectThrows(IllegalArgumentException.class, () -> remove(new BytesRef(malformed)));
        }
    }

    /** A label container is one value, even when the labels inside it hold several. */
    public void testMultivaluedLabelContainerFails() {
        var e = expectThrows(IllegalArgumentException.class, () -> remove(List.of(new BytesRef("{}"), new BytesRef("{}"))));
        assertThat(e.getMessage(), containsString("must be single-valued"));
    }

    private void remove(Object labelContainer) {
        var transform = new TimeSeriesKeep(
            Source.EMPTY,
            field("labelContainer", DataType.TSMD_DATA_TYPE),
            List.of(Literal.keyword(Source.EMPTY, "pod"))
        );
        try (
            ExpressionEvaluator eval = evaluator(transform).get(driverContext());
            Block block = eval.eval(row(Collections.singletonList(labelContainer)))
        ) {
            fail("expected a failure, got " + toJavaObject(block, 0));
        }
    }
}
