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

public class TimeSeriesMetadataPathTests extends AbstractScalarFunctionTestCase {
    public TimeSeriesMetadataPathTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType type : TimeSeriesMetadataFilterTests.containerTypes()) {
            for (DataType labelType : DataType.stringTypes()) {
                suppliers.addAll(cases(type, labelType));
            }
        }
        // A keyword container is the source document: nested, prefixed and in any order, read into the canonical form.
        String source = "{\"pod\":\"a\",\"labels\":{\"job\":\"api\"},\"k8s\":{\"node\":[\"n\",\"\",\"m\"]}}";
        suppliers.add(read("prefixed label", DataType.KEYWORD, DataType.KEYWORD, source, "job", equalTo(new BytesRef("api"))));
        suppliers.add(
            read(
                "nested label",
                DataType.KEYWORD,
                DataType.KEYWORD,
                source,
                "k8s.node",
                equalTo(List.of(new BytesRef("n"), new BytesRef("m")))
            )
        );
        suppliers.add(read("prefix is not a label", DataType.KEYWORD, DataType.KEYWORD, source, "labels.job", nullValue()));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static List<TestCaseSupplier> cases(DataType type, DataType labelType) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(read("single value", type, labelType, "{\"job\":\"api\",\"pod\":\"a\"}", "pod", equalTo(new BytesRef("a"))));
        suppliers.add(read("absent label", type, labelType, "{\"job\":\"api\"}", "pod", nullValue()));
        suppliers.add(read("empty label container", type, labelType, "{}", "pod", nullValue()));
        suppliers.add(
            read(
                "multiple values",
                type,
                labelType,
                "{\"pod\":[\"b\",\"a\",\"b\"]}",
                "pod",
                equalTo(List.of(new BytesRef("b"), new BytesRef("a"), new BytesRef("b")))
            )
        );
        suppliers.add(
            read(
                "escaped name and value",
                type,
                labelType,
                "{\"label\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\"}",
                "label\"\\",
                equalTo(new BytesRef("value\"\\\n\u03bb\ud83d\ude00"))
            )
        );
        return suppliers;
    }

    private static TestCaseSupplier read(
        String name,
        DataType type,
        DataType labelType,
        String labelContainer,
        String label,
        Matcher<?> expected
    ) {
        return new TestCaseSupplier(name, List.of(type, labelType), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef(labelContainer), type, "labelContainer"),
                new TestCaseSupplier.TypedData(new BytesRef(label), labelType, "label").forceLiteral()
            );
            String evaluator = "TimeSeriesMetadataFunctionEvaluator[operation=GET"
                + (type == DataType.KEYWORD ? ", keyword" : "")
                + ", names=["
                + label
                + "], labelContainer=Attribute[channel=0]]";
            return new TestCaseSupplier.TestCase(data, evaluator, DataType.KEYWORD, expected);
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesMetadataPath(source, args.get(0), args.get(1));
    }

    /** A label name must be a non-empty constant string, so the evaluator folds it once. */
    public void testLabelMustBeNonEmptyConstantString() {
        Expression labelContainer = field("labelContainer", DataType.TIME_SERIES_METADATA_DATA_TYPE);
        for (Expression label : List.of(
            Literal.integer(Source.EMPTY, 42),
            Literal.keyword(Source.EMPTY, ""),
            Literal.NULL,
            field("label", DataType.KEYWORD)
        )) {
            assertTrue(label.toString(), new TimeSeriesMetadataPath(Source.EMPTY, labelContainer, label).typeResolved().unresolved());
        }
    }

    /** Only a label container can be read, typed or keyword. */
    public void testLabelContainerMustBeOne() {
        var path = new TimeSeriesMetadataPath(Source.EMPTY, field("labelContainer", DataType.LONG), Literal.keyword(Source.EMPTY, "pod"));
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
        var path = new TimeSeriesMetadataPath(
            Source.EMPTY,
            field("labelContainer", randomFrom(TimeSeriesMetadataFilterTests.containerTypes())),
            Literal.keyword(Source.EMPTY, "pod")
        );
        try (
            ExpressionEvaluator eval = evaluator(path).get(driverContext());
            Block block = eval.eval(row(Collections.singletonList(labelContainer)))
        ) {
            fail("expected a failure, got " + toJavaObject(block, 0));
        }
    }
}
