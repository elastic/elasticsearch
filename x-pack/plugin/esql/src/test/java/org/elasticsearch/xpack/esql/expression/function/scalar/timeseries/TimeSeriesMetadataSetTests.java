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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesMetadataSetTests extends AbstractScalarFunctionTestCase {
    public TimeSeriesMetadataSetTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType type : TimeSeriesMetadataFilterTests.containerTypes()) {
            suppliers.addAll(cases(type));
        }
        // A keyword container is the source document: nested, prefixed and in any order, read into the canonical form.
        String source = "{\"pod\":\"a\",\"labels\":{\"job\":\"api\"}}";
        suppliers.add(set("source document", DataType.KEYWORD, source, "env", "prod", "{\"env\":\"prod\",\"job\":\"api\",\"pod\":\"a\"}"));
        suppliers.add(set("source document, prefixed label", DataType.KEYWORD, source, "job", "web", "{\"job\":\"web\",\"pod\":\"a\"}"));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static List<TestCaseSupplier> cases(DataType type) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        String labels = "{\"job\":\"api\",\"pod\":\"a\"}";
        suppliers.add(set("before every label", type, labels, "env", "prod", "{\"env\":\"prod\",\"job\":\"api\",\"pod\":\"a\"}"));
        suppliers.add(set("between labels", type, labels, "lab", "x", "{\"job\":\"api\",\"lab\":\"x\",\"pod\":\"a\"}"));
        suppliers.add(set("after every label", type, labels, "zone", "z", "{\"job\":\"api\",\"pod\":\"a\",\"zone\":\"z\"}"));
        suppliers.add(set("replaces a label", type, labels, "pod", "b", "{\"job\":\"api\",\"pod\":\"b\"}"));
        suppliers.add(set("empty value removes the label", type, labels, "pod", "", "{\"job\":\"api\"}"));
        suppliers.add(set("empty value of an absent label", type, labels, "env", "", labels));
        suppliers.add(set("empty label container", type, "{}", "pod", "a", "{\"pod\":\"a\"}"));
        suppliers.add(set("replaces several values", type, "{\"pod\":[\"b\",\"a\"]}", "pod", "c", "{\"pod\":\"c\"}"));
        suppliers.add(
            set(
                "escaped name and value",
                type,
                labels,
                "label\"\\",
                "value\"\\\n\u03bb\ud83d\ude00",
                "{\"job\":\"api\",\"label\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\",\"pod\":\"a\"}"
            )
        );
        suppliers.add(new TestCaseSupplier("several values", List.of(type, DataType.KEYWORD, DataType.KEYWORD), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef(labels), type, "labelContainer"),
                new TestCaseSupplier.TypedData(new BytesRef("pod"), DataType.KEYWORD, "label").forceLiteral(),
                new TestCaseSupplier.TypedData(List.of(new BytesRef("b"), new BytesRef(""), new BytesRef("c")), DataType.KEYWORD, "value")
            );
            return new TestCaseSupplier.TestCase(
                data,
                evaluator(type, "pod"),
                type,
                equalTo(new BytesRef("{\"job\":\"api\",\"pod\":[\"b\",\"c\"]}"))
            );
        }));
        return suppliers;
    }

    private static String evaluator(DataType type, String label) {
        return "TimeSeriesMetadataFunctionEvaluator[operation=SET"
            + (type == DataType.KEYWORD ? ", keyword" : "")
            + ", names=["
            + label
            + "], labelContainer=Attribute[channel=0], value=Attribute[channel=1]]";
    }

    private static TestCaseSupplier set(String name, DataType type, String labelContainer, String label, String value, String expected) {
        return new TestCaseSupplier(name, List.of(type, DataType.KEYWORD, DataType.KEYWORD), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef(labelContainer), type, "labelContainer"),
                new TestCaseSupplier.TypedData(new BytesRef(label), DataType.KEYWORD, "label").forceLiteral(),
                new TestCaseSupplier.TypedData(new BytesRef(value), DataType.KEYWORD, "value")
            );
            return new TestCaseSupplier.TestCase(data, evaluator(type, label), type, equalTo(new BytesRef(expected)));
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesMetadataSet(source, args.get(0), args.get(1), args.get(2));
    }

    /** A missing label container is the empty one and a missing value removes the label: the result is the empty container. */
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(new BytesRef("{}"));
    }

    /** The label must be a non-empty constant string, so the evaluator folds it once. */
    public void testLabelMustBeNonEmptyConstantString() {
        Expression labelContainer = field("labelContainer", DataType.TIME_SERIES_METADATA_DATA_TYPE);
        Expression value = field("value", DataType.KEYWORD);
        for (Expression label : List.of(
            Literal.integer(Source.EMPTY, 42),
            Literal.keyword(Source.EMPTY, ""),
            Literal.NULL,
            field("label", DataType.KEYWORD)
        )) {
            assertTrue(label.toString(), new TimeSeriesMetadataSet(Source.EMPTY, labelContainer, label, value).typeResolved().unresolved());
        }
    }
}
