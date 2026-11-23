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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
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
        String labels = "{\"job\":\"api\",\"pod\":\"a\"}";
        suppliers.add(set("before every label", labels, "env", "prod", "{\"env\":\"prod\",\"job\":\"api\",\"pod\":\"a\"}"));
        suppliers.add(set("between labels", labels, "lab", "x", "{\"job\":\"api\",\"lab\":\"x\",\"pod\":\"a\"}"));
        suppliers.add(set("after every label", labels, "zone", "z", "{\"job\":\"api\",\"pod\":\"a\",\"zone\":\"z\"}"));
        suppliers.add(set("replaces a label", labels, "pod", "b", "{\"job\":\"api\",\"pod\":\"b\"}"));
        suppliers.add(set("empty value removes the label", labels, "pod", "", "{\"job\":\"api\"}"));
        suppliers.add(set("empty value of an absent label", labels, "env", "", labels));
        suppliers.add(set("empty label container", "{}", "pod", "a", "{\"pod\":\"a\"}"));
        suppliers.add(set("replaces several values", "{\"pod\":[\"b\",\"a\"]}", "pod", "c", "{\"pod\":\"c\"}"));
        suppliers.add(
            set(
                "escaped name and value",
                labels,
                "label\"\\",
                "value\"\\\n\u03bb\ud83d\ude00",
                "{\"job\":\"api\",\"label\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\",\"pod\":\"a\"}"
            )
        );
        suppliers.add(new TestCaseSupplier("several values", List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef(labels), DataType.KEYWORD, "labelContainer"),
                new TestCaseSupplier.TypedData(new BytesRef("pod"), DataType.KEYWORD, "label").forceLiteral(),
                new TestCaseSupplier.TypedData(List.of(new BytesRef("b"), new BytesRef(""), new BytesRef("c")), DataType.KEYWORD, "value")
            );
            return new TestCaseSupplier.TestCase(
                data,
                evaluator("pod"),
                DataType.KEYWORD,
                equalTo(new BytesRef("{\"job\":\"api\",\"pod\":[\"b\",\"c\"]}"))
            );
        }));
        // The label container is the source document: objects nest as in the mapping, and a label is written at its path.
        String source = "{\"labels\":{\"job\":\"api\",\"pod\":\"a\"},\"region\":\"eu\"}";
        suppliers.add(
            set(
                "into its object",
                source,
                "cluster",
                "prod",
                "{\"labels\":{\"cluster\":\"prod\",\"job\":\"api\",\"pod\":\"a\"},\"region\":\"eu\"}"
            )
        );
        suppliers.add(
            set("replaces a nested label", source, "job", "web", "{\"labels\":{\"job\":\"web\",\"pod\":\"a\"},\"region\":\"eu\"}")
        );
        suppliers.add(
            set("outside its object", source, "zone", "z", "{\"labels\":{\"job\":\"api\",\"pod\":\"a\"},\"region\":\"eu\",\"zone\":\"z\"}")
        );
        suppliers.add(
            set("creates its object", "{\"region\":\"eu\"}", "cluster", "prod", "{\"labels\":{\"cluster\":\"prod\"},\"region\":\"eu\"}")
        );
        suppliers.add(
            set("leaves out an emptied object", "{\"labels\":{\"job\":\"api\"},\"region\":\"eu\"}", "job", "", "{\"region\":\"eu\"}")
        );
        suppliers.add(
            set(
                "moves a label to its path",
                "{\"job\":\"api\",\"region\":\"eu\"}",
                "job",
                "web",
                "{\"labels\":{\"job\":\"web\"},\"region\":\"eu\"}"
            )
        );
        return parameterSuppliersFromTypedData(suppliers);
    }

    /** The field a label is written to: {@code cluster} and {@code job} sit in a {@code labels} object, as remote write puts them. */
    private static List<String> path(String label) {
        return label.equals("cluster") || label.equals("job") ? List.of("labels", label) : List.of(label);
    }

    private static String evaluator(String label) {
        return "TimeSeriesMetadataSetEvaluator[labelContainer=Attribute[channel=0], label="
            + label
            + ", path="
            + path(label)
            + ", value=Attribute[channel=1]]";
    }

    private static TestCaseSupplier set(String name, String labelContainer, String label, String value, String expected) {
        return new TestCaseSupplier(name, List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef(labelContainer), DataType.KEYWORD, "labelContainer"),
                new TestCaseSupplier.TypedData(new BytesRef(label), DataType.KEYWORD, "label").forceLiteral(),
                new TestCaseSupplier.TypedData(new BytesRef(value), DataType.KEYWORD, "value")
            );
            return new TestCaseSupplier.TestCase(data, evaluator(label), DataType.KEYWORD, equalTo(new BytesRef(expected)));
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return set(source, args);
    }

    /** The function over {@code args}, writing a constant label at its {@link #path}. */
    static TimeSeriesMetadataSet set(Source source, List<Expression> args) {
        Expression label = args.get(1);
        List<String> path = label.foldable() && label.fold(FoldContext.small()) instanceof BytesRef name
            ? path(name.utf8ToString())
            : List.of("label");
        return new TimeSeriesMetadataSet(source, args.get(0), label, args.get(2), path);
    }

    /** A missing label container is the empty one and a missing value removes the label: the result is the empty container. */
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(new BytesRef("{}"));
    }

    /** The label must be a non-empty constant string, so the evaluator folds it once. */
    public void testLabelMustBeNonEmptyConstantString() {
        Expression labelContainer = field("labelContainer", DataType.KEYWORD);
        Expression value = field("value", DataType.KEYWORD);
        for (Expression label : List.of(
            Literal.integer(Source.EMPTY, 42),
            Literal.keyword(Source.EMPTY, ""),
            Literal.NULL,
            field("label", DataType.KEYWORD)
        )) {
            assertTrue(
                label.toString(),
                new TimeSeriesMetadataSet(Source.EMPTY, labelContainer, label, value, List.of("label")).typeResolved().unresolved()
            );
        }
    }

    /** The path must name the label it writes, or a label written back would land apart from where the source holds it. */
    public void testPathMustNameTheLabel() {
        Expression labelContainer = field("labelContainer", DataType.KEYWORD);
        Expression value = field("value", DataType.KEYWORD);
        Expression pod = Literal.keyword(Source.EMPTY, "pod");
        assertTrue(new TimeSeriesMetadataSet(Source.EMPTY, labelContainer, pod, value, List.of("labels", "pod")).typeResolved().resolved());
        assertTrue(new TimeSeriesMetadataSet(Source.EMPTY, labelContainer, pod, value, List.of("pod")).typeResolved().resolved());
        for (List<String> path : List.<List<String>>of(List.of(), List.of("job"), List.of("k8s", "pod"))) {
            assertTrue(
                path.toString(),
                new TimeSeriesMetadataSet(Source.EMPTY, labelContainer, pod, value, path).typeResolved().unresolved()
            );
        }
    }
}
