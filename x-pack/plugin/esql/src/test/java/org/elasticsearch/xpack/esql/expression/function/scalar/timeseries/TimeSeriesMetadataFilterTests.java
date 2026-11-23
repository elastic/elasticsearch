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

public class TimeSeriesMetadataFilterTests extends AbstractScalarFunctionTestCase {
    public TimeSeriesMetadataFilterTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
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
        // The label container is the source document: objects nest as in the mapping, and a label is named by its field path.
        String source = "{\"k8s\":{\"node\":\"n\",\"zone\":\"z\"},\"labels\":{\"job\":\"api\"},\"pod\":\"a\"}";
        suppliers.add(
            keep("source document", source, List.of("job", "k8s.node"), "{\"k8s\":{\"node\":\"n\"},\"labels\":{\"job\":\"api\"}}")
        );
        suppliers.add(keep("source document, emptied objects", source, List.of("pod"), "{\"pod\":\"a\"}"));
        suppliers.add(keep("source document, no label", source, List.of("missing"), "{}"));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier keep(String name, String labelContainer, List<String> labels, String expected) {
        List<DataType> types = new ArrayList<>();
        types.add(DataType.KEYWORD);
        labels.forEach(label -> types.add(DataType.KEYWORD));
        return new TestCaseSupplier(name, types, () -> {
            List<TestCaseSupplier.TypedData> data = new ArrayList<>();
            data.add(new TestCaseSupplier.TypedData(new BytesRef(labelContainer), DataType.KEYWORD, "labelContainer"));
            labels.forEach(label -> data.add(new TestCaseSupplier.TypedData(new BytesRef(label), DataType.KEYWORD, label).forceLiteral()));
            String evaluator = "TimeSeriesMetadataFilterEvaluator[labelContainer=Attribute[channel=0], labels="
                + labels
                + ", exclude=false]";
            return new TestCaseSupplier.TestCase(data, evaluator, DataType.KEYWORD, equalTo(new BytesRef(expected)));
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesMetadataFilter(source, args.getFirst(), args.subList(1, args.size()), false);
    }

    /** A missing label container is the empty one, so keeping labels of it leaves it empty rather than null. */
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(new BytesRef("{}"));
    }

    /** Label names must be non-empty constant strings, so the evaluator folds them once. */
    public void testLabelsMustBeNonEmptyConstantStrings() {
        Expression labelContainer = field("labelContainer", DataType.KEYWORD);
        for (Expression label : List.of(
            Literal.integer(Source.EMPTY, 42),
            Literal.keyword(Source.EMPTY, ""),
            Literal.NULL,
            field("label", DataType.KEYWORD)
        )) {
            var transform = new TimeSeriesMetadataFilter(
                Source.EMPTY,
                labelContainer,
                List.of(Literal.keyword(Source.EMPTY, "pod"), label),
                false
            );
            assertTrue(label.toString(), transform.typeResolved().unresolved());
        }
    }

    /** Only a keyword {@code _timeseries} can be rewritten. */
    public void testLabelContainerMustBeKeyword() {
        var transform = new TimeSeriesMetadataFilter(
            Source.EMPTY,
            field("labelContainer", DataType.LONG),
            List.of(Literal.keyword(Source.EMPTY, "pod")),
            false
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
        var transform = new TimeSeriesMetadataFilter(
            Source.EMPTY,
            field("labelContainer", DataType.KEYWORD),
            List.of(Literal.keyword(Source.EMPTY, "pod")),
            false
        );
        try (
            ExpressionEvaluator eval = evaluator(transform).get(driverContext());
            Block block = eval.eval(row(Collections.singletonList(labelContainer)))
        ) {
            fail("expected a failure, got " + toJavaObject(block, 0));
        }
    }
}
