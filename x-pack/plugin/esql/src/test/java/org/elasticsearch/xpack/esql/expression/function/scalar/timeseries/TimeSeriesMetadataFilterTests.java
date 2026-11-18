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
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesMetadataFilterTests extends AbstractScalarFunctionTestCase {
    public TimeSeriesMetadataFilterTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType type : containerTypes()) {
            suppliers.addAll(cases(type));
        }
        // A keyword container is the source document: nested, prefixed and in any order, read into the canonical form.
        String source = "{\"pod\":\"a\",\"labels\":{\"job\":\"api\"},\"k8s\":{\"node\":\"n\",\"empty\":\"\"}}";
        suppliers.add(
            keep("source document", DataType.KEYWORD, source, List.of("job", "k8s.node"), "{\"job\":\"api\",\"k8s.node\":\"n\"}")
        );
        suppliers.add(keep("source document, no label", DataType.KEYWORD, source, List.of("missing"), "{}"));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static List<TestCaseSupplier> cases(DataType type) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        String labels = "{\"job\":\"api\",\"pod\":\"a\"}";
        suppliers.add(keep("one label", type, labels, List.of("pod"), "{\"pod\":\"a\"}"));
        suppliers.add(keep("repeated and absent labels", type, labels, List.of("pod", "pod", "missing"), "{\"pod\":\"a\"}"));
        suppliers.add(keep("absent label", type, labels, List.of("missing"), "{}"));
        suppliers.add(keep("every label", type, labels, List.of("pod", "job"), labels));
        suppliers.add(keep("empty label container", type, "{}", List.of("pod"), "{}"));
        suppliers.add(
            keep("multiple values", type, "{\"pod\":[\"b\",\"a\"],\"single\":\"one\"}", List.of("pod"), "{\"pod\":[\"b\",\"a\"]}")
        );
        suppliers.add(
            keep(
                "escaped name and value",
                type,
                "{\"job\":\"api\",\"label\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\"}",
                List.of("label\"\\"),
                "{\"label\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\"}"
            )
        );
        return suppliers;
    }

    private static TestCaseSupplier keep(String name, DataType type, String labelContainer, List<String> labels, String expected) {
        List<DataType> types = new ArrayList<>();
        types.add(type);
        labels.forEach(label -> types.add(DataType.KEYWORD));
        return new TestCaseSupplier(name, types, () -> {
            List<TestCaseSupplier.TypedData> data = new ArrayList<>();
            data.add(new TestCaseSupplier.TypedData(container(labelContainer, type), type, "labelContainer"));
            labels.forEach(label -> data.add(new TestCaseSupplier.TypedData(new BytesRef(label), DataType.KEYWORD, label).forceLiteral()));
            String evaluator = "TimeSeriesMetadataFunctionEvaluator[operation=KEEP"
                + (type == DataType.KEYWORD ? ", keyword" : "")
                + ", names="
                + labels
                + ", labelContainer=Attribute[channel=0]]";
            return new TestCaseSupplier.TestCase(data, evaluator, type, equalTo(container(expected, type)));
        });
    }

    /**
     * The label container types: the keyword {@code _timeseries}, and the typed one where the build can serialize it, which
     * release builds can't while it is under construction.
     */
    static List<DataType> containerTypes() {
        return DataType.TIME_SERIES_METADATA_DATA_TYPE.supportedVersion().supportedLocally()
            ? List.of(DataType.KEYWORD, DataType.TIME_SERIES_METADATA_DATA_TYPE)
            : List.of(DataType.KEYWORD);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesMetadataFilter(source, args.getFirst(), args.subList(1, args.size()), false);
    }

    /** A missing label container is the empty one, so keeping labels of it leaves it empty rather than null. */
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return emptyContainer();
    }

    /** {@code json} as a label container of {@code type}: a keyword {@code _timeseries} is the JSON object itself. */
    static BytesRef container(String json, DataType type) {
        return type == DataType.KEYWORD ? new BytesRef(json) : TimeSeriesMetadataEncoding.fromSource(new BytesRef(json));
    }

    /** The empty label container of either type. */
    static Matcher<Object> emptyContainer() {
        return anyOf(equalTo(new BytesRef("{}")), equalTo(TimeSeriesMetadataEncoding.EMPTY));
    }

    /** Label names must be non-empty constant strings, so the evaluator folds them once. */
    public void testLabelsMustBeNonEmptyConstantStrings() {
        Expression labelContainer = field("labelContainer", DataType.TIME_SERIES_METADATA_DATA_TYPE);
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

    /** Only a label container can be rewritten, typed or keyword. */
    public void testLabelContainerMustBeOne() {
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
            field("labelContainer", randomFrom(containerTypes())),
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
