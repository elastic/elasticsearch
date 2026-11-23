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

public class TimeSeriesMetadataFilterExcludeTests extends AbstractScalarFunctionTestCase {
    public TimeSeriesMetadataFilterExcludeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        String keys = "{\"job\":\"api\",\"pod\":\"a\"}";
        suppliers.add(remove("one key", keys, List.of("pod"), "{\"job\":\"api\"}"));
        suppliers.add(remove("repeated and absent keys", keys, List.of("pod", "pod", "missing"), "{\"job\":\"api\"}"));
        suppliers.add(remove("absent key", keys, List.of("missing"), keys));
        suppliers.add(remove("every key", keys, List.of("job", "pod"), "{}"));
        suppliers.add(remove("empty metadata", "{}", List.of("pod"), "{}"));
        suppliers.add(
            remove("multiple values", "{\"pod\":[\"b\",\"a\"],\"single\":\"one\"}", List.of("single"), "{\"pod\":[\"b\",\"a\"]}")
        );
        suppliers.add(
            remove(
                "escaped name and value",
                "{\"job\":\"api\",\"key\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\"}",
                List.of("job"),
                "{\"key\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\"}"
            )
        );
        // The metadata is the source document: objects nest as in the mapping, and a key is named by its field path.
        String source = "{\"k8s\":{\"node\":\"n\"},\"labels\":{\"job\":\"api\"},\"pod\":\"a\"}";
        suppliers.add(remove("source document", source, List.of("pod"), "{\"k8s\":{\"node\":\"n\"},\"labels\":{\"job\":\"api\"}}"));
        suppliers.add(remove("source document, emptied object", source, List.of("job"), "{\"k8s\":{\"node\":\"n\"},\"pod\":\"a\"}"));
        suppliers.add(remove("source document, no key", source, List.of(), source));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier remove(String name, String metadata, List<String> keys, String expected) {
        List<DataType> types = new ArrayList<>();
        types.add(DataType.KEYWORD);
        keys.forEach(key -> types.add(DataType.KEYWORD));
        return new TestCaseSupplier(name, types, () -> {
            List<TestCaseSupplier.TypedData> data = new ArrayList<>();
            data.add(new TestCaseSupplier.TypedData(new BytesRef(metadata), DataType.KEYWORD, "metadata"));
            keys.forEach(key -> data.add(new TestCaseSupplier.TypedData(new BytesRef(key), DataType.KEYWORD, key).forceLiteral()));
            String evaluator = "TimeSeriesMetadataFilterEvaluator[metadata=Attribute[channel=0], keys=" + keys + ", exclude=true]";
            return new TestCaseSupplier.TestCase(data, evaluator, DataType.KEYWORD, equalTo(new BytesRef(expected)));
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesMetadataFilter(source, args.getFirst(), args.subList(1, args.size()), true);
    }

    /** A missing metadata is the empty one, so removing keys from it leaves it empty rather than null. */
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(new BytesRef("{}"));
    }

    /** Key names must be non-empty constant strings, so the evaluator folds them once. */
    public void testKeysMustBeNonEmptyConstantStrings() {
        Expression metadata = field("metadata", DataType.KEYWORD);
        for (Expression key : List.of(
            Literal.integer(Source.EMPTY, 42),
            Literal.keyword(Source.EMPTY, ""),
            Literal.NULL,
            field("key", DataType.KEYWORD)
        )) {
            var transform = new TimeSeriesMetadataFilter(Source.EMPTY, metadata, List.of(Literal.keyword(Source.EMPTY, "pod"), key), true);
            assertTrue(key.toString(), transform.typeResolved().unresolved());
        }
    }

    /** Only a keyword {@code _timeseries} can be rewritten. */
    public void testMetadataMustBeKeyword() {
        var transform = new TimeSeriesMetadataFilter(
            Source.EMPTY,
            field("metadata", DataType.LONG),
            List.of(Literal.keyword(Source.EMPTY, "pod")),
            true
        );
        assertThat(transform.typeResolved().message(), containsString("_timeseries"));
    }

    /** Metadata that is not one JSON object fails rather than dropping series' keys. */
    public void testMalformedMetadataFails() {
        for (String malformed : List.of("not JSON", "[]", "{} {}")) {
            expectThrows(IllegalArgumentException.class, () -> remove(new BytesRef(malformed)));
        }
    }

    /** Metadata is one value, even when the keys inside it hold several. */
    public void testMultivaluedMetadataFails() {
        var e = expectThrows(IllegalArgumentException.class, () -> remove(List.of(new BytesRef("{}"), new BytesRef("{}"))));
        assertThat(e.getMessage(), containsString("must be single-valued"));
    }

    private void remove(Object metadata) {
        var transform = new TimeSeriesMetadataFilter(
            Source.EMPTY,
            field("metadata", DataType.KEYWORD),
            List.of(Literal.keyword(Source.EMPTY, "pod")),
            true
        );
        try (
            ExpressionEvaluator eval = evaluator(transform).get(driverContext());
            Block block = eval.eval(row(Collections.singletonList(metadata)))
        ) {
            fail("expected a failure, got " + toJavaObject(block, 0));
        }
    }
}
