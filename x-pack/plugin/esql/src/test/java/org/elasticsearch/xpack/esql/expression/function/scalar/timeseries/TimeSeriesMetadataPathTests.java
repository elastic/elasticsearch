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
        for (DataType keyType : DataType.stringTypes()) {
            suppliers.add(read("single value", keyType, "{\"job\":\"api\",\"pod\":\"a\"}", "pod", equalTo(new BytesRef("a"))));
            suppliers.add(read("absent key", keyType, "{\"job\":\"api\"}", "pod", nullValue()));
            suppliers.add(read("empty metadata", keyType, "{}", "pod", nullValue()));
            suppliers.add(
                read(
                    "multiple values",
                    keyType,
                    "{\"pod\":[\"b\",\"a\",\"b\"]}",
                    "pod",
                    equalTo(List.of(new BytesRef("b"), new BytesRef("a"), new BytesRef("b")))
                )
            );
            suppliers.add(
                read(
                    "escaped name and value",
                    keyType,
                    "{\"key\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\"}",
                    "key\"\\",
                    equalTo(new BytesRef("value\"\\\n\u03bb\ud83d\ude00"))
                )
            );
        }
        // The metadata is the source document: objects nest as in the mapping, and a key is named by its field path.
        String source = "{\"k8s\":{\"node\":[\"m\",\"n\"]},\"labels\":{\"job\":\"api\"},\"pod\":\"a\"}";
        suppliers.add(read("prefixed key", DataType.KEYWORD, source, "job", equalTo(new BytesRef("api"))));
        suppliers.add(read("nested key", DataType.KEYWORD, source, "k8s.node", equalTo(List.of(new BytesRef("m"), new BytesRef("n")))));
        suppliers.add(read("prefix is not a key", DataType.KEYWORD, source, "labels.job", nullValue()));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier read(String name, DataType keyType, String metadata, String key, Matcher<?> expected) {
        return new TestCaseSupplier(name, List.of(DataType.KEYWORD, keyType), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef(metadata), DataType.KEYWORD, "metadata"),
                new TestCaseSupplier.TypedData(new BytesRef(key), keyType, "key").forceLiteral()
            );
            String evaluator = "TimeSeriesMetadataPathEvaluator[metadata=Attribute[channel=0], key=" + key + "]";
            return new TestCaseSupplier.TestCase(data, evaluator, DataType.KEYWORD, expected);
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesMetadataPath(source, args.get(0), args.get(1));
    }

    /** A key name must be a non-empty constant string, so the evaluator folds it once. */
    public void testKeyMustBeNonEmptyConstantString() {
        Expression metadata = field("metadata", DataType.KEYWORD);
        for (Expression key : List.of(
            Literal.integer(Source.EMPTY, 42),
            Literal.keyword(Source.EMPTY, ""),
            Literal.NULL,
            field("key", DataType.KEYWORD)
        )) {
            assertTrue(key.toString(), new TimeSeriesMetadataPath(Source.EMPTY, metadata, key).typeResolved().unresolved());
        }
    }

    /** Only a keyword {@code _timeseries} can be read. */
    public void testMetadataMustBeKeyword() {
        var path = new TimeSeriesMetadataPath(Source.EMPTY, field("metadata", DataType.LONG), Literal.keyword(Source.EMPTY, "pod"));
        assertThat(path.typeResolved().message(), containsString("_timeseries"));
    }

    /** Metadata that is not a JSON object fails rather than reading as if the key were absent. */
    public void testMalformedMetadataFails() {
        for (String malformed : List.of("not JSON", "[]")) {
            expectThrows(IllegalArgumentException.class, () -> read(new BytesRef(malformed)));
        }
    }

    /** Metadata is one value, even when the keys inside it hold several. */
    public void testMultivaluedMetadataFails() {
        var e = expectThrows(IllegalArgumentException.class, () -> read(List.of(new BytesRef("{}"), new BytesRef("{}"))));
        assertThat(e.getMessage(), containsString("must be single-valued"));
    }

    private void read(Object metadata) {
        var path = new TimeSeriesMetadataPath(Source.EMPTY, field("metadata", DataType.KEYWORD), Literal.keyword(Source.EMPTY, "pod"));
        try (
            ExpressionEvaluator eval = evaluator(path).get(driverContext());
            Block block = eval.eval(row(Collections.singletonList(metadata)))
        ) {
            fail("expected a failure, got " + toJavaObject(block, 0));
        }
    }
}
