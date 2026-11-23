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
        String keys = "{\"job\":\"api\",\"pod\":\"a\"}";
        suppliers.add(set("before every key", keys, "env", "prod", "{\"env\":\"prod\",\"job\":\"api\",\"pod\":\"a\"}"));
        suppliers.add(set("between keys", keys, "lab", "x", "{\"job\":\"api\",\"lab\":\"x\",\"pod\":\"a\"}"));
        suppliers.add(set("after every key", keys, "zone", "z", "{\"job\":\"api\",\"pod\":\"a\",\"zone\":\"z\"}"));
        suppliers.add(set("replaces a key", keys, "pod", "b", "{\"job\":\"api\",\"pod\":\"b\"}"));
        suppliers.add(set("empty value removes the key", keys, "pod", "", "{\"job\":\"api\"}"));
        suppliers.add(set("empty value of an absent key", keys, "env", "", keys));
        suppliers.add(set("empty metadata", "{}", "pod", "a", "{\"pod\":\"a\"}"));
        suppliers.add(set("replaces several values", "{\"pod\":[\"b\",\"a\"]}", "pod", "c", "{\"pod\":\"c\"}"));
        suppliers.add(
            set(
                "escaped name and value",
                keys,
                "key\"\\",
                "value\"\\\n\u03bb\ud83d\ude00",
                "{\"job\":\"api\",\"key\\\"\\\\\":\"value\\\"\\\\\\n\u03bb\ud83d\ude00\",\"pod\":\"a\"}"
            )
        );
        suppliers.add(new TestCaseSupplier("several values", List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef(keys), DataType.KEYWORD, "metadata"),
                new TestCaseSupplier.TypedData(new BytesRef("pod"), DataType.KEYWORD, "key").forceLiteral(),
                new TestCaseSupplier.TypedData(List.of(new BytesRef("b"), new BytesRef(""), new BytesRef("c")), DataType.KEYWORD, "value")
            );
            return new TestCaseSupplier.TestCase(
                data,
                evaluator("pod"),
                DataType.KEYWORD,
                equalTo(new BytesRef("{\"job\":\"api\",\"pod\":[\"b\",\"c\"]}"))
            );
        }));
        // The metadata is the source document: objects nest as in the mapping, and a key is written at its path.
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
        suppliers.add(set("replaces a nested key", source, "job", "web", "{\"labels\":{\"job\":\"web\",\"pod\":\"a\"},\"region\":\"eu\"}"));
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
                "moves a key to its path",
                "{\"job\":\"api\",\"region\":\"eu\"}",
                "job",
                "web",
                "{\"labels\":{\"job\":\"web\"},\"region\":\"eu\"}"
            )
        );
        return parameterSuppliersFromTypedData(suppliers);
    }

    /** The field a key is written to: {@code cluster} and {@code job} sit in a {@code labels} object, as remote write puts them. */
    private static List<String> path(String key) {
        return key.equals("cluster") || key.equals("job") ? List.of("labels", key) : List.of(key);
    }

    private static String evaluator(String key) {
        return "TimeSeriesMetadataSetEvaluator[metadata=Attribute[channel=0], key="
            + key
            + ", path="
            + path(key)
            + ", value=Attribute[channel=1]]";
    }

    private static TestCaseSupplier set(String name, String metadata, String key, String value, String expected) {
        return new TestCaseSupplier(name, List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef(metadata), DataType.KEYWORD, "metadata"),
                new TestCaseSupplier.TypedData(new BytesRef(key), DataType.KEYWORD, "key").forceLiteral(),
                new TestCaseSupplier.TypedData(new BytesRef(value), DataType.KEYWORD, "value")
            );
            return new TestCaseSupplier.TestCase(data, evaluator(key), DataType.KEYWORD, equalTo(new BytesRef(expected)));
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return set(source, args);
    }

    /** The function over {@code args}, writing a constant key at its {@link #path}. */
    static TimeSeriesMetadataSet set(Source source, List<Expression> args) {
        Expression key = args.get(1);
        List<String> path = key.foldable() && key.fold(FoldContext.small()) instanceof BytesRef name
            ? path(name.utf8ToString())
            : List.of("key");
        return new TimeSeriesMetadataSet(source, args.get(0), key, args.get(2), path);
    }

    /** A missing metadata is the empty one and a missing value removes the key: the result is the empty container. */
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(new BytesRef("{}"));
    }

    /** The key must be a non-empty constant string, so the evaluator folds it once. */
    public void testKeyMustBeNonEmptyConstantString() {
        Expression metadata = field("metadata", DataType.KEYWORD);
        Expression value = field("value", DataType.KEYWORD);
        for (Expression key : List.of(
            Literal.integer(Source.EMPTY, 42),
            Literal.keyword(Source.EMPTY, ""),
            Literal.NULL,
            field("key", DataType.KEYWORD)
        )) {
            assertTrue(
                key.toString(),
                new TimeSeriesMetadataSet(Source.EMPTY, metadata, key, value, List.of("key")).typeResolved().unresolved()
            );
        }
    }

    /** The path must name the key it writes, or a key written back would land apart from where the source holds it. */
    public void testPathMustNameTheKey() {
        Expression metadata = field("metadata", DataType.KEYWORD);
        Expression value = field("value", DataType.KEYWORD);
        Expression pod = Literal.keyword(Source.EMPTY, "pod");
        assertTrue(new TimeSeriesMetadataSet(Source.EMPTY, metadata, pod, value, List.of("labels", "pod")).typeResolved().resolved());
        assertTrue(new TimeSeriesMetadataSet(Source.EMPTY, metadata, pod, value, List.of("pod")).typeResolved().resolved());
        for (List<String> path : List.<List<String>>of(List.of(), List.of("job"), List.of("k8s", "pod"))) {
            assertTrue(path.toString(), new TimeSeriesMetadataSet(Source.EMPTY, metadata, pod, value, path).typeResolved().unresolved());
        }
    }
}
