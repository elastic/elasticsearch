/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class FieldExtractTests extends AbstractScalarFunctionTestCase {
    public FieldExtractTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType pathType : List.of(DataType.KEYWORD, DataType.TEXT)) {
            suppliers.add(
                new TestCaseSupplier(
                    "literal dotted key " + pathType.typeName(),
                    types(DataType.FLATTENED, pathType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("{\"host.name\":\"node-a\"}"), DataType.FLATTENED, "field"),
                            new TestCaseSupplier.TypedData(new BytesRef("host.name"), pathType, "path")
                        ),
                        "FieldExtractEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("node-a"))
                    )
                )
            );
        }

        suppliers.add(
            new TestCaseSupplier(
                "constant dotted path",
                types(DataType.FLATTENED, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("{\"k.inner\":\"v\"}"), DataType.FLATTENED, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef("k.inner"), DataType.KEYWORD, "path").forceLiteral()
                    ),
                    "FieldExtractConstantEvaluator[flattenedJson=Attribute[channel=0], path=k.inner]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef("v"))
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "missing path",
                types(DataType.FLATTENED, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("{\"a\":1}"), DataType.FLATTENED, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef("missing"), DataType.KEYWORD, "path")
                    ),
                    "FieldExtractEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                    DataType.KEYWORD,
                    nullValue()
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.IllegalArgumentException: path [missing] does not exist")
            )
        );

        addRandomizedMissingPathSuppliers(suppliers);

        addRandomizedMatchingPathSuppliers(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static void addRandomizedMissingPathSuppliers(List<TestCaseSupplier> suppliers) {
        for (TestCaseSupplier.TypedDataSupplier shape : TestCaseSupplier.flattenedCases()) {
            suppliers.add(
                new TestCaseSupplier(
                    "random " + shape.name() + " with non-existent path",
                    types(DataType.FLATTENED, DataType.KEYWORD),
                    () -> {
                        BytesRef json = (BytesRef) shape.get().getValue();
                        // FlattenedCases generates random keys of length 1-20, so any longer key is
                        // guaranteed not to collide. Pinning the lower bound at 25 keeps the
                        // guarantee even if the upstream max grows by a few characters.
                        String missingKey = randomAlphaOfLengthBetween(25, 35);
                        return new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(json, DataType.FLATTENED, "field"),
                                new TestCaseSupplier.TypedData(new BytesRef(missingKey), DataType.KEYWORD, "path")
                            ),
                            "FieldExtractEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                            DataType.KEYWORD,
                            nullValue()
                        ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                            .withWarning("Line 1:1: java.lang.IllegalArgumentException: path [" + missingKey + "] does not exist");
                    }
                )
            );
        }
    }

    private static void addRandomizedMatchingPathSuppliers(List<TestCaseSupplier> suppliers) {
        suppliers.add(matchingPathSupplier("random single-key flattened, matching path", false, FieldExtractTests::randomSingleKeyFlat));
        suppliers.add(
            matchingPathSupplier("random single-key flattened, matching literal path", true, FieldExtractTests::randomSingleKeyFlat)
        );
        suppliers.add(matchingPathSupplier("random multi-key flattened, matching path", false, FieldExtractTests::randomMultiKeyFlat));
        suppliers.add(
            matchingPathSupplier("random multi-key flattened, matching literal path", true, FieldExtractTests::randomMultiKeyFlat)
        );
    }

    private static TestCaseSupplier matchingPathSupplier(String name, boolean asLiteral, Supplier<FlatJsonWithKey> jsonGen) {
        return new TestCaseSupplier(name, types(DataType.FLATTENED, DataType.KEYWORD), () -> {
            FlatJsonWithKey flat = jsonGen.get();
            TestCaseSupplier.TypedData pathData = new TestCaseSupplier.TypedData(new BytesRef(flat.key()), DataType.KEYWORD, "path");
            if (asLiteral) {
                pathData = pathData.forceLiteral();
            }
            // The constant-path evaluator bakes the literal key into its toString, so the
            // expected string is derived from the same random key used to build the inputs.
            String expectedToString = asLiteral
                ? "FieldExtractConstantEvaluator[flattenedJson=Attribute[channel=0], path=" + flat.key() + "]"
                : "FieldExtractEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]";
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(flat.json(), DataType.FLATTENED, "field"), pathData),
                expectedToString,
                DataType.KEYWORD,
                equalTo(new BytesRef(flat.value()))
            );
        });
    }

    private record FlatJsonWithKey(BytesRef json, String key, String value) {}

    private static FlatJsonWithKey randomSingleKeyFlat() {
        String key = randomAlphaOfLengthBetween(1, 20);
        String value = randomAlphaOfLengthBetween(1, 20);
        return new FlatJsonWithKey(jsonOf(Map.of(key, value)), key, value);
    }

    private static FlatJsonWithKey randomMultiKeyFlat() {
        int n = randomIntBetween(2, 10);
        Map<String, String> kv = new LinkedHashMap<>();
        while (kv.size() < n) {
            kv.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        }
        String targetKey = randomFrom(new ArrayList<>(kv.keySet()));
        return new FlatJsonWithKey(jsonOf(kv), targetKey, kv.get(targetKey));
    }

    // randomAlphaOfLengthBetween yields ASCII letters only, so concatenation is safe (no JSON
    // escaping required). Insertion order is preserved by the LinkedHashMap input.
    private static BytesRef jsonOf(Map<String, String> kv) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : kv.entrySet()) {
            if (first == false) {
                sb.append(',');
            }
            sb.append('"').append(e.getKey()).append("\":\"").append(e.getValue()).append('"');
            first = false;
        }
        sb.append('}');
        return new BytesRef(sb.toString());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FieldExtract(source, args.get(0), args.get(1));
    }

    public void testConstantPathProducesConstantEvaluator() {
        Source source = Source.synthetic("field_extract");
        var evaluatorFactory = evaluator(
            new FieldExtract(source, field("field", DataType.FLATTENED), new Literal(source, new BytesRef("host.name"), DataType.KEYWORD))
        );
        assertThat(evaluatorFactory, instanceOf(FieldExtractConstantEvaluator.Factory.class));
    }

    public void testNonConstantPathProducesGenericEvaluator() {
        Source source = Source.synthetic("field_extract");
        var evaluatorFactory = evaluator(new FieldExtract(source, field("field", DataType.FLATTENED), field("path", DataType.KEYWORD)));
        assertThat(evaluatorFactory, instanceOf(FieldExtractEvaluator.Factory.class));
    }

    public void testFoldablePathWithArrayIndexFailsVerification() {
        var extract = new FieldExtract(
            Source.EMPTY,
            field("field", DataType.FLATTENED),
            new Literal(Source.EMPTY, new BytesRef("tags[0]"), DataType.KEYWORD)
        );
        assertTrue(extract.typeResolved().unresolved());
        assertThat(
            extract.typeResolved().message(),
            equalTo("field_extract path must be a literal flattened sub-field name. Brackets and array indices are not supported")
        );
    }

    public void testFoldablePathWithBracketsFailsVerification() {
        var extract = new FieldExtract(
            Source.EMPTY,
            field("field", DataType.FLATTENED),
            new Literal(Source.EMPTY, new BytesRef("['host.name']"), DataType.KEYWORD)
        );
        assertTrue(extract.typeResolved().unresolved());
        assertThat(
            extract.typeResolved().message(),
            equalTo("field_extract path must be a literal flattened sub-field name. Brackets and array indices are not supported")
        );
    }

    public void testFoldableEmptyPathFailsVerification() {
        var extract = new FieldExtract(
            Source.EMPTY,
            field("field", DataType.FLATTENED),
            new Literal(Source.EMPTY, new BytesRef(""), DataType.KEYWORD)
        );
        assertTrue(extract.typeResolved().unresolved());
        assertThat(extract.typeResolved().message(), equalTo("field_extract path must not be empty"));
    }

    private String extractFromBytes(BytesRef bytes, String path) {
        try (
            var eval = evaluator(new FieldExtract(Source.EMPTY, field("field", DataType.FLATTENED), field("path", DataType.KEYWORD))).get(
                driverContext()
            );
            Block block = eval.eval(row(List.of(bytes, new BytesRef(path))))
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
    }

    public void testLiteralDottedKeyMatchesFlatStorage() {
        // The doc-values shape of a flattened root: every leaf is one top-level key with a dotted name.
        // This test also pins down the literal-key semantics. If the implementation accidentally
        // treated `.` as a navigation separator, the "literal dotted key" supplier above would also
        // start failing, since its input has no nested object to walk into.
        assertThat(extractFromBytes(new BytesRef("{\"foo.bar.baz\":\"x\"}"), "foo.bar.baz"), equalTo("x"));
    }

    private static List<DataType> types(DataType firstType, DataType secondType) {
        return List.of(firstType, secondType);
    }
}
