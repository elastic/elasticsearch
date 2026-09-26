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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
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
            suppliers.add(new TestCaseSupplier("literal dotted key " + pathType.typeName(), types(DataType.FLATTENED, pathType), () -> {
                assumeTrue("Requires FIELD_EXTRACT_FUNCTION capability", EsqlCapabilities.Cap.FIELD_EXTRACT_FUNCTION.isEnabled());
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("{\"host.name\":\"node-a\"}"), DataType.FLATTENED, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef("host.name"), pathType, "path")
                    ),
                    "FieldExtractEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef("node-a"))
                );
            }));
        }

        suppliers.add(new TestCaseSupplier("constant dotted path", types(DataType.FLATTENED, DataType.KEYWORD), () -> {
            assumeTrue("Requires FIELD_EXTRACT_FUNCTION capability", EsqlCapabilities.Cap.FIELD_EXTRACT_FUNCTION.isEnabled());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("{\"k.inner\":\"v\"}"), DataType.FLATTENED, "field"),
                    new TestCaseSupplier.TypedData(new BytesRef("k.inner"), DataType.KEYWORD, "path").forceLiteral()
                ),
                "FieldExtractConstantEvaluator[flattenedJson=Attribute[channel=0], path=k.inner]",
                DataType.KEYWORD,
                equalTo(new BytesRef("v"))
            );
        }));

        suppliers.add(new TestCaseSupplier("missing path", types(DataType.FLATTENED, DataType.KEYWORD), () -> {
            assumeTrue("Requires FIELD_EXTRACT_FUNCTION capability", EsqlCapabilities.Cap.FIELD_EXTRACT_FUNCTION.isEnabled());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("{\"a\":1}"), DataType.FLATTENED, "field"),
                    new TestCaseSupplier.TypedData(new BytesRef("missing"), DataType.KEYWORD, "path")
                ),
                "FieldExtractEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: path [missing] does not exist");
        }));

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
                        assumeTrue("Requires FIELD_EXTRACT_FUNCTION capability", EsqlCapabilities.Cap.FIELD_EXTRACT_FUNCTION.isEnabled());
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
            assumeTrue("Requires FIELD_EXTRACT_FUNCTION capability", EsqlCapabilities.Cap.FIELD_EXTRACT_FUNCTION.isEnabled());
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

    /**
     * Returns the position-0 cell of the parse-path evaluator as a Java object. Multi-value
     * positions surface as a {@code List<BytesRef>}; single values as a {@code BytesRef};
     * null positions as {@code null}. Used by the multi-value tests below to assert both the
     * arity (single vs multi) and the elements without depending on block internals.
     */
    private Object extractFromBytesAsObject(BytesRef bytes, String path) {
        try (
            var eval = evaluator(new FieldExtract(Source.EMPTY, field("field", DataType.FLATTENED), field("path", DataType.KEYWORD))).get(
                driverContext()
            );
            Block block = eval.eval(row(List.of(bytes, new BytesRef(path))))
        ) {
            return block.isNull(0) ? null : BlockUtils.toJavaObject(block, 0);
        }
    }

    public void testLiteralDottedKeyMatchesFlatStorage() {
        // The doc-values shape of a flattened root: every leaf is one top-level key with a dotted name.
        // This test also pins down the literal-key semantics. If the implementation accidentally
        // treated `.` as a navigation separator, the "literal dotted key" supplier above would also
        // start failing, since its input has no nested object to walk into.
        assertThat(extractFromBytes(new BytesRef("{\"foo.bar.baz\":\"x\"}"), "foo.bar.baz"), equalTo("x"));
    }

    public void testMultiValueArrayOfStringsProducesMultiValuePosition() {
        // The CsvFlattenedKeywordIT variant wraps every multi-value keyword as {"v": [...]};
        // the parse path must produce a real multi-value keyword block to match what the
        // pushdown path returns from the keyed sub-field doc-values reader.
        Object result = extractFromBytesAsObject(new BytesRef("{\"v\":[\"a\",\"b\",\"c\"]}"), "v");
        assertThat(result, instanceOf(List.class));
        assertThat(((List<?>) result).stream().map(o -> ((BytesRef) o).utf8ToString()).toList(), equalTo(List.of("a", "b", "c")));
    }

    public void testMultiValueArrayOfNumbersProducesStringifiedMultiValue() {
        // Per the function's documented contract numbers come through as their string
        // representation. Multi-value numeric arrays must keep that promise per element.
        Object result = extractFromBytesAsObject(new BytesRef("{\"v\":[1,2,3.5]}"), "v");
        assertThat(result, instanceOf(List.class));
        assertThat(((List<?>) result).stream().map(o -> ((BytesRef) o).utf8ToString()).toList(), equalTo(List.of("1", "2", "3.5")));
    }

    public void testMultiValueArrayOfBooleansUsesCanonicalLiterals() {
        // Booleans are emitted via the TRUE_BYTES / FALSE_BYTES singletons rather than the
        // parser's text representation, matching the JsonExtract policy and avoiding any
        // chance of locale-dependent rendering.
        Object result = extractFromBytesAsObject(new BytesRef("{\"v\":[true,false,true]}"), "v");
        assertThat(result, instanceOf(List.class));
        assertThat(((List<?>) result).stream().map(o -> ((BytesRef) o).utf8ToString()).toList(), equalTo(List.of("true", "false", "true")));
    }

    public void testSingleElementArrayCollapsesToScalarPosition() {
        // Symmetric to CsvTestsDataLoader.parseDocument's single-element multi-value collapse:
        // a doc that started life as `[x]` round-trips through the parse path as the scalar
        // `x`, so MV_COUNT(field_extract(...)) returns 1 regardless of whether the underlying
        // sub-field stored the value as a list or as a scalar.
        Object result = extractFromBytesAsObject(new BytesRef("{\"v\":[\"only\"]}"), "v");
        assertThat(result, instanceOf(BytesRef.class));
        assertThat(((BytesRef) result).utf8ToString(), equalTo("only"));
    }

    public void testEmptyArrayProducesNullPosition() {
        // Empty arrays carry no value; the function's contract says "Returns null ... if no
        // sub-field with that name exists, or if the stored value is JSON null", and an empty
        // array is the natural extension of that "no value" reading.
        assertNull(extractFromBytesAsObject(new BytesRef("{\"v\":[]}"), "v"));
    }

    public void testJsonNullValueProducesNullPosition() {
        // VALUE_NULL was previously unhandled and crashed parser.text() on the BytesRef
        // constructor. Now it appends a null position, matching the documented behavior.
        assertNull(extractFromBytesAsObject(new BytesRef("{\"v\":null}"), "v"));
    }

    public void testJsonNullsInsideArrayAreDropped() {
        // Multi-value keyword positions cannot represent null elements. Dropping is the only
        // representable option that does not silently substitute an empty BytesRef for a null,
        // and is symmetrical with the empty-array-becomes-null rule below.
        Object result = extractFromBytesAsObject(new BytesRef("{\"v\":[\"a\",null,\"b\"]}"), "v");
        assertThat(result, instanceOf(List.class));
        assertThat(((List<?>) result).stream().map(o -> ((BytesRef) o).utf8ToString()).toList(), equalTo(List.of("a", "b")));
    }

    public void testArrayOfAllNullsCollapsesToNullPosition() {
        // After dropping null elements per testJsonNullsInsideArrayAreDropped, an
        // all-null array is indistinguishable from an empty array, so it collapses to null.
        assertNull(extractFromBytesAsObject(new BytesRef("{\"v\":[null,null]}"), "v"));
    }

    public void testNestedObjectAtSubKeyReturnsNull() {
        // The flattened mapper indexes leaves of a nested object under extended dotted keys
        // (here {@code v.a} and {@code v.b}), so the pushdown path sees no value at the
        // requested key {@code v}. The parse path is aligned to return null for the same
        // shape so the two paths produce interchangeable results.
        assertNull(extractFromBytesAsObject(new BytesRef("{\"v\":{\"a\":1,\"b\":\"x\"}}"), "v"));
    }

    public void testNestedObjectInsideArrayIsSkipped() {
        // Object elements of a multi-value sub-field are indexed under extended dotted keys,
        // so they are absent from the flat storage at the requested key. The parse path skips
        // them so the returned multi-value position lists only the scalar leaves that the
        // pushdown path would have read.
        Object result = extractFromBytesAsObject(new BytesRef("{\"v\":[\"x\",{\"k\":1},\"y\"]}"), "v");
        assertThat(result, instanceOf(List.class));
        assertThat(((List<?>) result).stream().map(o -> ((BytesRef) o).utf8ToString()).toList(), equalTo(List.of("x", "y")));
    }

    public void testArrayOfOnlyObjectsReturnsNull() {
        // A multi-value sub-field whose elements are all embedded objects has no leaves at
        // the requested key in the flat storage and the function returns null, matching how
        // the pushdown path would see no value.
        assertNull(extractFromBytesAsObject(new BytesRef("{\"v\":[{\"a\":1},{\"b\":2}]}"), "v"));
    }

    public void testNestedArrayInsideArrayFlattensScalars() {
        // The flattened mapper iterates nested arrays without extending the storage key, so
        // every scalar leaf ends up under the outer key. The parse path matches by recursing
        // into nested arrays and surfacing their scalar leaves in document order.
        Object result = extractFromBytesAsObject(new BytesRef("{\"v\":[[\"a\",\"b\"],\"c\"]}"), "v");
        assertThat(result, instanceOf(List.class));
        assertThat(((List<?>) result).stream().map(o -> ((BytesRef) o).utf8ToString()).toList(), equalTo(List.of("a", "b", "c")));
    }

    public void testNestedArrayContainingObjectsSkipsObjectsButFlattensScalars() {
        // Mixed structure: nested-array scalars survive (they share the outer key) but
        // object leaves inside the nested array do not (they live at extended dotted keys).
        Object result = extractFromBytesAsObject(new BytesRef("{\"v\":[[\"a\",{\"x\":1}],\"c\"]}"), "v");
        assertThat(result, instanceOf(List.class));
        assertThat(((List<?>) result).stream().map(o -> ((BytesRef) o).utf8ToString()).toList(), equalTo(List.of("a", "c")));
    }

    public void testMultiValueArrayPreservesOtherKeysWhenScanningPastTarget() {
        // The find-key loop must consume the entire matched-array sub-tree before returning;
        // a regression that returned mid-scan would corrupt the parser cursor for any siblings
        // (this matters for the constant-evaluator path, which reuses parsers under load).
        // Sanity-check by extracting the second of two keys; the result must reflect the
        // second key's array, not the first's.
        Object result = extractFromBytesAsObject(new BytesRef("{\"a\":[\"skip\"],\"v\":[\"x\",\"y\"]}"), "v");
        assertThat(result, instanceOf(List.class));
        assertThat(((List<?>) result).stream().map(o -> ((BytesRef) o).utf8ToString()).toList(), equalTo(List.of("x", "y")));
    }

    private static List<DataType> types(DataType firstType, DataType secondType) {
        return List.of(firstType, secondType);
    }
}
