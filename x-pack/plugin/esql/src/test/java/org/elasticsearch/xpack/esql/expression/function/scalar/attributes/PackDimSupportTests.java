/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.promql.function.RegexExpand;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests the public expression boundary with real evaluators, including dictionary and external-column execution. */
public class PackDimSupportTests extends ComputeTestCase {
    private static final Source SOURCE = Source.EMPTY;

    public void testPackedTypeAndUnsetNames() throws Exception {
        assertSame(DataType.PACK_DIM, DataType.fromTypeName("pack_dim"));
        assertSame(DataType.PACK_DIM, DataType.readFrom("pack_dim"));
        var unset = PackDimSupport.unset(dimension("packed", DataType.PACK_DIM), List.of(dimension("a", DataType.KEYWORD)));
        assertEquals("PACK_DIM_UNSET", unset.functionName());
        assertTrue(unset.nodeString(), unset.nodeString().startsWith("PACK_DIM_UNSET("));
    }

    public void testPackPreservesNullEmptyAndLiteralNames() throws Exception {
        var expression = PackDimSupport.pack(
            SOURCE,
            List.of(
                named("z", null),
                named("a.b", ""),
                named("a", "parent"),
                named("\uD800\uDC00", "supplementary"),
                named("\uE000", "bmp")
            )
        );
        try (var packed = (PackDimBlock) evaluate(expression, List.of(), 3)) {
            var record = packed.getPackDim(packed.getFirstValueIndex(0), new PackDimValue());
            assertEquals(5, record.size());
            assertEquals(new BytesRef("null"), record.get(new BytesRef("z"), new BytesRef()));
            assertEquals(new BytesRef("\"\""), record.get(new BytesRef("a.b"), new BytesRef()));
            assertNull(record.get(new BytesRef("absent"), new BytesRef()));
            assertEquals(new BytesRef("\uE000"), record.nameAt(3, new BytesRef()));
            assertEquals(new BytesRef("\uD800\uDC00"), record.nameAt(4, new BytesRef()));
            assertEquals(1, packed.asOrdinalPackDim().getDictionarySize());
        }
    }

    public void testSetAndUnsetAreDifferentAndImmutable() throws Exception {
        var source = PackDimSupport.pack(SOURCE, List.of(named("a", "original"), named("a.b", "literal")));
        var nullSet = PackDimSupport.set(source, named("a", null));
        var emptySet = PackDimSupport.set(source, named("a", ""));
        var removed = PackDimSupport.unset(source, List.of(dimension("a", DataType.KEYWORD)));
        try (
            var original = (PackDimBlock) evaluate(source, List.of(), 2);
            var withNull = (PackDimBlock) evaluate(nullSet, List.of(), 2);
            var withEmpty = (PackDimBlock) evaluate(emptySet, List.of(), 2);
            var without = (PackDimBlock) evaluate(removed, List.of(), 2)
        ) {
            assertEquals(Map.of("a", "original", "a.b", "literal"), record(original, 0));
            assertTrue(record(withNull, 0).containsKey("a"));
            assertNull(record(withNull, 0).get("a"));
            assertEquals("", record(withEmpty, 0).get("a"));
            assertEquals(Map.of("a.b", "literal"), record(without, 0));
            assertEquals(2, without.getPositionCount());
        }
    }

    public void testMultipleUnsetsAndEmptyConstructor() throws Exception {
        var empty = PackDimSupport.pack(SOURCE, List.of());
        var full = PackDimSupport.pack(SOURCE, List.of(named("a", "x"), named("b", "y")));
        var removeAll = PackDimSupport.unset(
            full,
            List.of(dimension("missing", DataType.KEYWORD), dimension("b", DataType.KEYWORD), dimension("a", DataType.KEYWORD))
        );
        try (var first = (PackDimBlock) evaluate(empty, List.of(), 4); var second = (PackDimBlock) evaluate(removeAll, List.of(), 4)) {
            for (int p = 0; p < 4; p++) {
                assertFalse(first.isNull(p));
                assertFalse(second.isNull(p));
                assertEquals(0, first.getPackDim(first.getFirstValueIndex(p), new PackDimValue()).size());
                assertEquals(0, second.getPackDim(second.getFirstValueIndex(p), new PackDimValue()).size());
            }
        }
    }

    public void testGetTypedValuesAndUnpackNamedProjections() {
        List<Alias> values = List.of(
            new Alias(SOURCE, "integer", new Literal(SOURCE, 7, DataType.INTEGER)),
            new Alias(SOURCE, "long", new Literal(SOURCE, Long.MAX_VALUE, DataType.LONG)),
            new Alias(SOURCE, "double", new Literal(SOURCE, -0.0d, DataType.DOUBLE)),
            new Alias(SOURCE, "boolean", new Literal(SOURCE, false, DataType.BOOLEAN)),
            named("string", "")
        );
        var packed = PackDimSupport.pack(SOURCE, values);
        var outputs = PackDimSupport.unpack(packed, values.stream().map(Alias::toAttribute).toList());
        List<Object> expected = List.of(7, Long.MAX_VALUE, -0.0d, false, new BytesRef(""));
        for (int i = 0; i < values.size(); i++) {
            assertEquals(values.get(i).id(), outputs.get(i).id());
            assertEquals(values.get(i).dataType(), outputs.get(i).dataType());
            try (var actual = evaluate(outputs.get(i).child(), List.of(), 2)) {
                assertEquals(expected.get(i), BlockUtils.toJavaObject(actual, 0));
                assertEquals(expected.get(i), BlockUtils.toJavaObject(actual, 1));
            }
        }
    }

    public void testGetPreservesMultivalueOrderAndDuplicates() {
        var field = dimension("numbers", DataType.INTEGER);
        var packed = PackDimSupport.pack(SOURCE, List.of(field));
        try (var values = blockFactory().newIntBlockBuilder(2)) {
            values.beginPositionEntry().appendInt(3).appendInt(1).appendInt(3).endPositionEntry();
            values.appendNull();
            try (var input = values.build(); var output = evaluate(PackDimSupport.get(packed, field), List.of(field), 2, input)) {
                assertEquals(List.of(3, 1, 3), BlockUtils.toJavaObject(output, 0));
                assertTrue(output.isNull(1));
            }
        }
    }

    public void testGetOmitsNullMultivalueElementsWithoutCreatingPositions() {
        var ref = dimension("packed", DataType.PACK_DIM);
        var key = dimension("a", DataType.KEYWORD);
        try (var builder = blockFactory().newPackDimBlockBuilder(2)) {
            builder.append(new BytesRef[] { new BytesRef("a") }, new BytesRef[] { new BytesRef("[null,\"x\",null,\"x\"]") });
            builder.append(new BytesRef[] { new BytesRef("a") }, new BytesRef[] { new BytesRef("[null,null]") });
            try (var input = builder.build(); var result = evaluate(PackDimSupport.get(ref, key), List.of(ref), 2, input)) {
                assertEquals(2, result.getPositionCount());
                assertEquals(List.of(new BytesRef("x"), new BytesRef("x")), BlockUtils.toJavaObject(result, 0));
                assertTrue(result.isNull(1));
                assertTrue(input.getPackDim(input.getFirstValueIndex(1), new PackDimValue()).contains(new BytesRef("a")));
            }
        }
    }

    public void testGetDescriptorsAreNotColumnDependencies() {
        var packed = dimension("packed", DataType.PACK_DIM);
        var missingColumn = dimension("a.b", DataType.KEYWORD);
        var get = PackDimSupport.get(packed, missingColumn);
        var unset = PackDimSupport.unset(packed, List.of(missingColumn));
        assertEquals(packed.references(), get.references());
        assertEquals(packed.references(), unset.references());
        assertTrue(get.typeResolved().resolved());
    }

    public void testSetFromExternalColumnDoesNotCacheByRecordOnly() throws Exception {
        var packedRef = dimension("packed", DataType.PACK_DIM);
        var replacement = dimension("dst", DataType.KEYWORD);
        try (
            var packed = evaluate(PackDimSupport.pack(SOURCE, List.of(named("keep", "same"))), List.of(), 4);
            var values = strings("x", "y", "", null);
            var result = (PackDimBlock) evaluate(
                PackDimSupport.set(packedRef, replacement),
                List.of(packedRef, replacement),
                4,
                packed,
                values
            )
        ) {
            assertEquals("x", record(result, 0).get("dst"));
            assertEquals("y", record(result, 1).get("dst"));
            assertEquals("", record(result, 2).get("dst"));
            assertTrue(record(result, 3).containsKey("dst"));
            assertNull(record(result, 3).get("dst"));
        }
    }

    public void testExplicitDimensionOnlyConcatUsesRecordBatch() throws Exception {
        var packed = dimension("packed", DataType.PACK_DIM);
        var field = dimension("src", DataType.KEYWORD);
        var get = PackDimSupport.get(packed, field);
        var update = PackDimSupport.setFromDimensions(
            packed,
            new Alias(SOURCE, "dst", new Concat(SOURCE, get, List.of(Literal.keyword(SOURCE, "/"), get)))
        );
        var layout = new Layout.Builder().append(packed).build();
        var factory = (PackDimEvaluator.Factory) update.toEvaluator(EvalMapper.toEvaluatorContext(FoldContext.small(), layout, null, null));
        assertEquals(PackDimSupport.Operation.SET_FROM_DIMENSIONS, factory.expression().operation());
        try (
            var values = strings("eu", "us", "eu", "us", "eu");
            var input = (PackDimBlock) evaluate(PackDimSupport.pack(SOURCE, List.of(field)), List.of(field), 5, values);
            var result = (PackDimBlock) evaluate(update, List.of(packed), 5, input)
        ) {
            assertEquals("eu/eu", record(result, 0).get("dst"));
            assertEquals("us/us", record(result, 1).get("dst"));
            assertEquals(record(result, 0), record(result, 4));
            assertEquals(2, result.asOrdinalPackDim().getDictionarySize());
        }
    }

    public void testRegexReuseDoesNotImportPromqlDeletionSemantics() throws Exception {
        var packedRef = dimension("packed", DataType.PACK_DIM);
        var field = dimension("src", DataType.KEYWORD);
        var source = new Coalesce(SOURCE, PackDimSupport.get(packedRef, field), List.of(Literal.keyword(SOURCE, "")));
        var regex = new RegexExpand(SOURCE, source, Literal.keyword(SOURCE, "a"), Literal.keyword(SOURCE, ""));
        var update = PackDimSupport.setFromDimensions(packedRef, new Alias(SOURCE, "dst", regex));
        try (
            var values = strings("a", "b", "a");
            var packed = evaluate(PackDimSupport.pack(SOURCE, List.of(field)), List.of(field), 3, values);
            var result = (PackDimBlock) evaluate(update, List.of(packedRef), 3, packed)
        ) {
            assertEquals("", record(result, 0).get("dst"));
            assertTrue(record(result, 1).containsKey("dst"));
            assertNull(record(result, 1).get("dst"));
        }
    }

    public void testRecordScopedConditionalKeepsSetsAndUnsets() throws Exception {
        var packedRef = dimension("packed", DataType.PACK_DIM);
        var src = dimension("src", DataType.KEYWORD);
        var dst = dimension("dst", DataType.KEYWORD);
        var replacement = new RegexExpand(
            SOURCE,
            PackDimSupport.get(packedRef, src),
            Literal.keyword(SOURCE, "a(.*)"),
            Literal.keyword(SOURCE, "$1")
        );
        var changed = new Case(
            SOURCE,
            new Equals(SOURCE, replacement, Literal.keyword(SOURCE, "")),
            List.of(PackDimSupport.unset(packedRef, List.of(dst)), PackDimSupport.set(packedRef, new Alias(SOURCE, "dst", replacement)))
        );
        var update = PackDimSupport.mapFromDimensions(
            packedRef,
            new Case(SOURCE, new IsNull(SOURCE, replacement), List.of(packedRef, changed))
        );
        try (
            var values = strings("a", "b", "ax", "a", "ax", "b");
            var packed = evaluate(PackDimSupport.pack(SOURCE, List.of(src, named("dst", "old"))), List.of(src), 6, values);
            var result = (PackDimBlock) evaluate(update, List.of(packedRef), 6, packed)
        ) {
            assertFalse(record(result, 0).containsKey("dst"));
            assertEquals("old", record(result, 1).get("dst"));
            assertEquals("x", record(result, 2).get("dst"));
            assertEquals(record(result, 0), record(result, 3));
            assertEquals(record(result, 2), record(result, 4));
            assertEquals(3, result.asOrdinalPackDim().getDictionarySize());
        }
    }

    public void testNullPackedRecordsPropagateInsteadOfCreatingDimensions() {
        var packed = dimension("packed", DataType.PACK_DIM);
        try (var input = blockFactory().newConstantNullBlock(3)) {
            for (Expression expression : List.of(
                PackDimSupport.get(packed, dimension("a", DataType.KEYWORD)),
                PackDimSupport.set(packed, named("a", "value")),
                PackDimSupport.unset(packed, List.of(dimension("a", DataType.KEYWORD)))
            )) {
                try (var output = evaluate(expression, List.of(packed), 3, input)) {
                    assertTrue(output.areAllValuesNull());
                    assertEquals(3, output.getPositionCount());
                }
            }
        }
    }

    public void testRandomUpdatesAgainstMapOracle() throws Exception {
        var ref = dimension("packed", DataType.PACK_DIM);
        Map<String, Object> expected = new HashMap<>();
        PackDimBlock current = (PackDimBlock) evaluate(PackDimSupport.pack(SOURCE, List.of()), List.of(), 3);
        try {
            for (int i = 0; i < 50; i++) {
                String key = randomFrom("a", "a.b", "z", "\uE000", "\uD800\uDC00");
                Expression update;
                if (randomBoolean()) {
                    String value = randomBoolean() ? null : randomFrom("", "same", randomAlphaOfLength(5));
                    expected.put(key, value);
                    update = PackDimSupport.set(ref, named(key, value));
                } else {
                    expected.remove(key);
                    update = PackDimSupport.unset(ref, List.of(dimension(key, DataType.KEYWORD)));
                }
                var next = (PackDimBlock) evaluate(update, List.of(ref), 3, current);
                current.close();
                current = next;
                for (int p = 0; p < 3; p++)
                    assertEquals(expected, record(current, p));
            }
        } finally {
            current.close();
        }
    }

    public void testDuplicateNamesAndUnsupportedTypesRejected() {
        expectThrows(IllegalArgumentException.class, () -> PackDimSupport.pack(SOURCE, List.of(named("a", "x"), named("a", "y"))));
        var object = dimension("object", DataType.OBJECT);
        assertFalse(PackDimSupport.pack(SOURCE, List.of(object)).typeResolved().resolved());
        assertFalse(PackDimSupport.get(Literal.keyword(SOURCE, "not packed"), dimension("a", DataType.KEYWORD)).typeResolved().resolved());
    }

    public void testLazyFallbackDoesNotReadAnUnusedDimension() throws Exception {
        var packed = dimension("packed", DataType.PACK_DIM);
        var fallback = PackDimSupport.get(packed, dimension("number", DataType.KEYWORD));
        var value = new Coalesce(SOURCE, Literal.keyword(SOURCE, "chosen"), List.of(fallback));
        var update = PackDimSupport.setFromDimensions(packed, new Alias(SOURCE, "dst", value));
        var layout = new Layout.Builder().append(packed).build();
        var factory = (PackDimEvaluator.Factory) update.toEvaluator(EvalMapper.toEvaluatorContext(FoldContext.small(), layout, null, null));
        assertEquals(PackDimSupport.Operation.SET_FROM_DIMENSIONS, factory.expression().operation());
        try (
            var input = evaluate(
                PackDimSupport.pack(SOURCE, List.of(new Alias(SOURCE, "number", new Literal(SOURCE, 7, DataType.INTEGER)))),
                List.of(),
                3
            );
            var result = (PackDimBlock) evaluate(update, List.of(packed), 3, input)
        ) {
            assertEquals("chosen", record(result, 0).get("dst"));
        }
    }

    public void testNonFiniteValuesAreRejectedInsteadOfBecomingStrings() {
        for (double value : new double[] { Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY }) {
            var expression = PackDimSupport.pack(SOURCE, List.of(new Alias(SOURCE, "number", new Literal(SOURCE, value, DataType.DOUBLE))));
            expectThrows(IllegalArgumentException.class, () -> {
                try (var ignored = evaluate(expression, List.of(), 1)) {
                    fail("non-finite packed value accepted");
                }
            });
        }
    }

    public void testRecordScopeRejectsExternalColumnDependencies() {
        var packed = dimension("packed", DataType.PACK_DIM);
        var external = dimension("sample", DataType.KEYWORD);
        var expression = PackDimSupport.setFromDimensions(packed, new Alias(SOURCE, "dst", external));
        var layout = new Layout.Builder().append(packed).append(external).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> expression.toEvaluator(EvalMapper.toEvaluatorContext(FoldContext.small(), layout, null, null))
        );
    }

    public void testConvergingUnsetInternsValuesButDoesNotMergeSamples() throws Exception {
        var instance = dimension("instance", DataType.KEYWORD);
        var packedRef = dimension("packed", DataType.PACK_DIM);
        try (
            var values = strings("a", "b", "a", "b", "a");
            var input = (PackDimBlock) evaluate(
                PackDimSupport.pack(SOURCE, List.of(instance, named("region", "eu"))),
                List.of(instance),
                5,
                values
            );
            var output = (PackDimBlock) evaluate(PackDimSupport.unset(packedRef, List.of(instance)), List.of(packedRef), 5, input)
        ) {
            assertEquals(2, input.asOrdinalPackDim().getDictionarySize());
            assertEquals(1, output.asOrdinalPackDim().getDictionarySize());
            assertEquals(5, output.getPositionCount());
            for (int p = 0; p < 5; p++)
                assertEquals(Map.of("region", "eu"), record(output, p));
            assertTrue(record(input, 0).containsKey("instance"));
        }
    }

    public void testGeneratedUnsetHandlesNullHolesAndEmptyRecords() {
        var factory = blockFactory();
        var context = new DriverContext(factory.bigArrays(), factory, null);
        try (var builder = factory.newPackDimBlockBuilder(3)) {
            builder.appendNull();
            builder.append(new BytesRef[] { new BytesRef("a") }, new BytesRef[] { new BytesRef("null") });
            builder.append(new BytesRef[0], new BytesRef[0]);
            try (
                var input = builder.build();
                var evaluator = new PackDimValuesUnsetEvaluator.Factory(
                    SOURCE,
                    new LoadFromPageEvaluator.Factory(0),
                    new BytesRef[] { new BytesRef("a") }
                ).get(context);
                var output = (PackDimBlock) evaluator.eval(new Page(input))
            ) {
                assertTrue(output.isNull(0));
                for (int p = 1; p < 3; p++) {
                    assertFalse(output.isNull(p));
                    assertEquals(0, output.getPackDim(output.getFirstValueIndex(p), new PackDimValue()).size());
                }
            }
        }
    }

    private Block evaluate(Expression expression, List<? extends Attribute> inputs, int positions, Block... blocks) {
        assertTrue(expression.typeResolved().message(), expression.typeResolved().resolved());
        var factory = blockFactory();
        var context = new DriverContext(factory.bigArrays(), factory, null);
        var layout = new Layout.Builder().append(inputs).build();
        // The page borrows blocks from the test's resource scope.
        try (var evaluator = EvalMapper.toEvaluator(FoldContext.small(), expression, layout).get(context)) {
            return evaluator.eval(new Page(positions, blocks));
        }
    }

    private Block strings(String... values) {
        try (var builder = blockFactory().newBytesRefBlockBuilder(values.length)) {
            for (String value : values) {
                if (value == null) builder.appendNull();
                else builder.appendBytesRef(new BytesRef(value));
            }
            return builder.build();
        }
    }

    private static Map<String, Object> record(PackDimBlock block, int position) throws IOException {
        var result = new HashMap<String, Object>();
        var record = block.getPackDim(block.getFirstValueIndex(position), new PackDimValue());
        for (int i = 0; i < record.size(); i++) {
            result.put(record.nameAt(i, new BytesRef()).utf8ToString(), PackDimValueCodec.decode(record.valueAt(i, new BytesRef())));
        }
        return result;
    }

    private static Alias named(String name, String value) {
        return new Alias(SOURCE, name, value == null ? new Literal(SOURCE, null, DataType.NULL) : Literal.keyword(SOURCE, value));
    }

    private static Attribute dimension(String name, DataType type) {
        return new ReferenceAttribute(SOURCE, null, name, type);
    }
}
