/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Tests ordinary JSON object edits, independent of PromQL label semantics. */
public class JsonObjectEditsTests extends ESTestCase {
    public void testRemoveNestedAndLiteralDottedFields() throws IOException {
        assertEquals(
            "{\"labels\":{\"job\":\"api\"},\"region\":\"eu\"}",
            remove("{\"labels\":{\"instance\":\"a\",\"job\":\"api\"},\"labels.instance\":\"b\",\"region\":\"eu\"}", "labels.instance")
        );
        assertEquals("{\"region\":\"eu\"}", remove("{\"labels\":{\"instance\":\"a\"},\"region\":\"eu\"}", "labels.instance"));
    }

    public void testRemovalIsExactNotWildcardOrPrefix() throws IOException {
        assertEquals(
            "{\"a.b\":\"keep\",\"ab\":\"keep\",\"other\":null}",
            remove("{\"a*\":\"drop\",\"ab\":\"keep\",\"a.b\":\"keep\",\"other\":null}", "a*")
        );
    }

    public void testEmptyNullAndMultivalueAreRetained() throws IOException {
        assertEquals(
            "{\"empty\":\"\",\"null\":null,\"values\":[\"a\",\"b\"]}",
            remove("{\"values\":[\"a\",\"b\"],\"null\":null,\"empty\":\"\",\"gone\":1}", "gone")
        );
    }

    public void testWithoutOverWithoutUsesCurrentObject() throws IOException {
        String input = "{\"cluster\":\"prod\",\"instance\":\"a\",\"region\":\"eu\"}";
        assertEquals(remove(input, "instance", "region"), remove(remove(input, "instance"), "region"));
        assertEquals(remove(input, "instance"), remove(remove(input, "instance"), "instance"));
    }

    public void testOverlayDoesNotTreatNullAsRemoval() throws IOException {
        assertEquals("{\"a\":null,\"b\":\"\",\"c\":false}", merge("{\"a\":\"old\",\"b\":\"old\"}", "{\"a\":null,\"b\":\"\",\"c\":false}"));
        assertEquals("{\"b\":\"\",\"c\":false}", remove(merge("{\"a\":\"old\"}", "{\"a\":null,\"b\":\"\",\"c\":false}"), "a"));
    }

    public void testOverlayReplacesWholeMemberAndPreservesUnrelatedValues() throws IOException {
        assertEquals("{\"a\":{\"new\":2},\"unchanged\":[1,2]}", merge("{\"a\":{\"old\":1},\"unchanged\":[1,2]}", "{\"a\":{\"new\":2}}"));
    }

    public void testEditOrderingDoesNotDependOnUpdateOrder() throws IOException {
        assertEquals(merge("{\"b\":2,\"a\":1}", "{\"d\":4,\"c\":3}"), merge("{\"a\":1,\"b\":2}", "{\"c\":3,\"d\":4}"));
        assertEquals(remove("{\"nested\":{\"b\":2,\"a\":1}}", "missing"), remove("{\"nested\":{\"a\":1,\"b\":2}}", "missing"));
    }

    public void testStringsAreEscapedByTheJsonWriter() throws IOException {
        String input = "{\"quote\\\"key\":\"newline\\nbackslash\\\\\",\"other\":1}";
        assertEquals(JsonMerge.readObject(new BytesRef(input)), JsonMerge.readObject(new BytesRef(merge(input, "{}"))));
    }

    public void testRejectTrailingContent() {
        expectThrows(IllegalArgumentException.class, () -> remove("{}{}", "a"));
    }

    public void testRejectNonObjectsAndMalformedJson() {
        for (String input : List.of("[]", "null", "\"value\"", "1", "", "{\"a\":}")) {
            expectThrows(IllegalArgumentException.class, () -> remove(input, "a"));
            expectThrows(IllegalArgumentException.class, () -> merge("{}", input));
        }
    }

    public void testGeneratedEvaluatorsComposeWithExistingJsonString() {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(32)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blocks = BlockFactory.builder(bigArrays).build();
        DriverContext context = new DriverContext(bigArrays, blocks, null);
        var field = new FieldAttribute(
            Source.EMPTY,
            "json",
            new EsField("json", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        var edit = new JsonMerge(
            Source.EMPTY,
            new JsonRemove(Source.EMPTY, field, List.of("drop")),
            new JsonString(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, "new"), Literal.keyword(Source.EMPTY, "value")))
        );
        try (
            var evaluator = AbstractScalarFunctionTestCase.evaluator(edit).get(context);
            BytesRefBlock.Builder builder = blocks.newBytesRefBlockBuilder(4)
        ) {
            builder.appendBytesRef(new BytesRef("{\"drop\":\"a\",\"keep\":\"b\"}"));
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("{\"drop\":\"a\",\"keep\":\"b\"}"));
            builder.appendBytesRef(new BytesRef("{}"));
            Page page = new Page(builder.build());
            try (Block output = evaluator.eval(page)) {
                BytesRefBlock result = (BytesRefBlock) output;
                assertEquals(
                    "{\"keep\":\"b\",\"new\":\"value\"}",
                    result.getBytesRef(result.getFirstValueIndex(0), new BytesRef()).utf8ToString()
                );
                assertTrue(result.isNull(1));
                assertEquals(
                    result.getBytesRef(result.getFirstValueIndex(0), new BytesRef()),
                    result.getBytesRef(result.getFirstValueIndex(2), new BytesRef())
                );
                assertEquals("{\"new\":\"value\"}", result.getBytesRef(result.getFirstValueIndex(3), new BytesRef()).utf8ToString());
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
        assertTrue(context.warnings().isEmpty());
    }

    private static String remove(String json, String... fields) throws IOException {
        return JsonRemove.process(new BytesRef(json), Set.of(fields)).utf8ToString();
    }

    private static String merge(String object, String updates) throws IOException {
        return JsonMerge.process(new BytesRef(object), new BytesRef(updates)).utf8ToString();
    }
}
