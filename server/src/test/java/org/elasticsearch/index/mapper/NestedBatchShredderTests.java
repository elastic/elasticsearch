/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.mapper.NestedBatchShredder.Children;
import org.elasticsearch.index.mapper.NestedBatchShredder.Fallback;
import org.elasticsearch.index.mapper.ShardBatchMapper.NestedGroupResolution;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/** Tests the shredding of the values of a nested field into a batch of elements. */
public class NestedBatchShredderTests extends AbstractShardBatchMapperResolveTestCase {

    private static final long NO_LIMIT = 10_000;

    private MapperService mapperService() throws IOException {
        return columnarMapperService(mapping(b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("k").field("type", "keyword").endObject();
            b.startObject("v").field("type", "long").endObject();
            b.startObject("tags").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.startObject("m").field("type", "nested").startObject("properties");
            b.startObject("x").field("type", "long").endObject();
            b.endObject().endObject();
        }));
    }

    private static EscfBatch encode(String... docs) throws IOException {
        final List<BytesReference> sources = Arrays.stream(docs).<BytesReference>map(BytesArray::new).toList();
        return EscfEncoder.encode(sources, XContentType.JSON);
    }

    /** The nested group of the path, of the batch's own resolution. */
    private NestedGroupResolution group(EscfBatch batch, String path) throws IOException {
        final ShardBatchMapper.BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            batch,
            mapperService().mappingLookup(),
            indexSettings
        );
        assertNotNull(resolution);
        return Arrays.stream(resolution.nestedGroups()).filter(g -> g.mapper().fullPath().equals(path)).findFirst().orElseThrow();
    }

    private Children shred(EscfBatch batch, String path, int[] childrenPerRow, long limit) throws IOException {
        return NestedBatchShredder.shred(batch, group(batch, path), BytesRefRecycler.NON_RECYCLING_INSTANCE, childrenPerRow, limit);
    }

    private Children shred(EscfBatch batch) throws IOException {
        return shred(batch, "n", new int[batch.docCount()], NO_LIMIT);
    }

    private static EscfColumn column(Children children, String key) {
        final int leaf = children.batch().schema().findLeaf(key, 0);
        assertThat("leaf [" + key + "]", leaf, greaterThanOrEqualTo(0));
        return children.batch().column(leaf);
    }

    public void testArrayElementsBecomeRowsInSourceOrder() throws IOException {
        try (
            EscfBatch batch = encode(
                "{\"host\":\"h\",\"n\":[{\"k\":\"a\",\"v\":1},{\"k\":\"b\",\"v\":2}]}",
                "{\"host\":\"h\"}",
                "{\"n\":[{\"k\":\"c\"}]}"
            )
        ) {
            final Children children = shred(batch);
            try (EscfBatch elements = children.batch()) {
                assertArrayEquals(new int[] { 0, 2, 2, 3 }, children.parentOffsets());
                assertArrayEquals(new int[] { 0, 0, 2 }, children.childToRow());
                assertEquals(3, elements.docCount());
                final EscfColumn k = column(children, "k");
                assertEquals("a", k.getStringValue(0).string());
                assertEquals("b", k.getStringValue(1).string());
                assertEquals("c", k.getStringValue(2).string());
                final EscfColumn v = column(children, "v");
                assertEquals(1L, v.getLongValue(0));
                assertEquals(2L, v.getLongValue(1));
                assertEquals("the third element has no v", SourceValueType.ABSENT, v.getTypeByte(2));
                assertThat(children.floatingKeys(), empty());
            }
        }
    }

    public void testObjectSpellingBecomesOneRowForEachRowThatHasIt() throws IOException {
        try (EscfBatch batch = encode("{\"n\":{\"k\":\"x\",\"v\":1}}", "{\"host\":\"h\"}", "{\"n\":{\"k\":\"y\"}}")) {
            final Children children = shred(batch);
            try (EscfBatch elements = children.batch()) {
                assertArrayEquals(new int[] { 0, 1, 1, 2 }, children.parentOffsets());
                assertArrayEquals(new int[] { 0, 2 }, children.childToRow());
                assertEquals(2, elements.docCount());
                assertEquals("x", column(children, "k").getStringValue(0).string());
                assertEquals("y", column(children, "k").getStringValue(1).string());
                assertEquals(1L, column(children, "v").getLongValue(0));
            }
        }
    }

    public void testBothSpellingsInOneBatch() throws IOException {
        try (EscfBatch batch = encode("{\"n\":[{\"k\":\"a\"},{\"k\":\"b\"}]}", "{\"n\":{\"k\":\"c\"}}")) {
            final Children children = shred(batch);
            try (EscfBatch elements = children.batch()) {
                assertArrayEquals(new int[] { 0, 2, 3 }, children.parentOffsets());
                assertEquals(
                    List.of("a", "b", "c"),
                    List.of(
                        column(children, "k").getStringValue(0).string(),
                        column(children, "k").getStringValue(1).string(),
                        column(children, "k").getStringValue(2).string()
                    )
                );
            }
        }
    }

    public void testEmptyValuesHaveTheSameNumberOfChildrenAsTheSequentialPath() throws IOException {
        try (
            EscfBatch batch = encode(
                "{\"n\":[]}",
                "{\"n\":null}",
                "{\"n\":{}}",
                "{\"n\":[{}]}",
                "{\"n\":[{},{\"k\":\"x\"}]}",
                "{\"host\":\"h\"}"
            )
        ) {
            final Children children = shred(batch);
            try (EscfBatch elements = children.batch()) {
                assertArrayEquals(new int[] { 0, 0, 0, 1, 2, 4, 4 }, children.parentOffsets());
                assertArrayEquals(new int[] { 2, 3, 4, 4 }, children.childToRow());
            }
        }
    }

    public void testNoChildrenGivesNull() throws IOException {
        try (EscfBatch batch = encode("{\"n\":[]}", "{\"n\":null}", "{\"host\":\"h\"}")) {
            assertNull(shred(batch));
        }
    }

    public void testScalarArrayInElementIsKeptAsAnArray() throws IOException {
        try (EscfBatch batch = encode("{\"n\":[{\"tags\":[\"a\",\"b\"]},{\"tags\":[\"c\"]}]}", "{\"n\":{\"tags\":[\"d\",\"e\"]}}")) {
            final Children children = shred(batch);
            try (EscfBatch elements = children.batch()) {
                final EscfColumn tags = column(children, "tags");
                assertEquals(List.of("a", "b"), strings(tags.getArrayValue(0)));
                assertEquals(List.of("c"), strings(tags.getArrayValue(1)));
                assertEquals(
                    "an array of the object spelling is packed like the one of a blob",
                    List.of("d", "e"),
                    strings(tags.getArrayValue(2))
                );
            }
        }
    }

    private static List<String> strings(ArrayReader array) {
        final List<String> values = new ArrayList<>();
        while (array.next()) {
            values.add(array.stringValue());
        }
        return values;
    }

    public void testFloatingPointNumbersOfABlobAreMarked() throws IOException {
        try (EscfBatch batch = encode("{\"n\":[{\"k\":1.5,\"v\":2},{\"tags\":[0.5]}]}")) {
            final Children children = shred(batch);
            try (EscfBatch elements = children.batch()) {
                assertEquals(Set.of("k", "tags"), children.floatingKeys());
            }
        }
    }

    public void testObjectSpellingKeepsTheSpellingOfADouble() throws IOException {
        try (EscfBatch batch = encode("{\"n\":{\"k\":1.50}}")) {
            final Children children = shred(batch);
            try (EscfBatch elements = children.batch()) {
                assertThat(children.floatingKeys(), empty());
                assertEquals("1.50", column(children, "k").getStringValue(0).string());
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // What falls back
    // ---------------------------------------------------------------------------------------------------------------------------

    private void assertFallsBack(String expectedMessage, String... docs) throws IOException {
        try (EscfBatch batch = encode(docs)) {
            final Fallback e = expectThrows(Fallback.class, () -> shred(batch));
            assertThat(e.getMessage(), containsString(expectedMessage));
        }
    }

    public void testScalarAndNullElementsFallBack() throws IOException {
        assertFallsBack("not an object", "{\"n\":[\"a\"]}");
        assertFallsBack("not an object", "{\"n\":[1,2]}");
        assertFallsBack("not an object", "{\"n\":[null,{\"k\":\"a\"}]}");
        assertFallsBack("not an object or an array", "{\"n\":\"a\"}", "{\"n\":[{\"k\":\"b\"}]}");
    }

    public void testObjectInsideAnElementFallsBack() throws IOException {
        assertFallsBack("has an object", "{\"n\":[{\"o\":{\"x\":1}}]}");
        assertFallsBack("has an object", "{\"n\":[{\"a\":[{\"x\":1}]}]}");
    }

    public void testRowThatSpellsTheFieldTwiceFallsBack() throws IOException {
        assertFallsBack("as an array and as an object", "{\"n\":[{\"k\":\"a\"}],\"n\":{\"k\":\"b\"}}");
    }

    public void testDuplicateKeyInAnElementFallsBack() throws IOException {
        assertFallsBack("duplicate key", "{\"n\":[{\"k\":\"a\",\"k\":\"b\"}]}");
    }

    public void testMoreChildrenThanTheLimitFallBack() throws IOException {
        try (EscfBatch batch = encode("{\"n\":[{\"k\":\"a\"},{\"k\":\"b\"},{\"k\":\"c\"}]}")) {
            expectThrows(Fallback.class, () -> shred(batch, "n", new int[1], 2));
            final Children children = shredAndKeep(batch, 3);
            try (EscfBatch elements = children.batch()) {
                assertEquals(3, elements.docCount());
            }
        }
    }

    private Children shredAndKeep(EscfBatch batch, long limit) throws IOException {
        return shred(batch, "n", new int[batch.docCount()], limit);
    }

    public void testTheLimitCountsTheChildrenOfAllNestedPaths() throws IOException {
        try (EscfBatch batch = encode("{\"n\":[{\"k\":\"a\"}],\"m\":[{\"x\":1},{\"x\":2}]}")) {
            final int[] childrenPerRow = new int[1];
            try (EscfBatch first = shred(batch, "m", childrenPerRow, 3).batch()) {
                assertEquals(2, first.docCount());
            }
            assertArrayEquals(new int[] { 2 }, childrenPerRow);
            try (EscfBatch second = shred(batch, "n", childrenPerRow, 3).batch()) {
                assertEquals(1, second.docCount());
            }
            assertArrayEquals(new int[] { 3 }, childrenPerRow);
            final int[] again = new int[] { 2 };
            expectThrows(Fallback.class, () -> shred(batch, "n", again, 2));
        }
    }
}
