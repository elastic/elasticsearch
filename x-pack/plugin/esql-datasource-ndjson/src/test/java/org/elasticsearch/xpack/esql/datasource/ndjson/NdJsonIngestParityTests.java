/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Compares the NDJSON reader against real Elasticsearch ingest on the same document.
 * Every case runs one JSON object through {@link DocumentMapper#parse} with dynamic mapping and through
 * {@link NdJsonFormatReader}, then asserts the two agree on the set of leaf names produced and the values landing on
 * each. A dotted key, a nested object, and the two mixed in one document must all resolve to the same leaves a
 * matching index would create, so a query can name a column by the same path either way.
 *
 * <p>All fixtures use integer values, because ingest maps a dynamic string to {@code text} plus a {@code .keyword}
 * multi-field. That would compare a mapping detail rather than the flattening behavior under test.
 *
 * <p>{@link #testRepeatedFlatKeyIsRejectedByBoth} is the one case where the agreement is that the document is
 * rejected, so it compares the two failures instead of two sets of leaves.
 */
public class NdJsonIngestParityTests extends MapperServiceTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testDottedKey() throws IOException {
        assertParity("""
            {"a.b":1}""");
    }

    public void testNestedObject() throws IOException {
        assertParity("""
            {"a":{"b":1}}""");
    }

    public void testDeepNesting() throws IOException {
        assertParity("""
            {"a":{"b":{"c":1}}}""");
    }

    public void testDottedKeyHoldingObject() throws IOException {
        assertParity("""
            {"a.b":{"c":1}}""");
    }

    public void testStructuredAndFlatMixedUnderOneObject() throws IOException {
        assertParity("""
            {"a":{"b.c":1,"b":{"d":2}}}""");
    }

    /**
     * Both spellings of one leaf in one document. Ingest emits two values for the leaf and the mapper builds a
     * multivalue from them; the reader must do the same rather than let the second occurrence overwrite or be dropped.
     */
    public void testBothSpellingsOfOneLeafBecomeAMultivalue() throws IOException {
        assertParity("""
            {"a":{"b":1},"a.b":2}""");
    }

    public void testArrayOfObjectsBecomesAMultivalueLeaf() throws IOException {
        assertParity("""
            {"a":[{"b":1},{"b":2}]}""");
    }

    /**
     * A dot is an ordinary character in a column name, so a scalar at {@code a} and a flat key {@code a.b} are
     * unrelated leaves. Both ingest and reader accept the document and produce two independent columns.
     */
    public void testScalarAndObjectForOneNameCoexist() throws IOException {
        assertParity("""
            {"a":1,"a.b":2}""");
    }

    /** Mirror of {@link #testScalarAndObjectForOneNameCoexist} with the object spelling first. */
    public void testObjectAndScalarForOneNameCoexist() throws IOException {
        assertParity("""
            {"a.b":1,"a":2}""");
    }

    /** An empty array contributes no values, so the leaf is simply absent from the document. */
    public void testEmptyArrayContributesNothing() throws IOException {
        assertParity("""
            {"a.b":[],"id":1}""");
    }

    /**
     * An empty array under one spelling of a leaf, then a populated array of objects under the other. The empty
     * array contributes nothing, so the leaf must still take the later array's value rather than being pinned to
     * the null the empty array appeared to claim.
     */
    public void testEmptyArrayThenObjectArrayOnPrefix() throws IOException {
        assertParity("""
            {"a.b":[],"a":[{"b":1}],"id":10}""");
    }

    /** Mirror of {@link #testEmptyArrayThenObjectArrayOnPrefix}, empty array on the prefix. */
    public void testEmptyArrayOnPrefixThenFlatKey() throws IOException {
        assertParity("""
            {"a":[],"a.b":1,"id":10}""");
    }

    /** An empty array beside the other spelling of the same leaf, in either order: the value still lands. */
    public void testEmptyArrayBesideTheOtherSpellingOfOneLeaf() throws IOException {
        assertParity("""
            {"a.b":[],"a":{"b":2},"id":10}""");
        assertParity("""
            {"a":{"b":3},"a.b":[],"id":20}""");
    }

    /** The same, one level deeper, so the empty array and the value meet below the array's own node. */
    public void testDeepEmptyArrayThenObjectArrayOnPrefix() throws IOException {
        assertParity("""
            {"a.b.c":[],"a":[{"b":{"c":1}}],"id":10}""");
    }

    /** A flat spelling then an array of objects on the prefix: both occurrences reach the leaf. */
    public void testFlatKeyThenObjectArrayOnPrefix() throws IOException {
        assertParity("""
            {"a.b":1,"a":[{"b":2}],"id":10}""");
    }

    /** Mirror of {@link #testFlatKeyThenObjectArrayOnPrefix} with the array first. */
    public void testObjectArrayOnPrefixThenFlatKey() throws IOException {
        assertParity("""
            {"a":[{"b":1}],"a.b":2,"id":10}""");
    }

    /**
     * An array of empty objects contributes no leaf values, so a later flat spelling of that leaf still lands.
     * Claiming the empty child entry as null would pin the cell against that value.
     */
    public void testEmptyObjectArrayThenFlatKey() throws IOException {
        assertParity("""
            {"a":[{}],"a.b":1,"id":10}""");
        assertParity("""
            {"a.b":1,"a":[{}],"id":20}""");
    }

    /** A JSON null inside an object-array element contributes nothing, so a later spelling of that leaf still lands. */
    public void testNullInsideObjectArrayThenFlatKey() throws IOException {
        assertParity("""
            {"a":[{"b":null}],"a.b":2,"id":10}""");
    }

    /** An object-array element that fills a sibling leaf does not pin an omitted leaf against a later spelling. */
    public void testObjectArraySiblingThenFlatKey() throws IOException {
        assertParity("""
            {"a":[{"c":1}],"a.b":2,"id":10}""");
    }

    /**
     * Mirror of {@link #testObjectArraySiblingThenFlatKey}: a flat spelling already filled one leaf, then an
     * object-array element fills a sibling. Both leaves are present; the array does not touch the claimed leaf.
     */
    public void testFlatKeyThenObjectArraySibling() throws IOException {
        assertParity("""
            {"a.b":1,"a":[{"c":2}],"id":10}""");
    }

    /**
     * An array of objects whose elements disagree on the shape at one name: {@code x.a} is a scalar in the first
     * element and an object in the second, so the array fills the leaf-and-prefix node {@code x.a} and its child
     * {@code x.a.b} from different elements.
     */
    public void testSparseObjectArrayOverLeafAndPrefix() throws IOException {
        assertParity("""
            {"x":[{"a":1},{"a":{"b":2}}],"id":10}""");
    }

    /** Mirror of {@link #testBothSpellingsOfOneLeafBecomeAMultivalue}: the values land in document order either way. */
    public void testBothSpellingsOfOneLeafMergeInDocumentOrder() throws IOException {
        assertParity("""
            {"a.b":2,"a":{"b":1}}""");
    }

    /**
     * A key repeated verbatim is rejected by both, and for the same reason: the record has no single
     * interpretation, so neither side picks one. The two sides differ only in how the rejection is carried, since
     * the reader's {@code error_mode} can turn it into a dropped line with a warning whereas indexing one document
     * has no such dial. Parity of the outcome is asserted rather than parity of the leaves, as there are none.
     * <p>
     * Merging the two occurrences instead, the way the two dotted spellings of one leaf merge, would answer over a
     * record that could never have been indexed. That both messages name the same field is what pins the two
     * checks to one cause.
     */
    public void testRepeatedFlatKeyIsRejectedByBoth() {
        String json = """
            {"a.b":1,"a.b":2}""";
        ParsingException readerFailure = expectThrows(ParsingException.class, () -> readerLeaves(json));
        assertThat(readerFailure.getMessage(), containsString("Duplicate field 'a.b'"));
        DocumentParsingException ingestFailure = expectThrows(DocumentParsingException.class, () -> ingestLeaves(json));
        assertThat(ingestFailure.getMessage(), containsString("Duplicate field 'a.b'"));
    }

    /**
     * A JSON null under one spelling contributes nothing, so the other spelling's value is the leaf's only value
     * rather than one half of a multivalue.
     */
    public void testNullUnderOneSpellingContributesNothing() throws IOException {
        assertParity("""
            {"a":{"b":null},"a.b":2,"id":10}""");
        assertParity("""
            {"a.b":2,"a":{"b":null},"id":20}""");
    }

    /** A null at a prefix position leaves the dotted leaf spelled beside it untouched. */
    public void testNullAtPrefixThenFlatKey() throws IOException {
        assertParity("""
            {"a":null,"a.b":1,"id":10}""");
    }

    /** An empty object holds no leaves, so it contributes nothing, exactly as an empty array does. */
    public void testEmptyObjectUnderFlatKeyContributesNothing() throws IOException {
        assertParity("""
            {"a.b":{},"id":10}""");
    }

    /** A flat dotted key holding an array of scalars: the leaf takes every element. */
    public void testScalarArrayUnderFlatKey() throws IOException {
        assertParity("""
            {"a.b":[1,2],"id":10}""");
    }

    /** The same leaf's values split across the two spellings, each holding its own single-element array. */
    public void testScalarArraysSplitAcrossBothSpellings() throws IOException {
        assertParity("""
            {"a.b":[1],"a":{"b":[2]},"id":10}""");
    }

    /** Nested arrays are flattened, so an object one level deeper inside the array still reaches the leaf. */
    public void testNestedArrayIsFlattened() throws IOException {
        assertParity("""
            {"a":[[{"b":1}]],"id":10}""");
    }

    /** Both spellings of one leaf inside array elements of a prefix, so the merge happens under an open array entry. */
    public void testBothSpellingsInsideArrayElements() throws IOException {
        assertParity("""
            {"x":[{"a.b":1},{"a":{"b":2}}],"id":10}""");
    }

    /** Three spellings of one leaf in one document: flat, split at the first dot, and split at the second. */
    public void testThreeSpellingsOfOneLeafMerge() throws IOException {
        assertParity("""
            {"a.b.c":1,"a":{"b.c":2},"a.b":{"c":3}}""");
    }

    /**
     * A flat key with an empty dotted segment. This is the shape ingest's dot-expanding parser rejects, so it is
     * worth pinning that the {@code subobjects: false} semantics the reader mirrors accept it.
     */
    public void testFlatKeysWithEmptyDottedSegments() throws IOException {
        assertParity("""
            {"a..b":1,"id":10}""");
        assertParity("""
            {".a":1,"id":10}""");
        assertParity("""
            {"a.":1,"id":10}""");
    }

    /**
     * An empty JSON field name is an ordinary segment, so ingest concatenates it into a leaf with an empty segment
     * and the reader addresses that leaf the same way. Without this the value would be read as null while the same
     * bytes indexed cleanly, which is the divergence this whole path exists to remove.
     *
     * <p>Only an empty name holding a scalar is compared. An empty name holding an <em>object</em>
     * ({@code {"a":{"":{"b":1}}}}) makes ingest build the intermediate prefix {@code "a."} and trip an assertion in
     * {@code ObjectMapper#hasMappedFieldsWithPrefix}, so there is no ingest behavior to compare against.
     */
    public void testEmptyFieldNameIsAnOrdinarySegment() throws IOException {
        assertParity("""
            {"a":{"":1},"id":10}""");
        assertParity("""
            {"a":{"b":{"":1}},"id":10}""");
    }

    /** Both spellings of a column whose last segment is empty, so they merge as any other pair of spellings does. */
    public void testBothSpellingsOfAnEmptyLastSegmentMerge() throws IOException {
        assertParity("""
            {"a":{"":1},"a.":2,"id":10}""");
        assertParity("""
            {"a.":1,"a":{"":2},"id":20}""");
    }

    private void assertParity(String json) throws IOException {
        assertThat("reader disagrees with ingest on " + json, readerLeaves(json), equalTo(ingestLeaves(json)));
    }

    /** The leaf full paths ingest produced, each mapped to its values in document order. */
    private Map<String, List<Long>> ingestLeaves(String json) throws IOException {
        ParsedDocument doc = mapper().parse(source(json));
        Map<String, List<Long>> leaves = new TreeMap<>();
        for (IndexableField field : doc.rootDoc().getFields()) {
            // Metadata fields (_source, _seq_no, ...) are not part of the document's own shape. Every fixture value is
            // an integer, so a numeric value identifies a real leaf and skips the mapper's internal marker fields.
            if (field.name().startsWith("_") || field.numericValue() == null) {
                continue;
            }
            leaves.computeIfAbsent(field.name(), k -> new ArrayList<>()).add(field.numericValue().longValue());
        }
        return leaves;
    }

    /** The columns the reader inferred, each mapped to its values, in the same form as {@link #ingestLeaves}. */
    private Map<String, List<Long>> readerLeaves(String json) throws IOException {
        var object = new BytesStorageObject("memory://parity.ndjson", (json + "\n").getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        List<Attribute> schema = reader.metadata(object).schema();
        Map<String, List<Long>> leaves = new TreeMap<>();
        try (var iterator = reader.read(object, FormatReadContext.builder().batchSize(100).errorPolicy(ErrorPolicy.STRICT).build())) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals("one record in, one row out", 1, page.getPositionCount());
            for (int c = 0; c < schema.size(); c++) {
                List<Long> values = valuesAt(page.getBlock(c), 0);
                // An inferred column absent from this record is a null cell, which is the reader's equivalent of ingest
                // simply not emitting a field for it.
                if (values.isEmpty() == false) {
                    leaves.put(schema.get(c).name(), values);
                }
            }
        }
        return leaves;
    }

    private static List<Long> valuesAt(Block block, int position) {
        List<Long> values = new ArrayList<>();
        int first = block.getFirstValueIndex(position);
        for (int i = 0; i < block.getValueCount(position); i++) {
            values.add(switch (block) {
                case IntBlock ints -> (long) ints.getInt(first + i);
                case LongBlock longs -> longs.getLong(first + i);
                default -> throw new AssertionError("fixtures use integer values only, got [" + block.getClass() + "]");
            });
        }
        return values;
    }

    /** A dynamic mapper that treats dots as literal characters in field names, matching the reader's behavior. */
    private DocumentMapper mapper() throws IOException {
        return createDocumentMapper(topMapping(b -> b.field("subobjects", false)));
    }
}
