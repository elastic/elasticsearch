/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.cluster.metadata.DatasetMapping.Subobjects;
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
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Compares the NDJSON reader against real Elasticsearch ingest on the same document, at both values of
 * {@code subobjects}. Every case runs one JSON object through {@link DocumentMapper#parse} with dynamic mapping and
 * through {@link NdJsonFormatReader}, then asserts the two agree on the set of leaf names produced and the values
 * landing on each. Agreement here is the whole contract of the setting: a dotted key, a nested object, and the two
 * mixed in one document must all resolve to the same leaves a matching index would create, so a query can name a
 * column by the same path either way.
 *
 * <p>All fixtures use integer values, because ingest maps a dynamic string to {@code text} plus a {@code .keyword}
 * multi-field. That would compare a mapping detail rather than the flattening behavior under test.
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
     * This holds at both settings, because both spellings converge on one leaf either way.
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
     * The one shape where the two settings genuinely differ, so it cannot go through {@link #assertParity}. A document
     * naming {@code a} as a scalar and as an object is a contradiction only when a dot is a path separator: under
     * ENABLED ingest rejects the document and the reader fails the read under STRICT, while under DISABLED both accept
     * it as two unrelated leaves. Ordered scalar-first here and object-first in
     * {@link #testObjectAndScalarForOneNameDiverge}.
     */
    public void testScalarAndObjectForOneNameDiverge() throws IOException {
        String json = """
            {"a":1,"a.b":2}""";
        assertRejectedByBothWhenSubobjectsEnabled(json);
        assertParity(Subobjects.DISABLED, json);
    }

    /** Mirror of {@link #testScalarAndObjectForOneNameDiverge} with the object spelling first. */
    public void testObjectAndScalarForOneNameDiverge() throws IOException {
        String json = """
            {"a.b":1,"a":2}""";
        assertRejectedByBothWhenSubobjectsEnabled(json);
        assertParity(Subobjects.DISABLED, json);
    }

    /** Runs the case at both settings; use this whenever the two are expected to reach the same leaves. */
    private void assertParity(String json) throws IOException {
        assertParity(Subobjects.ENABLED, json);
        assertParity(Subobjects.DISABLED, json);
    }

    private void assertParity(Subobjects subobjects, String json) throws IOException {
        assertThat(
            "reader disagrees with ingest at subobjects: " + subobjects + " on " + json,
            readerLeaves(subobjects, json),
            equalTo(ingestLeaves(subobjects, json))
        );
    }

    /**
     * Ingest rejects the document with a mapping conflict on the contended name, and the reader refuses the same
     * document under STRICT. The two failures are not the same exception type (ingest fails a single document, the
     * reader fails a query), so this asserts each side refuses and names the field, not that the messages match.
     */
    private void assertRejectedByBothWhenSubobjectsEnabled(String json) throws IOException {
        DocumentParsingException ingestFailure = expectThrows(
            DocumentParsingException.class,
            () -> mapper(Subobjects.ENABLED).parse(source(json))
        );
        assertThat(ingestFailure.getMessage(), containsString("a"));

        Exception readerFailure = expectThrows(Exception.class, () -> readerLeaves(Subobjects.ENABLED, json, ErrorPolicy.STRICT));
        // A structural node carries no attribute name, so which spelling identifies the contended field depends on
        // which shape won inference: a scalar column names itself, an object prefix is named by its JSON pointer.
        assertThat(readerFailure.getMessage(), anyOf(containsString("[a]"), containsString("[/a]")));
    }

    /** The leaf full paths ingest produced, each mapped to its values in document order. */
    private Map<String, List<Long>> ingestLeaves(Subobjects subobjects, String json) throws IOException {
        ParsedDocument doc = mapper(subobjects).parse(source(json));
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

    private Map<String, List<Long>> readerLeaves(Subobjects subobjects, String json) throws IOException {
        return readerLeaves(subobjects, json, ErrorPolicy.STRICT);
    }

    /** The columns the reader inferred, each mapped to its values, in the same form as {@link #ingestLeaves}. */
    private Map<String, List<Long>> readerLeaves(Subobjects subobjects, String json, ErrorPolicy errorPolicy) throws IOException {
        var object = new BytesStorageObject("memory://parity.ndjson", (json + "\n").getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory).withSubobjects(subobjects);
        List<Attribute> schema = reader.metadata(object).schema();
        Map<String, List<Long>> leaves = new TreeMap<>();
        try (var iterator = reader.read(object, FormatReadContext.builder().batchSize(100).errorPolicy(errorPolicy).build())) {
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

    /** A dynamic mapper whose root carries the setting under test, so ingest resolves names exactly as the reader must. */
    private DocumentMapper mapper(Subobjects subobjects) throws IOException {
        return createDocumentMapper(topMapping(b -> b.field("subobjects", subobjects.asBoolean())));
    }
}
