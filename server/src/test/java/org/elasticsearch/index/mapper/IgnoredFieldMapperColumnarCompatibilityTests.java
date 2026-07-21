/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Compatibility tests for {@link IgnoredFieldMapper}'s columnar batch path
 * ({@link IgnoredFieldMapper#postColumnarParse}) against its row-major path
 * ({@link IgnoredFieldMapper#postParse}).
 *
 * <p>Unlike the metadata mappers exercised through {@link #assertColumnarMatchesXContent}, the
 * {@code _ignored} values originate in field mappers (via {@link DocumentParserContext#addIgnoredField},
 * e.g. keyword {@code ignore_above}), which have no columnar driver yet, and under synthetic source
 * a real ignored value also emits {@code _ignored_source}/fallback fields the columnar path cannot
 * yet produce. So instead of the full x-content harness we drive the two {@code IgnoredFieldMapper}
 * halves directly with the same per-document ignored-name sets and assert the resulting Lucene
 * fields match, via {@link #assertColumnsMatchRowFields}.
 */
public class IgnoredFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    /** A single ignored name per document. */
    public void testSingleIgnoredField() throws IOException {
        assertIgnoredParity(List.of(List.of("field_a")));
    }

    /** No ignored fields: the accumulator stays empty and no {@code _ignored} column is attached. */
    public void testNoIgnoredFields() throws IOException {
        assertIgnoredParity(List.of(List.of(), List.of()));
    }

    /**
     * Multiple documents with overlapping and multi-name sets. Overlapping names across documents
     * exercise the accumulator's value interning (one shared {@link org.apache.lucene.util.BytesRef}),
     * and a document with several distinct names exercises the multi-valued array column.
     */
    public void testOverlappingAndMultiValued() throws IOException {
        assertIgnoredParity(
            List.of(List.of("field_a", "field_b"), List.of(), List.of("field_a"), List.of("field_b", "field_c", "field_a"))
        );
    }

    /**
     * Drives {@link IgnoredFieldMapper#postParse} (row) and {@link IgnoredFieldMapper#postColumnarParse}
     * (columnar) with the same per-document ignored-name sets and asserts the emitted {@code _ignored}
     * Lucene fields match. Index version defaults to {@code current} (modern), matching the
     * columnar-only, doc-values shape of {@code _ignored}.
     */
    private void assertIgnoredParity(List<List<String>> ignoredPerDoc) throws IOException {
        final int docCount = ignoredPerDoc.size();
        final MapperService mapperService = createMapperService(topMapping(b -> {}));
        final IgnoredFieldMapper mapper = mapperService.mappingLookup().getMapping().getMetadataMapperByClass(IgnoredFieldMapper.class);

        // Row path: seed each document's ignored fields and collect the _ignored fields postParse emits.
        final List<List<IndexableField>> expectedPerDoc = new ArrayList<>(docCount);
        for (List<String> names : ignoredPerDoc) {
            final TestDocumentParserContext rowContext = new TestDocumentParserContext(mapperService.mappingLookup(), null);
            for (String name : names) {
                rowContext.addIgnoredField(name);
            }
            mapper.postParse(rowContext);
            expectedPerDoc.add(new ArrayList<>(rowContext.doc().getFields(IgnoredFieldMapper.NAME)));
        }

        // Columnar path: record the same ignored fields into the batch context and drain via postColumnarParse.
        final IndexRequest[] requests = new IndexRequest[docCount];
        for (int i = 0; i < docCount; i++) {
            requests[i] = new IndexRequest("test-index").id("id-" + i).source("{}", XContentType.JSON);
        }
        final BatchMappingContext ctx = new BatchMappingContext(requests, mapperService.mappingLookup(), mapperService.getIndexSettings());
        for (int doc = 0; doc < docCount; doc++) {
            for (String name : ignoredPerDoc.get(doc)) {
                ctx.addIgnoredFieldColumnar(doc, name);
            }
        }
        mapper.postColumnarParse(ctx);
        final MappedColumns columnar = ctx.columns();

        assertColumnsMatchRowFields(expectedPerDoc, columnar, "_ignored columnar vs row parity");
    }
}
