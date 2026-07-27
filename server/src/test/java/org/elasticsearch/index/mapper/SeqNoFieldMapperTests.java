/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.sourcebatch.MappedColumns;

import java.io.IOException;

/**
 * Unit tests for {@link SeqNoFieldMapper}'s columnar batch-mapping path. These tests exercise
 * {@link MetadataFieldMapper#postColumnarParse(BatchMappingContext)} directly against production
 * mapper code — no engine, no shard, no {@code IndexShard} required.
 */
public class SeqNoFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return SeqNoFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // SeqNoFieldMapper has no user-configurable mapping parameters.
    }

    public void testColumnarParseRegistersSeqNoAndPrimaryTermColumns() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        SeqNoFieldMapper mapper = (SeqNoFieldMapper) mapperService.documentMapper().mappers().getMapper(SeqNoFieldMapper.NAME);
        assertNotNull(mapper);
        assertTrue("supportsColumnarParse must be true for _seq_no", mapper.supportsColumnarParse(mapperService.getIndexSettings()));

        IndexRequest[] requests = new IndexRequest[] { new IndexRequest("index").id("1"), new IndexRequest("index").id("2") };
        BatchMappingContext context = new BatchMappingContext(requests, mapperService.mappingLookup(), mapperService.getIndexSettings());

        mapper.postColumnarParse(context);

        final MappedColumns mappedColumns = context.columns();
        Column seqNoColumn = null;
        Column primaryTermColumn = null;
        for (Column column : mappedColumns.toColumnBatch().columns()) {
            if (column.name().equals(SeqNoFieldMapper.NAME)) {
                seqNoColumn = column;
            } else if (column.name().equals(SeqNoFieldMapper.PRIMARY_TERM_NAME)) {
                primaryTermColumn = column;
            }
        }

        assertNotNull("expected a _seq_no column", seqNoColumn);
        assertNotNull("expected a _primary_term column", primaryTermColumn);

        // Both columns must be NUMERIC doc-values-only.
        assertEquals("_seq_no doc values type must be NUMERIC", DocValuesType.NUMERIC, seqNoColumn.fieldType().docValuesType());
        assertEquals("_seq_no must have no inverted index", IndexOptions.NONE, seqNoColumn.fieldType().indexOptions());
        assertFalse("_seq_no must not be stored", seqNoColumn.fieldType().stored());

        assertEquals("_primary_term doc values type must be NUMERIC", DocValuesType.NUMERIC, primaryTermColumn.fieldType().docValuesType());
        assertEquals("_primary_term must have no inverted index", IndexOptions.NONE, primaryTermColumn.fieldType().indexOptions());
        assertFalse("_primary_term must not be stored", primaryTermColumn.fieldType().stored());

        // The engine fills values later; the column should initially yield UNASSIGNED_SEQ_NO for _seq_no
        // and 0L for _primary_term (zero-init).
        LongColumn seqNoLongCol = (LongColumn) seqNoColumn;
        var seqNoCursor = seqNoLongCol.tuples();
        assertEquals(0, seqNoCursor.nextDoc());
        assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, seqNoCursor.longValue());
        assertEquals(1, seqNoCursor.nextDoc());
        assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, seqNoCursor.longValue());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, seqNoCursor.nextDoc());

        LongColumn primaryTermLongCol = (LongColumn) primaryTermColumn;
        var primaryTermCursor = primaryTermLongCol.tuples();
        assertEquals(0, primaryTermCursor.nextDoc());
        assertEquals(0L, primaryTermCursor.longValue());
        assertEquals(1, primaryTermCursor.nextDoc());
        assertEquals(0L, primaryTermCursor.longValue());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, primaryTermCursor.nextDoc());
    }
}
