/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.escf.LuceneLongColumn;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;

import static org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper.DEFAULT_PATH;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link DataStreamTimestampFieldMapper#postColumnarParse(BatchMappingContext)}.
 * These tests exercise the method directly, without an engine or shard.
 */
public class DataStreamTimestampFieldMapperColumnarTests extends MapperServiceTestCase {

    // ---- helpers -------------------------------------------------------------------------------

    private static Settings columnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
    }

    /**
     * Creates a batch context with N documents (no columns pre-populated).
     * The mapping includes {@code @timestamp} as a {@code date} field so that
     * {@link DataStreamTimestampFieldMapper#postColumnarParse} can resolve the field type.
     */
    private BatchMappingContext contextWithNDocs(MapperService mapperService, int docCount) {
        IndexRequest[] requests = new IndexRequest[docCount];
        for (int i = 0; i < docCount; i++) {
            requests[i] = new IndexRequest("index").id(String.valueOf(i + 1));
        }
        IndexOperationBatch batch = EngineTestCase.initFromRequests(requests);
        return new BatchMappingContext(
            batch,
            mapperService.mappingLookup(),
            mapperService.getIndexSettings(),
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
    }

    /** Dense column: every document is present, with the given sequential timestamp values. */
    private static LuceneLongColumn denseTimestampColumn(long... timestamps) {
        byte[] bytes = new byte[timestamps.length * 8];
        for (int i = 0; i < timestamps.length; i++) {
            ByteUtils.writeLongLE(timestamps[i], bytes, i * 8);
        }
        return LuceneLongColumn.longColumn(
            new BytesRef(bytes),
            DEFAULT_PATH,
            SortedNumericDocValuesField.TYPE,
            LongColumn.NumericKind.LONG
        );
    }

    /** Sparse column: only the docs at {@code presentDocs} positions are present. */
    private static LuceneLongColumn sparseTimestampColumn(int docCount, long timestamp, int... presentDocs) {
        byte[] bytes = new byte[docCount * 8];
        FixedBitSet validity = new FixedBitSet(docCount);
        for (int doc : presentDocs) {
            ByteUtils.writeLongLE(timestamp, bytes, doc * 8);
            validity.set(doc);
        }
        return LuceneLongColumn.sparseLongColumn(
            bytes,
            validity,
            docCount,
            DEFAULT_PATH,
            SortedNumericDocValuesField.TYPE,
            LongColumn.NumericKind.LONG
        );
    }

    // ---- tests ---------------------------------------------------------------------------------

    /** When no @timestamp column was produced by mapColumnBatch, postColumnarParse throws. */
    public void testMissingColumnThrows() throws IOException {
        MapperService mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(DEFAULT_PATH).field("type", "date").endObject())
        );
        BatchMappingContext context = contextWithNDocs(mapperService, 2);

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context)
        );
        assertThat(ex.getMessage(), equalTo("data stream timestamp field [" + DEFAULT_PATH + "] is missing"));
    }

    /** When @timestamp is absent for at least one document in the batch, postColumnarParse throws. */
    public void testAbsentDocThrows() throws IOException {
        MapperService mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(DEFAULT_PATH).field("type", "date").endObject())
        );

        // 2-doc batch; only doc 0 has a timestamp — doc 1 is absent in the sparse column.
        BatchMappingContext context = contextWithNDocs(mapperService, 2);
        context.addColumn(sparseTimestampColumn(2, 1_000_000L, 0));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context)
        );
        assertThat(ex.getMessage(), equalTo("data stream timestamp field [" + DEFAULT_PATH + "] is missing"));
    }

    /** When all documents in the batch have a @timestamp value, postColumnarParse succeeds. */
    public void testAllDocsPresent() throws IOException {
        MapperService mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(DEFAULT_PATH).field("type", "date").endObject())
        );

        BatchMappingContext context = contextWithNDocs(mapperService, 3);
        context.addColumn(denseTimestampColumn(1_000L, 2_000L, 3_000L));

        DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context); // must not throw
    }

    /** A single-doc batch with a present @timestamp succeeds. */
    public void testSingleDocPresent() throws IOException {
        MapperService mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(DEFAULT_PATH).field("type", "date").endObject())
        );

        BatchMappingContext context = contextWithNDocs(mapperService, 1);
        context.addColumn(denseTimestampColumn(42_000L));

        DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context); // must not throw
    }

    /** First doc absent in an otherwise populated batch also triggers the missing-timestamp error. */
    public void testFirstDocAbsentThrows() throws IOException {
        MapperService mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(DEFAULT_PATH).field("type", "date").endObject())
        );

        // 3-doc batch; only docs 1 and 2 are present — doc 0 is absent.
        BatchMappingContext context = contextWithNDocs(mapperService, 3);
        context.addColumn(sparseTimestampColumn(3, 1_000_000L, 1, 2));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context)
        );
        assertThat(ex.getMessage(), equalTo("data stream timestamp field [" + DEFAULT_PATH + "] is missing"));
    }
}
