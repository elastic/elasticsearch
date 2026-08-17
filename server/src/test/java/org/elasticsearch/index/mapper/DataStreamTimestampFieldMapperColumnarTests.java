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
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import static org.hamcrest.Matchers.containsString;
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
     * TIME_SERIES settings with a fixed 24-hour window: [2021-04-28T00:00:00Z, 2021-04-29T00:00:00Z).
     * {@link IndexMode#shouldValidateTimestamp()} returns {@code true} for this mode, so
     * {@link DataStreamTimestampFieldMapper#postColumnarParse} will validate each timestamp against these bounds.
     */
    private static Settings timeSeriesSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();
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
        assertThat(ex.getMessage(), equalTo("document [1] is missing data stream timestamp field [" + DEFAULT_PATH + "]"));
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
        assertThat(ex.getMessage(), equalTo("document [0] is missing data stream timestamp field [" + DEFAULT_PATH + "]"));
    }

    /** A timestamp before the time-series window start is rejected. */
    public void testTimestampBeforeStartBoundsThrows() throws IOException {
        MapperService mapperService = createMapperService(timeSeriesSettings(), mapping(b -> {
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(DEFAULT_PATH).field("type", "date").endObject();
        }));

        // 1 ms before the 2021-04-28T00:00:00Z window start
        BatchMappingContext context = contextWithNDocs(mapperService, 1);
        context.addColumn(denseTimestampColumn(1_619_567_999_999L));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context)
        );
        assertThat(ex.getMessage(), containsString("must be larger than"));
    }

    /** A timestamp exactly at the time-series window end is rejected (end is exclusive). */
    public void testTimestampAtEndBoundsThrows() throws IOException {
        MapperService mapperService = createMapperService(timeSeriesSettings(), mapping(b -> {
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(DEFAULT_PATH).field("type", "date").endObject();
        }));

        // Exactly 2021-04-29T00:00:00Z — equal to end, so out of bounds
        BatchMappingContext context = contextWithNDocs(mapperService, 1);
        context.addColumn(denseTimestampColumn(1_619_654_400_000L));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context)
        );
        assertThat(ex.getMessage(), containsString("must be smaller than"));
    }

    /** All timestamps within the time-series window pass bounds validation. */
    public void testTimestampWithinBoundsSucceeds() throws IOException {
        MapperService mapperService = createMapperService(timeSeriesSettings(), mapping(b -> {
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(DEFAULT_PATH).field("type", "date").endObject();
        }));

        // Window start (inclusive) and midday — both valid
        BatchMappingContext context = contextWithNDocs(mapperService, 2);
        context.addColumn(denseTimestampColumn(1_619_568_000_000L, 1_619_610_000_000L));

        DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context); // must not throw
    }

    /**
     * date_nanos timestamps are stored as epoch-nanoseconds; {@code validateTimestampValue} must
     * divide by {@code NSEC_PER_MSEC} before comparing against the millisecond-precision bounds.
     * This test confirms that a nanosecond value falling within the window is accepted.
     */
    public void testDateNanosTimestampWithinBoundsSucceeds() throws IOException {
        MapperService mapperService = dateNanosTimeSeriesMapperService();

        // 2021-04-28T12:00:00Z in nanoseconds — midday, well within the window
        BatchMappingContext context = contextWithNDocs(mapperService, 1);
        context.addColumn(denseTimestampColumn(1_619_611_200_000_000_000L));

        DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context); // must not throw
    }

    /**
     * date_nanos timestamps that fall outside the time-series window must be rejected after the
     * nanosecond-to-millisecond conversion.
     */
    public void testDateNanosTimestampBeforeStartBoundsThrows() throws IOException {
        MapperService mapperService = dateNanosTimeSeriesMapperService();

        // 1 ms before 2021-04-28T00:00:00Z, expressed in nanoseconds
        BatchMappingContext context = contextWithNDocs(mapperService, 1);
        context.addColumn(denseTimestampColumn(1_619_567_999_999_000_000L));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> DataStreamTimestampFieldMapper.ENABLED_INSTANCE.postColumnarParse(context)
        );
        assertThat(ex.getMessage(), containsString("must be larger than"));
    }

    /**
     * TIME_SERIES applies a default mapping that sets {@code @timestamp} to {@code date}.
     * This helper bypasses that to build a mapper service with {@code date_nanos} for {@code @timestamp}
     * from scratch, with the same 24-hour window as {@link #timeSeriesSettings()}.
     */
    private MapperService dateNanosTimeSeriesMapperService() throws IOException {
        MapperService mapperService = new TestMapperServiceBuilder().settings(timeSeriesSettings()).applyDefaultMapping(false).build();
        merge(mapperService, topMapping(b -> {
            b.startObject(DataStreamTimestampFieldMapper.NAME).field("enabled", true).endObject();
            b.startObject("properties");
            b.startObject(DEFAULT_PATH).field("type", "date_nanos").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject();
        }));
        return mapperService;
    }

    /**
     * When the timestamp field is disabled, {@code postColumnarParse} must return immediately
     * without inspecting the mapped columns. Passing a context with no timestamp column verifies
     * that the {@code enabled == false} guard is in place.
     */
    public void testDisabledInstanceSkipsValidation() throws IOException {
        MapperService mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(DEFAULT_PATH).field("type", "date").endObject())
        );

        // No column added — if the enabled==false guard were missing, this would throw
        BatchMappingContext context = contextWithNDocs(mapperService, 2);

        DataStreamTimestampFieldMapper.DISABLED_INSTANCE.postColumnarParse(context); // must not throw
    }
}
