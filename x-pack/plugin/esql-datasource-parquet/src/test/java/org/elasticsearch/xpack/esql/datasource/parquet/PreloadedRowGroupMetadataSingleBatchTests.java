/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.NodeByteBudgetService;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.QueryAdmission;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Guards the single-GET preload for small spans (PR-B v3 1.5). VPC-sized files stay on one
 * unadmitted GET. Spans up to 1 MiB take one admitted GET when the budget has room, and v3's
 * split when it does not.
 */
public class PreloadedRowGroupMetadataSingleBatchTests extends ESTestCase {

    private static final long FLOOR = ParquetFormatReader.FOOTER_TAIL_PREFETCH_BYTES;
    private static final long CEILING = CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP;
    private static final int LANE_O_THREADS = 24;
    private static final List<String> VPC_COLUMNS = List.of(
        "start",
        "action",
        "protocol",
        "srcaddr",
        "dstaddr",
        "srcport",
        "dstport",
        "bytes",
        "packets",
        "account",
        "iface",
        "az",
        "vpc",
        "region"
    );
    private static final List<Set<String>> VPC_PANELS = List.of(
        Set.of("start"),
        Set.of("action"),
        Set.of("protocol"),
        Set.of("srcaddr"),
        Set.of("dstaddr"),
        Set.of("srcport"),
        Set.of("dstport"),
        Set.of("bytes"),
        Set.of("packets"),
        Set.of("account"),
        Set.of("start", "action"),
        Set.of("start", "protocol"),
        Set.of("action", "protocol"),
        Set.of("start", "action", "protocol")
    );

    private final FooterByteCache footerByteCache = FooterByteCache.fromSettings(Settings.EMPTY);
    private final CircuitBreaker breaker = new LimitedBreaker("single-batch", ByteSizeValue.ofMb(32));

    public void testNeededSpanEmptyAndSingleRange() {
        assertEquals(0L, PreloadedRowGroupMetadata.neededSpan(List.of()));
        assertEquals(40L, PreloadedRowGroupMetadata.neededSpan(List.of(new CoalescedRangeReader.ByteRange(10, 40))));
        assertEquals(
            90L,
            PreloadedRowGroupMetadata.neededSpan(
                List.of(new CoalescedRangeReader.ByteRange(10, 20), new CoalescedRangeReader.ByteRange(80, 20))
            )
        );
    }

    public void testSingleBatchAllowanceStepsAndWaiters() throws Exception {
        assertEquals(FLOOR, PreloadedRowGroupMetadata.singleBatchAllowance(null, LANE_O_THREADS));
        assertEquals(FLOOR, PreloadedRowGroupMetadata.singleBatchAllowance(new ParquetIoWatermark(110L << 20), 0));

        assertEquals(CEILING, PreloadedRowGroupMetadata.singleBatchAllowance(new ParquetIoWatermark(110L << 20), LANE_O_THREADS));
        assertEquals(512L << 10, PreloadedRowGroupMetadata.singleBatchAllowance(new ParquetIoWatermark(20L << 20), LANE_O_THREADS));
        assertEquals(256L << 10, PreloadedRowGroupMetadata.singleBatchAllowance(new ParquetIoWatermark(8L << 20), LANE_O_THREADS));
        assertEquals(128L << 10, PreloadedRowGroupMetadata.singleBatchAllowance(new ParquetIoWatermark(4L << 20), LANE_O_THREADS));
        assertEquals(FLOOR, PreloadedRowGroupMetadata.singleBatchAllowance(new ParquetIoWatermark(2L << 20), LANE_O_THREADS));

        NodeByteBudgetService budget = new NodeByteBudgetService(64L << 20);
        ParquetIoWatermark watermark = new ParquetIoWatermark(budget);
        assertEquals(CEILING, PreloadedRowGroupMetadata.singleBatchAllowance(watermark, 1));
        NodeByteBudget.Hold parked = occupyUnderLimit(budget, 60L << 20);
        assertEquals("used under the cap still allows a 1 MiB step", CEILING, PreloadedRowGroupMetadata.singleBatchAllowance(watermark, 1));
        CountDownLatch granted = new CountDownLatch(1);
        try {
            // Null lease: a lease would take the overshoot slot and grant immediately.
            budget.admitAsync(8L << 20, null, () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
                hold.close();
                granted.countDown();
            }, e -> granted.countDown()));
            assertBusy(() -> assertEquals(1, watermark.waiterCount()));
            assertTrue("waiter must queue while used is still under the cap", watermark.used() < watermark.limit());
            assertEquals(FLOOR, PreloadedRowGroupMetadata.singleBatchAllowance(watermark, 1));
        } finally {
            parked.close();
        }
        assertTrue(granted.await(5, TimeUnit.SECONDS));
    }

    public void testVpcSizedOpenIssuesOneUnadmittedGet() throws Exception {
        byte[] file = writeVpcSizedParquet();
        long span = neededSpan(file, Set.of("start"));
        assertThat("VPC fixture span must sit on the unadmitted floor", span, greaterThan(0L));
        assertThat(span, lessThanOrEqualTo(FLOOR));
        ParquetIoWatermark roomy = new ParquetIoWatermark(64L << 20);
        for (Set<String> panel : VPC_PANELS) {
            long panelSpan = neededSpan(file, panel);
            assertThat("panel " + panel, panelSpan, greaterThan(0L));
            assertThat("panel " + panel, panelSpan, lessThanOrEqualTo(FLOOR));
            GetCount result = preloadCounting(file, panel, roomy, 1);
            assertEquals("panel " + panel + " span=" + panelSpan, 1, result.gets);
            assertEquals("VPC floor must not create a reservation, panel=" + panel, 0, result.peakHolders);
            assertEquals(0L, roomy.used());
            assertEquals(0, roomy.holders());
        }
    }

    public void testMidSpanFreeBudgetOneAdmittedGet() throws Exception {
        Fixture fixture = writeSpanBetween(FLOOR + 1, CEILING);
        ParquetIoWatermark watermark = new ParquetIoWatermark(64L << 20);
        AtomicLong peakUsed = new AtomicLong();
        GetCount result = preloadCounting(fixture.bytes, Set.of("a"), watermark, 1, peakUsed);
        assertEquals("free budget must collapse a " + fixture.span + " span to one GET", 1, result.gets);
        assertEquals("admitted single GET must hold during dispatch", 1, result.peakHolders);
        assertThat(peakUsed.get(), lessThanOrEqualTo(watermark.limit()));
        assertEquals(0L, watermark.used());
        assertEquals(0, watermark.holders());
    }

    public void testMidSpanFullBudgetOrWaiterKeepsSplit() throws Exception {
        Fixture fixture = writeSpanBetween(FLOOR + 1, CEILING);
        ParquetIoWatermark full = new ParquetIoWatermark(1);
        GetCount fullResult = preloadCounting(fixture.bytes, Set.of("a"), full, 1);
        assertThat("full budget must keep v3's split, span=" + fixture.span, fullResult.gets, greaterThan(1));
        assertEquals(0L, full.used());

        NodeByteBudgetService budget = new NodeByteBudgetService(64L << 20);
        ParquetIoWatermark watermark = new ParquetIoWatermark(budget);
        NodeByteBudget.Hold parked = occupyUnderLimit(budget, 60L << 20);
        assertEquals(
            "without a waiter this budget would admit a 1 MiB GET",
            CEILING,
            PreloadedRowGroupMetadata.singleBatchAllowance(watermark, 1)
        );
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<NodeByteBudget.Hold> waiter = new AtomicReference<>();
        try {
            budget.admitAsync(8L << 20, null, () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
                waiter.set(hold);
                granted.countDown();
            }, e -> granted.countDown()));
            assertBusy(() -> assertEquals(1, watermark.waiterCount()));
            assertTrue("waiter must queue while used is still under the cap", watermark.used() < watermark.limit());
            GetCount waiterResult = preloadCounting(fixture.bytes, Set.of("a"), watermark, 1);
            assertEquals("queued waiter must keep the same split GET count", fullResult.gets, waiterResult.gets);
            assertEquals("preload must not jump the FIFO", 1, watermark.waiterCount());
            assertNull(waiter.get());
        } finally {
            parked.close();
        }
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertNotNull(waiter.get());
        waiter.get().close();
        assertEquals(0, watermark.waiterCount());
        assertEquals(0L, watermark.used());
    }

    public void testTryAdmitSingleBatchUsesHeapEstimateAndRequiresPreWarm() {
        long span = 200_000L;
        long estimate = HeapFootprint.byteArrayBytes(span);
        assertThat(estimate, greaterThan(span));

        ParquetIoWatermark roomy = new ParquetIoWatermark(64L << 20);
        assertNull("floor must not reserve", PreloadedRowGroupMetadata.tryAdmitSingleBatch(roomy, FLOOR, CEILING, true));
        assertNull("no pre-warm must not reserve", PreloadedRowGroupMetadata.tryAdmitSingleBatch(roomy, span, CEILING, false));
        assertNull("over allowance must not reserve", PreloadedRowGroupMetadata.tryAdmitSingleBatch(roomy, span, span - 1, true));
        ParquetIoWatermark.AdmitHold granted = PreloadedRowGroupMetadata.tryAdmitSingleBatch(roomy, span, CEILING, true);
        assertNotNull(granted);
        granted.drop();
        assertEquals(0, roomy.holders());

        ParquetIoWatermark tight = new ParquetIoWatermark(estimate - 1);
        assertNull("estimate-1 must refuse", PreloadedRowGroupMetadata.tryAdmitSingleBatch(tight, span, CEILING, true));
        ParquetIoWatermark.AdmitHold plain = tight.tryAdmit(span);
        assertNotNull("plain span would have been granted — production must charge the heap estimate", plain);
        plain.drop();
    }

    public void testSingleBatchMatchesSplitIndexesAndChunks() throws Exception {
        Fixture fixture = writeSpanBetween(FLOOR + 1, CEILING);
        ParquetIoWatermark admittedWm = new ParquetIoWatermark(64L << 20);
        ParquetIoWatermark splitWm = new ParquetIoWatermark(1);
        CountingGetsStorage admittedStorage = new CountingGetsStorage(fixture.bytes, admittedWm, null);
        CountingGetsStorage splitStorage = new CountingGetsStorage(fixture.bytes, splitWm, null);
        try (ParquetFileReader reader = openReader(fixture.bytes)) {
            try (
                PreloadedRowGroupMetadata admitted = preload(reader, admittedStorage, Set.of("a"), admittedWm);
                PreloadedRowGroupMetadata split = preload(reader, splitStorage, Set.of("a"), splitWm)
            ) {
                assertEquals("admitted path must be one GET", 1, admittedStorage.gets.get());
                assertThat("split path must issue more than one GET", splitStorage.gets.get(), greaterThan(1));
                assertEquivalent(admitted, split, reader);
            }
        }
        assertEquals(0L, admittedWm.used());
        assertEquals(0L, splitWm.used());
    }

    public void testThrowBeforeDispatchAndFailedGetDropHold() throws Exception {
        Fixture fixture = writeSpanBetween(FLOOR + 1, CEILING);
        ParquetIoWatermark throwWatermark = new ParquetIoWatermark(64L << 20);
        CountingGetsStorage throwing = throwingStorage(fixture.bytes, throwWatermark);
        try (ParquetFileReader reader = openReader(fixture.bytes)) {
            try (
                PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(
                    reader,
                    throwing,
                    Set.of("a"),
                    null,
                    null,
                    Integer.MAX_VALUE,
                    breaker,
                    throwWatermark,
                    null,
                    QueryAdmission.DEFAULT_ACQUIRE_TIMEOUT_MS,
                    1
                )
            ) {
                metadata.releaseRawBuffers();
            }
        }
        assertThat(throwing.gets.get(), greaterThanOrEqualTo(1));
        assertThat(throwing.peakHolders.get(), greaterThanOrEqualTo(1));
        assertEquals(0L, throwWatermark.used());
        assertEquals(0, throwWatermark.holders());

        ParquetIoWatermark failWatermark = new ParquetIoWatermark(64L << 20);
        CountingGetsStorage failing = failingStorage(fixture.bytes, failWatermark);
        try (ParquetFileReader reader = openReader(fixture.bytes)) {
            try (
                PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(
                    reader,
                    failing,
                    Set.of("a"),
                    null,
                    null,
                    Integer.MAX_VALUE,
                    breaker,
                    failWatermark,
                    null,
                    QueryAdmission.DEFAULT_ACQUIRE_TIMEOUT_MS,
                    1
                )
            ) {
                metadata.releaseRawBuffers();
            }
        }
        assertThat(failing.gets.get(), greaterThanOrEqualTo(1));
        assertThat(failing.peakHolders.get(), greaterThanOrEqualTo(1));
        assertEquals(0L, failWatermark.used());
        assertEquals(0, failWatermark.holders());
    }

    private GetCount preloadCounting(byte[] file, Set<String> predicates, ParquetIoWatermark watermark, int ioThreads) throws Exception {
        return preloadCounting(file, predicates, watermark, ioThreads, null);
    }

    private GetCount preloadCounting(byte[] file, Set<String> predicates, ParquetIoWatermark watermark, int ioThreads, AtomicLong peakUsed)
        throws Exception {
        CountingGetsStorage storage = new CountingGetsStorage(file, watermark, peakUsed);
        try (ParquetFileReader reader = openReader(file)) {
            try (
                PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(
                    reader,
                    storage,
                    predicates,
                    null,
                    null,
                    Integer.MAX_VALUE,
                    breaker,
                    watermark,
                    null,
                    QueryAdmission.DEFAULT_ACQUIRE_TIMEOUT_MS,
                    ioThreads
                )
            ) {
                metadata.releaseRawBuffers();
            }
        }
        return new GetCount(storage.gets.get(), storage.peakHolders.get());
    }

    private PreloadedRowGroupMetadata preload(
        ParquetFileReader reader,
        StorageObject storage,
        Set<String> predicates,
        ParquetIoWatermark watermark
    ) throws IOException {
        return PreloadedRowGroupMetadata.preload(
            reader,
            storage,
            predicates,
            null,
            null,
            Integer.MAX_VALUE,
            breaker,
            watermark,
            null,
            QueryAdmission.DEFAULT_ACQUIRE_TIMEOUT_MS,
            1
        );
    }

    private static void assertEquivalent(PreloadedRowGroupMetadata a, PreloadedRowGroupMetadata b, ParquetFileReader reader) {
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        for (int rg = 0; rg < rowGroups.size(); rg++) {
            for (ColumnChunkMetaData col : rowGroups.get(rg).getColumns()) {
                String path = col.getPath().toDotString();
                ColumnIndex aci = a.getColumnIndex(rg, path);
                ColumnIndex bci = b.getColumnIndex(rg, path);
                assertEquals(path + " column-index presence rg=" + rg, aci == null, bci == null);
                if (aci != null) {
                    assertByteBuffersEqual(path + " min rg=" + rg, aci.getMinValues(), bci.getMinValues());
                    assertByteBuffersEqual(path + " max rg=" + rg, aci.getMaxValues(), bci.getMaxValues());
                }
                OffsetIndex aoi = a.getOffsetIndex(rg, path);
                OffsetIndex boi = b.getOffsetIndex(rg, path);
                assertEquals(path + " offset-index presence rg=" + rg, aoi == null, boi == null);
                if (aoi != null) {
                    assertEquals(path + " page count rg=" + rg, aoi.getPageCount(), boi.getPageCount());
                    for (int page = 0; page < aoi.getPageCount(); page++) {
                        assertEquals(path + " page offset rg=" + rg, aoi.getOffset(page), boi.getOffset(page));
                    }
                }
            }
        }
        assertEquals(a.preWarmedChunks().keySet(), b.preWarmedChunks().keySet());
        for (var e : a.preWarmedChunks().entrySet()) {
            ColumnChunkPrefetcher.PrefetchedChunk other = b.preWarmedChunks().get(e.getKey());
            assertEquals(e.getValue().length(), other.length());
            assertArrayEquals(copyRemaining(e.getValue().data()), copyRemaining(other.data()));
        }
    }

    private static void assertByteBuffersEqual(String label, List<ByteBuffer> a, List<ByteBuffer> b) {
        assertEquals(label + " size", a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertArrayEquals(label + " [" + i + "]", copyRemaining(a.get(i)), copyRemaining(b.get(i)));
        }
    }

    private static byte[] copyRemaining(ByteBuffer buffer) {
        ByteBuffer view = buffer.duplicate();
        byte[] bytes = new byte[view.remaining()];
        view.get(bytes);
        return bytes;
    }

    private ParquetFileReader openReader(byte[] file) throws IOException {
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        return ParquetFileReader.open(new ParquetStorageObjectAdapter(createSyncStorage(file), footerByteCache, breaker), options);
    }

    private long neededSpan(byte[] file, Set<String> predicates) throws IOException {
        try (ParquetFileReader reader = openReader(file)) {
            return PreloadedRowGroupMetadata.neededSpan(collectNeededRanges(reader, predicates));
        }
    }

    private static List<CoalescedRangeReader.ByteRange> collectNeededRanges(ParquetFileReader reader, Set<String> predicates) {
        List<CoalescedRangeReader.ByteRange> ranges = new ArrayList<>();
        boolean fetchPreWarm = predicates != null && predicates.isEmpty() == false;
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            for (ColumnChunkMetaData col : rowGroups.get(rgIdx).getColumns()) {
                String path = col.getPath().toDotString();
                IndexReference ci = col.getColumnIndexReference();
                if (ci != null && ci.getLength() > 0) {
                    ranges.add(new CoalescedRangeReader.ByteRange(ci.getOffset(), ci.getLength()));
                }
                IndexReference oi = col.getOffsetIndexReference();
                if (oi != null && oi.getLength() > 0) {
                    ranges.add(new CoalescedRangeReader.ByteRange(oi.getOffset(), oi.getLength()));
                }
                if (fetchPreWarm && predicates.contains(path)) {
                    CoalescedRangeReader.ByteRange dict = ColumnChunkPrefetcher.dictionaryPageRange(col, col.getFirstDataPageOffset());
                    if (dict != null) {
                        ranges.add(dict);
                    }
                    if (col.getBloomFilterOffset() > 0 && col.getBloomFilterLength() > 0) {
                        ranges.add(new CoalescedRangeReader.ByteRange(col.getBloomFilterOffset(), col.getBloomFilterLength()));
                    }
                }
            }
        }
        return ranges;
    }

    private static byte[] writeVpcSizedParquet() throws IOException {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (String name : VPC_COLUMNS) {
            builder.required(PrimitiveType.PrimitiveTypeName.INT64).named(name);
        }
        MessageType schema = builder.named("vpc");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withPageSize(512)
                .withDictionaryPageSize(1024)
                .withRowGroupSize(64 * 1024L)
                .build()
        ) {
            for (int i = 0; i < 16; i++) {
                Group g = groups.newGroup();
                for (String name : VPC_COLUMNS) {
                    g.add(name, (long) (i % 8));
                }
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    private static Fixture writeSpanBetween(long minSpan, long maxSpan) throws IOException {
        int pad = 32;
        int rowsPerGroup = 256;
        int groups = 8;
        AssertionError last = new AssertionError("no fixture");
        for (int attempt = 0; attempt < 14; attempt++) {
            byte[] file = writePaddedRowGroups(groups, rowsPerGroup, pad);
            long span;
            int rowGroups;
            int rangeCount;
            try (
                ParquetFileReader reader = ParquetFileReader.open(
                    new ParquetStorageObjectAdapter(
                        createSyncStorage(file),
                        FooterByteCache.fromSettings(Settings.EMPTY),
                        NoopCircuitBreaker.INSTANCE
                    ),
                    PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
                )
            ) {
                List<CoalescedRangeReader.ByteRange> ranges = collectNeededRanges(reader, Set.of("a"));
                span = PreloadedRowGroupMetadata.neededSpan(ranges);
                rowGroups = reader.getRowGroups().size();
                rangeCount = ranges.size();
            }
            if (span >= minSpan && span <= maxSpan) {
                assertThat("fixture must write several row groups", rowGroups, greaterThanOrEqualTo(8));
                assertThat("fixture must collect index and dictionary ranges", rangeCount, greaterThanOrEqualTo(2));
                return new Fixture(file, span);
            }
            last = new AssertionError("span " + span + " not in [" + minSpan + ", " + maxSpan + "] pad=" + pad + " rows=" + rowsPerGroup);
            if (span < minSpan) {
                pad = Math.max(pad + 16, pad * 2);
                rowsPerGroup = Math.min(rowsPerGroup * 2, 4_096);
            } else {
                pad = Math.max(8, pad / 2);
                rowsPerGroup = Math.max(64, rowsPerGroup / 2);
            }
        }
        throw last;
    }

    private static byte[] writePaddedRowGroups(int groups, int rowsPerGroup, int padBytes) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("a")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("pad")
            .named("schema");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory groupsFactory = new SimpleGroupFactory(schema);
        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");
        int rows = groups * rowsPerGroup;
        long rowGroupSize = Math.max(8 * 1024L, (long) rowsPerGroup * (8 + padBytes));
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withDictionaryEncoding("pad", false)
                .withPageSize(4 * 1024)
                .withDictionaryPageSize(16 * 1024)
                .withRowGroupSize(rowGroupSize)
                .build()
        ) {
            for (int i = 0; i < rows; i++) {
                byte[] pad = new byte[padBytes];
                pad[0] = (byte) i;
                pad[padBytes - 1] = (byte) (i >> 8);
                writer.write(groupsFactory.newGroup().append("a", (long) (i % 16)).append("pad", Binary.fromConstantByteArray(pad)));
            }
        }
        return out.toByteArray();
    }

    private static StorageObject createSyncStorage(byte[] data) {
        return new AbstractTestStorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.EPOCH;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://single-batch.parquet");
            }
        };
    }

    private static CountingGetsStorage throwingStorage(byte[] data, ParquetIoWatermark watermark) {
        return new CountingGetsStorage(data, watermark, null) {
            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                java.util.concurrent.Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                recordDispatch();
                throw new IllegalStateException("throw before dispatch");
            }
        };
    }

    private static CountingGetsStorage failingStorage(byte[] data, ParquetIoWatermark watermark) {
        return new CountingGetsStorage(data, watermark, null) {
            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                java.util.concurrent.Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                recordDispatch();
                listener.onFailure(new IOException("failed GET"));
            }
        };
    }

    private static NodeByteBudget.Hold occupyUnderLimit(NodeByteBudgetService budget, long bytes) {
        NodeByteBudget.Hold hold = budget.tryAdmit(bytes);
        assertNotNull("must park under the cap", hold);
        assertTrue("used must stay under limit so the waiter, not overshoot, forces the floor", budget.used() < budget.limit());
        return hold;
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return out.size();
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://single-batch.parquet";
            }
        };
    }

    private record Fixture(byte[] bytes, long span) {}

    private record GetCount(int gets, int peakHolders) {}

    private static class CountingGetsStorage extends AbstractTestStorageObject {
        private final byte[] data;
        private final ParquetIoWatermark watermark;
        private final AtomicLong peakUsed;
        private final AtomicInteger gets = new AtomicInteger();
        private final AtomicInteger peakHolders = new AtomicInteger();

        private CountingGetsStorage(byte[] data, ParquetIoWatermark watermark, AtomicLong peakUsed) {
            this.data = data;
            this.watermark = watermark;
            this.peakUsed = peakUsed;
        }

        void recordDispatch() {
            int n = gets.incrementAndGet();
            if (watermark != null) {
                // Sample before the first alloc. Later GETs see forceAdd leftovers, which
                // make holders() look non-zero even without an AdmitHold.
                if (n == 1) {
                    peakHolders.set(watermark.holders());
                }
                if (peakUsed != null) {
                    peakUsed.accumulateAndGet(watermark.used(), Math::max);
                }
            }
        }

        @Override
        public boolean supportsNativeAsync() {
            return true;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            java.util.concurrent.Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            recordDispatch();
            try {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                DirectReadBuffer dest = factory.allocateWritableWindow(len);
                dest.buffer().put(data, pos, len);
                dest.buffer().flip();
                if (watermark != null && peakUsed != null) {
                    peakUsed.accumulateAndGet(watermark.used(), Math::max);
                }
                listener.onResponse(dest);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://single-batch.parquet");
        }
    }
}
