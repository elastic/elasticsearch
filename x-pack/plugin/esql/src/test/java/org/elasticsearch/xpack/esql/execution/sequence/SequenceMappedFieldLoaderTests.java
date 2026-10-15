/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.lucene.read.ComputeBlockLoaderFactory;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockStoredFieldsReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** Real mapped loaders exercise cached reader accounting, backwards document reads and failed row-stride builders. */
public class SequenceMappedFieldLoaderTests extends ESTestCase {
    public void testDocValueReaderCanBeReusedAndRebuiltForEarlierDocument() throws Exception {
        var breaker = new TrackingBreaker();
        var factory = factory(breaker);
        try (var directory = newDirectory(); var writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (int doc = 0; doc < 2; doc++) {
                var document = new Document();
                document.add(new NumericDocValuesField("value", 9_007_199_254_740_993L + doc));
                writer.addDocument(document);
            }
            try (
                var reader = DirectoryReader.open(writer);
                var loaderFactory = new ComputeBlockLoaderFactory(factory);
                var field = new SequenceDocValuesFetchSubPhase.FieldLoader("value", new LongsBlockLoader("value"))
            ) {
                assertEquals(1, reader.leaves().size());
                var leaf = reader.leaves().getFirst();
                field.setNextReader(leaf);
                for (int doc : new int[] { 0, 1, 0 }) {
                    try (Block block = field.read(hit(leaf, doc, Map.of()), factory, loaderFactory)) {
                        assertEquals(9_007_199_254_740_993L + doc, BlockUtils.toJavaObject(block, 0));
                    }
                    assertTrue("cached reader retains its reservation", breaker.getUsed() > 0);
                }
                field.setNextReader(leaf);
                assertEquals(0, breaker.getUsed());
            }
        }
        assertEquals(0, breaker.getUsed());
        factory.ensureAllBlocksAreReleased();
    }

    public void testStoredKeywordUsesRawStoredBytesAndPreservesOrderAndDuplicates() throws Exception {
        var breaker = new TrackingBreaker();
        var factory = factory(breaker);
        try (
            var loaderFactory = new ComputeBlockLoaderFactory(factory);
            var field = new SequenceDocValuesFetchSubPhase.FieldLoader(
                "stored",
                new BlockStoredFieldsReader.BytesFromBytesRefsBlockLoader("stored")
            )
        ) {
            field.setNextReader(null);
            List<Object> values = List.of(new BytesRef("z"), new BytesRef("a"), new BytesRef("z"));
            try (Block block = field.read(hit(null, 0, Map.of("stored", values)), factory, loaderFactory)) {
                assertEquals(values, BlockUtils.toJavaObject(block, 0));
            }
            assertTrue(breaker.getUsed() > 0);
        }
        assertEquals(0, breaker.getUsed());
        factory.ensureAllBlocksAreReleased();
    }

    public void testRowLoadingExceptionReleasesBuilderAndCachedReader() throws Exception {
        var breaker = new TrackingBreaker();
        var factory = factory(breaker);
        var closed = new AtomicInteger();
        // Only the final failure is injected: the stored-field reader, builder and breaker accounting are production classes.
        var failing = new BlockStoredFieldsReader.BytesFromBytesRefsBlockLoader("stored") {
            @Override
            public BlockLoader.RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
                var delegate = super.rowStrideReader(breaker, context);
                return new BlockLoader.RowStrideReader() {
                    @Override
                    public boolean canReuse(int doc) {
                        return delegate.canReuse(doc);
                    }

                    @Override
                    public void read(int doc, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
                        delegate.read(doc, storedFields, builder);
                        throw new IOException("injected mapped read failure");
                    }

                    @Override
                    public void close() {
                        delegate.close();
                        closed.incrementAndGet();
                    }
                };
            }
        };
        try (
            var loaderFactory = new ComputeBlockLoaderFactory(factory);
            var field = new SequenceDocValuesFetchSubPhase.FieldLoader("stored", failing)
        ) {
            field.setNextReader(null);
            var hit = hit(null, 0, Map.of("stored", List.of(new BytesRef("allocated before failure"))));
            assertEquals(
                "injected mapped read failure",
                expectThrows(IOException.class, () -> field.read(hit, factory, loaderFactory)).getMessage()
            );
            assertTrue(breaker.getUsed() > 0);
        }
        assertEquals(1, closed.get());
        assertEquals(0, breaker.getUsed());
        factory.ensureAllBlocksAreReleased();
    }

    public void testAdditionalRetainedBytesCoverNativeArraysOnly() {
        BytesRef value = new BytesRef(new byte[1024 * 1024]);
        var field = new DocumentField("payload", List.of(value));
        long retained = SequenceDocValuesFetchSubPhase.additionalRetainedBytes(field);
        assertTrue("the copied byte array must remain reserved after its compute block is closed", retained > value.bytes.length);
        assertTrue(retained >= value.bytes.length);
        assertEquals(0L, SequenceDocValuesFetchSubPhase.additionalRetainedBytes(new DocumentField("payload", List.of("ordinary text"))));
    }

    public void testReaderReservationsFollowDynamicSettingsAndResetToNodeDefaults() {
        var ordinals = PlannerSettings.BLOCK_LOADER_SIZE_ORDINALS;
        var script = PlannerSettings.BLOCK_LOADER_SIZE_SCRIPT;
        var settings = new ClusterSettings(
            Settings.builder().put(ordinals.getKey(), "11kb").put(script.getKey(), "22kb").build(),
            Set.of(ordinals, script)
        );
        assertEquals(
            new SequenceDocValuesFetchSubPhase.LoaderSizes(ByteSizeValue.ofKb(11), ByteSizeValue.ofKb(22)),
            SequenceDocValuesFetchSubPhase.loaderSizes(settings)
        );
        settings.applySettings(Settings.builder().put(ordinals.getKey(), "33kb").put(script.getKey(), "44kb").build());
        assertEquals(
            new SequenceDocValuesFetchSubPhase.LoaderSizes(ByteSizeValue.ofKb(33), ByteSizeValue.ofKb(44)),
            SequenceDocValuesFetchSubPhase.loaderSizes(settings)
        );
        settings.applySettings(Settings.EMPTY);
        assertEquals(
            new SequenceDocValuesFetchSubPhase.LoaderSizes(ByteSizeValue.ofKb(11), ByteSizeValue.ofKb(22)),
            SequenceDocValuesFetchSubPhase.loaderSizes(settings)
        );
    }

    private static HitContext hit(LeafReaderContext leaf, int doc, Map<String, List<Object>> fields) {
        return new HitContext(SearchHit.unpooled(doc, "document"), leaf, doc, fields, Source.empty(XContentType.JSON), null);
    }

    private static MockBlockFactory factory(TrackingBreaker breaker) {
        return new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
    }

    /** Counts actual production reader reservations while leaving admission unlimited for these lifecycle tests. */
    private static final class TrackingBreaker extends NoopCircuitBreaker {
        private long used;

        private TrackingBreaker() {
            super("sequence-mapped-reader");
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            used += bytes;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        @Override
        public long getUsed() {
            return used;
        }
    }
}
