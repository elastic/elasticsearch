/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Verifies that {@link SegmentStatsCollector}s registered for an index are filtered by {@link SegmentStatsCollector#appliesTo}
 * against the index's mapping, that an applicable collector is handed every doc values field when a segment is flushed, and
 * that the attributes it records end up in the segment info. This is the extension point that lets a module (e.g. serverless
 * metering) derive per-segment statistics without touching the formats that write the segment.
 */
public class SegmentStatsCollectorTests extends MapperServiceTestCase {
    private static final String TRACKED = "tracked";
    private static final String SUM_ATTRIBUTE = "tracked_sum";

    /** Applies to indices mapping the tracked field and records the sum of its values as a segment attribute. */
    private static class SumCollector implements SegmentStatsCollector {
        final AtomicInteger flushes = new AtomicInteger();
        final AtomicInteger notifications = new AtomicInteger();
        final Set<String> seenFields = ConcurrentHashMap.newKeySet();

        @Override
        public boolean appliesTo(@Nullable MapperService mapperService) {
            return mapperService != null && mapperService.mappingLookup().getMapper(TRACKED) != null;
        }

        @Override
        public void onFlush(SegmentInfo segment, FieldInfo field, DocValuesProducer values) throws IOException {
            assertThat("doc values updates must not be reported", field.getDocValuesGen(), equalTo(-1L));
            notifications.incrementAndGet();
            seenFields.add(field.name);
            if (TRACKED.equals(field.name) == false) {
                return;
            }
            flushes.incrementAndGet();
            NumericDocValues numeric = values.getNumeric(field);
            long sum = 0;
            while (numeric.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                sum += numeric.longValue();
            }
            segment.putAttribute(SUM_ATTRIBUTE, Long.toString(sum));
        }

        @Override
        public MergeCollector onMergeStart(SegmentInfo mergedSegment) {
            return (CodecReader source) -> fail("merges are observed by the engine, not the codec");
        }
    }

    private static final String MAPPING_WITH_TRACKED = """
        { "_doc": { "properties": { "tracked": { "type": "long" }, "other": { "type": "long" } } } }""";
    private static final String MAPPING_WITHOUT_TRACKED = """
        { "_doc": { "properties": { "other": { "type": "long" } } } }""";

    public void testSupplierFiltersCollectorsByApplicability() throws IOException {
        SumCollector collector = new SumCollector();
        SegmentStatsCollector never = new SumCollector() {
            @Override
            public boolean appliesTo(MapperService mapperService) {
                return false;
            }
        };
        var registered = SegmentStatsCollectors.of(List.of(collector, never));
        var withTracked = new PerFieldFormatSupplier(
            createMapperService(MAPPING_WITH_TRACKED),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            registered
        );
        assertThat(withTracked.getSegmentStatsCollectors().size(), equalTo(1));

        var withoutTracked = new PerFieldFormatSupplier(
            createMapperService(MAPPING_WITHOUT_TRACKED),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            registered
        );
        assertThat(withoutTracked.getSegmentStatsCollectors(), sameInstance(SegmentStatsCollectors.NONE));

        // a missing mapper service is forwarded to the collectors, which decide for themselves
        var withoutMapperService = new PerFieldFormatSupplier(null, BigArrays.NON_RECYCLING_INSTANCE, null, registered);
        assertThat(withoutMapperService.getSegmentStatsCollectors(), sameInstance(SegmentStatsCollectors.NONE));
        var appliesWithoutMapperService = SegmentStatsCollectors.of(List.of(new OrderRecording(new ArrayList<>(), true)));
        assertThat(
            new PerFieldFormatSupplier(null, BigArrays.NON_RECYCLING_INSTANCE, null, appliesWithoutMapperService)
                .getSegmentStatsCollectors(),
            sameInstance(appliesWithoutMapperService)
        );

        // all registered collectors apply: the registered instance is returned as is, without allocating
        var allApplicable = SegmentStatsCollectors.of(List.of(collector));
        var onlyApplicable = new PerFieldFormatSupplier(
            createMapperService(MAPPING_WITH_TRACKED),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            allApplicable
        );
        assertThat(onlyApplicable.getSegmentStatsCollectors(), sameInstance(allApplicable));
    }

    public void testApplicableToFiltersAndKeepsOrder() throws IOException {
        MapperService mapperService = createMapperService(MAPPING_WITH_TRACKED);
        SumCollector first = new SumCollector();
        SumCollector never = new SumCollector() {
            @Override
            public boolean appliesTo(MapperService mapperService) {
                return false;
            }
        };
        SumCollector last = new SumCollector();
        var all = SegmentStatsCollectors.of(List.of(first, never, last));

        var applicable = all.applicableTo(mapperService);
        assertThat(applicable.size(), equalTo(2));
        assertThat(SegmentStatsCollectors.of(List.of(first, last)).applicableTo(mapperService).size(), equalTo(2));
        assertThat(SegmentStatsCollectors.of(List.of(never)).applicableTo(mapperService), sameInstance(SegmentStatsCollectors.NONE));
        assertThat(SegmentStatsCollectors.NONE.applicableTo(mapperService), sameInstance(SegmentStatsCollectors.NONE));

        // the surviving collectors are notified in registration order
        List<SegmentStatsCollector> order = new ArrayList<>();
        OrderRecording firstRecording = new OrderRecording(order, true);
        OrderRecording skipped = new OrderRecording(order, false);
        OrderRecording lastRecording = new OrderRecording(order, true);
        SegmentStatsCollectors.of(List.of(firstRecording, skipped, lastRecording)).applicableTo(mapperService).onFlush(null, null, null);
        assertThat(order, equalTo(List.of(firstRecording, lastRecording)));
    }

    /** Records itself on flush so notification order can be asserted. */
    private static class OrderRecording implements SegmentStatsCollector {
        final List<SegmentStatsCollector> order;
        final boolean applies;

        OrderRecording(List<SegmentStatsCollector> order, boolean applies) {
            this.order = order;
            this.applies = applies;
        }

        @Override
        public boolean appliesTo(MapperService mapperService) {
            return applies;
        }

        @Override
        public void onFlush(SegmentInfo segment, FieldInfo field, DocValuesProducer values) throws IOException {
            order.add(this);
        }

        @Override
        public MergeCollector onMergeStart(SegmentInfo mergedSegment) {
            return source -> {};
        }
    }

    public void testApplicableCollectorSeesEveryFieldOnFlush() throws IOException {
        SumCollector collector = new SumCollector();
        withIndex(createMapperService(MAPPING_WITH_TRACKED), List.of(collector), iw -> {
            for (long value : new long[] { 3, 5, 7 }) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField(TRACKED, value));
                doc.add(new NumericDocValuesField("other", 100));
                iw.addDocument(doc);
            }
            // no forceMerge: merges are not observed through the codec but by the engine (see IndexEngine in stateless)
            iw.commit();
        }, reader -> {
            long total = 0;
            for (LeafReaderContext leaf : reader.leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(leaf.reader());
                String attribute = segmentReader.getSegmentInfo().info.getAttribute(SUM_ATTRIBUTE);
                assertNotNull("segment [" + segmentReader.getSegmentName() + "] has no attribute", attribute);
                total += Long.parseLong(attribute);
            }
            // whether the random writer flushed one or several segments, the sum of segment sums covers all values
            assertThat(total, equalTo(15L));
        });
        assertTrue("collector should have observed at least one flush", collector.flushes.get() > 0);
        // the collector is handed every doc values field once and filters itself
        assertThat(collector.seenFields, hasItems(TRACKED, "other"));
    }

    public void testDocValuesUpdatesAreNotReported() throws IOException {
        SumCollector collector = new SumCollector();
        withIndex(createMapperService(MAPPING_WITH_TRACKED), List.of(collector), iw -> {
            Document doc = new Document();
            doc.add(new StringField("id", "1", Field.Store.NO));
            doc.add(new NumericDocValuesField(TRACKED, 3));
            doc.add(new NumericDocValuesField("other", 100));
            iw.addDocument(doc);
            iw.commit();
            int notificationsAfterFlush = collector.notifications.get();
            assertTrue(notificationsAfterFlush > 0);

            // a doc values update writes a new generation of the field for the existing segment; no new segment is flushed
            iw.updateNumericDocValue(new Term("id", "1"), "other", 200L);
            iw.commit();
            assertThat(collector.notifications.get(), equalTo(notificationsAfterFlush));
        }, reader -> {
            NumericDocValues other = Lucene.segmentReader(reader.leaves().get(0).reader()).getNumericDocValues("other");
            assertThat(other.advance(0), equalTo(0));
            assertThat(other.longValue(), equalTo(200L));
        });
    }

    /**
     * In production a failing collector is logged and skipped so that it never fails the flush or merge it observes. With
     * assertions enabled, as in tests, any failure is a bug and surfaces as an {@link AssertionError} carrying the original
     * exception, which is what these tests verify. The lenient production path is therefore not reachable from tests.
     */
    public void testFailingCollectorTripsAssertionOnFlush() throws IOException {
        IOException failure = new IOException("simulated");
        SegmentStatsCollector failing = new OrderRecording(new ArrayList<>(), true) {
            @Override
            public void onFlush(SegmentInfo segment, FieldInfo field, DocValuesProducer values) throws IOException {
                throw failure;
            }
        };
        try (Directory directory = newDirectory()) {
            SegmentInfo segment = newSegmentInfo(directory);
            var collectors = SegmentStatsCollectors.of(List.of(failing));
            AssertionError error = expectThrows(AssertionError.class, () -> collectors.onFlush(segment, null, null));
            assertThat(error.getCause(), sameInstance(failure));
        }
    }

    public void testFailingCollectorTripsAssertionOnMerge() throws IOException {
        IllegalStateException bug = new IllegalStateException("simulated bug at merge start");
        SegmentStatsCollector failingAtStart = new OrderRecording(new ArrayList<>(), true) {
            @Override
            public MergeCollector onMergeStart(SegmentInfo mergedSegment) {
                throw bug;
            }
        };
        IOException ioFailure = new IOException("simulated on source");
        SegmentStatsCollector failingOnSource = new OrderRecording(new ArrayList<>(), true) {
            @Override
            public MergeCollector onMergeStart(SegmentInfo mergedSegment) {
                return source -> { throw ioFailure; };
            }
        };
        UncheckedIOException uncheckedFailure = new UncheckedIOException(new IOException("simulated on source"));
        SegmentStatsCollector failingUncheckedOnSource = new OrderRecording(new ArrayList<>(), true) {
            @Override
            public MergeCollector onMergeStart(SegmentInfo mergedSegment) {
                return source -> { throw uncheckedFailure; };
            }
        };
        List<CodecReader> sources = new ArrayList<>();
        SegmentStatsCollector recording = new OrderRecording(new ArrayList<>(), true) {
            @Override
            public MergeCollector onMergeStart(SegmentInfo mergedSegment) {
                return sources::add;
            }
        };
        try (Directory directory = newDirectory()) {
            SegmentInfo merged = newSegmentInfo(directory);

            AssertionError error = expectThrows(
                AssertionError.class,
                () -> SegmentStatsCollectors.of(List.of(failingAtStart)).onMergeStart(merged)
            );
            assertThat(error.getCause(), sameInstance(bug));

            // the single collector shortcut is shielded as well: this is the production configuration
            error = expectThrows(
                AssertionError.class,
                () -> SegmentStatsCollectors.of(List.of(failingOnSource)).onMergeStart(merged).onSource(null)
            );
            assertThat(error.getCause(), sameInstance(ioFailure));
            error = expectThrows(
                AssertionError.class,
                () -> SegmentStatsCollectors.of(List.of(failingUncheckedOnSource)).onMergeStart(merged).onSource(null)
            );
            assertThat(error.getCause(), sameInstance(uncheckedFailure));

            // with several collectors the ones before the failing one still see the source
            var mergeCollector = SegmentStatsCollectors.of(List.of(recording, failingOnSource)).onMergeStart(merged);
            expectThrows(AssertionError.class, () -> mergeCollector.onSource(null));
            assertThat(sources.size(), equalTo(1));
        }
    }

    private static SegmentInfo newSegmentInfo(Directory directory) {
        return new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_test",
            0,
            false,
            false,
            Codec.getDefault(),
            Map.of(),
            StringHelper.randomId(),
            Map.of(),
            null
        );
    }

    public void testInapplicableCollectorIsNotInvoked() throws IOException {
        SumCollector collector = new SumCollector();
        withIndex(createMapperService(MAPPING_WITHOUT_TRACKED), List.of(collector), iw -> {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("other", 100));
            iw.addDocument(doc);
            iw.commit();
        }, reader -> {
            for (LeafReaderContext leaf : reader.leaves()) {
                assertThat(Lucene.segmentReader(leaf.reader()).getSegmentInfo().info.getAttribute(SUM_ATTRIBUTE), nullValue());
            }
        });
        assertThat(collector.seenFields, empty());
    }

    private static void withIndex(
        MapperService mapperService,
        List<SegmentStatsCollector> collectors,
        CheckedConsumer<RandomIndexWriter, IOException> builder,
        CheckedConsumer<DirectoryReader, IOException> test
    ) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer()).setCodec(
            new PerFieldMapperCodec(
                Lucene104Codec.Mode.BEST_SPEED,
                ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                mapperService,
                BigArrays.NON_RECYCLING_INSTANCE,
                null,
                SegmentStatsCollectors.of(collectors)
            )
        );
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc)) {
            builder.accept(iw);
            try (DirectoryReader reader = iw.getReader()) {
                test.accept(reader);
            }
        }
    }
}
