/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.NullInfoStream;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.codec.tsdb.ES93TSDBDefaultCompressionLucene103Codec;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdStoredFieldsReader;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.IdFieldMapper.standardIdField;
import static org.elasticsearch.index.mapper.IdFieldMapper.syntheticIdField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class RecoverySourcePruneMergePolicyTests extends ESTestCase {

    public void testPruneAll() throws IOException {
        for (boolean pruneIdField : List.of(true, false)) {
            for (boolean syntheticRecoverySource : List.of(true, false)) {
                try (Directory dir = newDirectory()) {
                    IndexWriterConfig iwc = newIndexWriterConfig();
                    RecoverySourcePruneMergePolicy mp = new RecoverySourcePruneMergePolicy(
                        syntheticRecoverySource ? null : "extra_source",
                        syntheticRecoverySource ? "extra_source_size" : "extra_source",
                        pruneIdField,
                        () -> Queries.NO_DOCS_INSTANCE,
                        newLogMergePolicy(),
                        false
                    );
                    iwc.setMergePolicy(new ShuffleForcedMergePolicy(mp));
                    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                        for (int i = 0; i < 20; i++) {
                            if (i > 0 && randomBoolean()) {
                                writer.flush();
                            }
                            Document doc = new Document();
                            doc.add(new StoredField(IdFieldMapper.NAME, "_id"));
                            doc.add(new StoredField("source", "hello world"));
                            if (syntheticRecoverySource) {
                                doc.add(new NumericDocValuesField("extra_source_size", randomIntBetween(10, 10000)));
                            } else {
                                doc.add(new StoredField("extra_source", "hello world"));
                                doc.add(new NumericDocValuesField("extra_source", 1));
                            }
                            writer.addDocument(doc);
                        }
                        writer.forceMerge(1);
                        writer.commit();
                        try (DirectoryReader reader = DirectoryReader.open(writer)) {
                            StoredFields storedFields = reader.storedFields();
                            for (int i = 0; i < reader.maxDoc(); i++) {
                                Document document = storedFields.document(i);
                                if (pruneIdField) {
                                    assertEquals(1, document.getFields().size());
                                    assertEquals("source", document.getFields().get(0).name());
                                } else {
                                    assertEquals(2, document.getFields().size());
                                    assertEquals(IdFieldMapper.NAME, document.getFields().get(0).name());
                                    assertEquals("source", document.getFields().get(1).name());
                                }
                            }

                            assertEquals(1, reader.leaves().size());
                            LeafReader leafReader = reader.leaves().get(0).reader();

                            NumericDocValues extra_source = leafReader.getNumericDocValues(
                                syntheticRecoverySource ? "extra_source_size" : "extra_source"
                            );
                            if (extra_source != null) {
                                assertEquals(DocIdSetIterator.NO_MORE_DOCS, extra_source.nextDoc());
                            }
                            if (leafReader instanceof CodecReader codecReader && reader instanceof StandardDirectoryReader sdr) {
                                SegmentInfos segmentInfos = sdr.getSegmentInfos();
                                MergePolicy.MergeSpecification forcedMerges = mp.findForcedDeletesMerges(
                                    segmentInfos,
                                    new MergePolicy.MergeContext() {
                                        @Override
                                        public int numDeletesToMerge(SegmentCommitInfo info) {
                                            return info.info.maxDoc() - 1;
                                        }

                                        @Override
                                        public int numDeletedDocs(SegmentCommitInfo info) {
                                            return info.info.maxDoc() - 1;
                                        }

                                        @Override
                                        public InfoStream getInfoStream() {
                                            return new NullInfoStream();
                                        }

                                        @Override
                                        public Set<SegmentCommitInfo> getMergingSegments() {
                                            return Collections.emptySet();
                                        }
                                    }
                                );
                                // don't wrap if there is nothing to do
                                assertSame(codecReader, forcedMerges.merges.get(0).wrapForMerge(codecReader));
                            }
                        }
                    }
                }
            }
        }
    }

    public void testPruneSome() throws IOException {
        for (boolean pruneIdField : List.of(true, false)) {
            for (boolean syntheticRecoverySource : List.of(true, false)) {
                try (Directory dir = newDirectory()) {
                    IndexWriterConfig iwc = newIndexWriterConfig();
                    iwc.setMergePolicy(
                        new RecoverySourcePruneMergePolicy(
                            syntheticRecoverySource ? null : "extra_source",
                            syntheticRecoverySource ? "extra_source_size" : "extra_source",
                            pruneIdField,
                            () -> new TermQuery(new Term("even", "true")),
                            iwc.getMergePolicy(),
                            false
                        )
                    );
                    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                        for (int i = 0; i < 20; i++) {
                            if (i > 0 && randomBoolean()) {
                                writer.flush();
                            }
                            Document doc = new Document();
                            doc.add(new StoredField(IdFieldMapper.NAME, "_id"));
                            doc.add(new StringField("even", Boolean.toString(i % 2 == 0), Field.Store.YES));
                            doc.add(new StoredField("source", "hello world"));
                            if (syntheticRecoverySource) {
                                doc.add(new NumericDocValuesField("extra_source_size", randomIntBetween(10, 10000)));
                            } else {
                                doc.add(new StoredField("extra_source", "hello world"));
                                doc.add(new NumericDocValuesField("extra_source", 1));
                            }
                            writer.addDocument(doc);
                        }
                        writer.forceMerge(1);
                        writer.commit();
                        try (DirectoryReader reader = DirectoryReader.open(writer)) {
                            assertEquals(1, reader.leaves().size());
                            String extraSourceDVName = syntheticRecoverySource ? "extra_source_size" : "extra_source";
                            NumericDocValues extra_source = reader.leaves().get(0).reader().getNumericDocValues(extraSourceDVName);
                            assertNotNull(extra_source);
                            StoredFields storedFields = reader.storedFields();
                            for (int i = 0; i < reader.maxDoc(); i++) {
                                Document document = storedFields.document(i);
                                Set<String> collect = document.getFields().stream().map(IndexableField::name).collect(Collectors.toSet());
                                assertTrue(collect.contains("source"));
                                assertTrue(collect.contains("even"));
                                boolean isEven = Booleans.parseBoolean(document.getField("even").stringValue());
                                if (isEven) {
                                    assertTrue(collect.contains(IdFieldMapper.NAME));
                                    assertThat(collect.contains("extra_source"), equalTo(syntheticRecoverySource == false));
                                    if (extra_source.docID() < i) {
                                        extra_source.advance(i);
                                    }
                                    assertEquals(i, extra_source.docID());
                                    if (syntheticRecoverySource) {
                                        assertThat(extra_source.longValue(), greaterThanOrEqualTo(10L));
                                    } else {
                                        assertThat(extra_source.longValue(), equalTo(1L));
                                    }
                                } else {
                                    assertThat(collect.contains(IdFieldMapper.NAME), equalTo(pruneIdField == false));
                                    assertFalse(collect.contains("extra_source"));
                                    if (extra_source.docID() < i) {
                                        extra_source.advance(i);
                                    }
                                    assertNotEquals(i, extra_source.docID());
                                }
                            }
                            if (extra_source.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                                assertEquals(DocIdSetIterator.NO_MORE_DOCS, extra_source.nextDoc());
                            }
                        }
                    }
                }
            }
        }
    }

    public void testPruneNone() throws IOException {
        for (boolean syntheticRecoverySource : List.of(true, false)) {
            try (Directory dir = newDirectory()) {
                IndexWriterConfig iwc = newIndexWriterConfig();
                iwc.setMergePolicy(
                    new RecoverySourcePruneMergePolicy(
                        syntheticRecoverySource ? null : "extra_source",
                        syntheticRecoverySource ? "extra_source_size" : "extra_source",
                        false,
                        () -> Queries.ALL_DOCS_INSTANCE,
                        iwc.getMergePolicy(),
                        false
                    )
                );
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    for (int i = 0; i < 20; i++) {
                        if (i > 0 && randomBoolean()) {
                            writer.flush();
                        }
                        Document doc = new Document();
                        doc.add(new StoredField("source", "hello world"));
                        if (syntheticRecoverySource) {
                            doc.add(new NumericDocValuesField("extra_source_size", randomIntBetween(10, 10000)));
                        } else {
                            doc.add(new StoredField("extra_source", "hello world"));
                            doc.add(new NumericDocValuesField("extra_source", 1));
                        }
                        writer.addDocument(doc);
                    }
                    writer.forceMerge(1);
                    writer.commit();
                    try (DirectoryReader reader = DirectoryReader.open(writer)) {
                        assertEquals(1, reader.leaves().size());
                        String extraSourceDVName = syntheticRecoverySource ? "extra_source_size" : "extra_source";
                        NumericDocValues extra_source = reader.leaves().get(0).reader().getNumericDocValues(extraSourceDVName);
                        assertNotNull(extra_source);
                        StoredFields storedFields = reader.storedFields();
                        for (int i = 0; i < reader.maxDoc(); i++) {
                            Document document = storedFields.document(i);
                            Set<String> collect = document.getFields().stream().map(IndexableField::name).collect(Collectors.toSet());
                            assertTrue(collect.contains("source"));
                            assertThat(collect.contains("extra_source"), equalTo(syntheticRecoverySource == false));
                            assertEquals(i, extra_source.nextDoc());
                        }
                        assertEquals(DocIdSetIterator.NO_MORE_DOCS, extra_source.nextDoc());
                    }
                }
            }
        }
    }

    public void testSkipSyntheticIdStoredFieldValuesDuringMerges() throws IOException {
        runTestSkipSyntheticIdStoredFieldValuesDuringMerges(true, true, true);
    }

    public void testSkipSyntheticIdStoredFieldValuesDuringMergesNoSyntheticRecoverySource() throws IOException {
        runTestSkipSyntheticIdStoredFieldValuesDuringMerges(true, true, false);
    }

    public void testSkipSyntheticIdStoredFieldValuesDuringMergesNoIdPruning() throws Exception {
        runTestSkipSyntheticIdStoredFieldValuesDuringMerges(true, false, true);
    }

    public void testSkipSyntheticIdStoredFieldValuesDuringMergesNoIdPruningNoSyntheticRecoverySource() throws Exception {
        runTestSkipSyntheticIdStoredFieldValuesDuringMerges(true, false, false);
    }

    public void testSkipSyntheticIdStoredFieldValuesDuringMergesNoSyntheticId() throws Exception {
        runTestSkipSyntheticIdStoredFieldValuesDuringMerges(false, true, true);
    }

    public void testSkipSyntheticIdStoredFieldValuesDuringMergesNoSyntheticIdNoSyntheticRecoverySource() throws Exception {
        runTestSkipSyntheticIdStoredFieldValuesDuringMerges(false, true, false);
    }

    public void testSkipSyntheticIdStoredFieldValuesDuringMergesNoSyntheticIdNoIdPruning() throws Exception {
        runTestSkipSyntheticIdStoredFieldValuesDuringMerges(false, false, true);
    }

    public void testSkipSyntheticIdStoredFieldValuesDuringMergesNoSyntheticIdNoIdPruningNoSyntheticRecoverySource() throws Exception {
        runTestSkipSyntheticIdStoredFieldValuesDuringMerges(false, false, true);
    }

    private void runTestSkipSyntheticIdStoredFieldValuesDuringMerges(
        final boolean useSyntheticId,
        final boolean pruneIdField,
        final boolean syntheticRecoverySource
    ) throws IOException {
        try (var dir = newDirectory()) {
            // Checking the index on close requires to support Terms#getMin()/getMax() methods on invalid (or incomplete) terms,
            // something that is not supported in TSDBSyntheticIdFieldsProducer today.
            //
            // TODO would be nice to enable check-index-on-close
            dir.setCheckIndexOnClose(false);

            IndexWriterConfig iwc = newIndexWriterConfig();
            var sortOnTsId = new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING);
            sortOnTsId.setMissingValue(SortField.STRING_LAST);
            var sortOnTimestamp = new SortedNumericSortField(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                SortField.Type.LONG,
                true,
                SortedNumericSelector.Type.MAX
            );
            sortOnTimestamp.setMissingValue(Long.MIN_VALUE);
            iwc.setIndexSort(new Sort(sortOnTsId, sortOnTimestamp));
            iwc.setMergePolicy(
                new RecoverySourcePruneMergePolicy(
                    syntheticRecoverySource ? null : SourceFieldMapper.RECOVERY_SOURCE_NAME,
                    syntheticRecoverySource ? SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME : SourceFieldMapper.RECOVERY_SOURCE_NAME,
                    pruneIdField,
                    () -> NumericDocValuesField.newSlowExactQuery("retained", 1),
                    iwc.getMergePolicy(),
                    useSyntheticId
                )
            );
            final var indexSettings = new IndexSettings(
                IndexMetadata.builder(randomIdentifier())
                    .settings(
                        indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                            // SYNTHETIC_ID here is only used to have the correct postings format selected by the codec
                            .put(IndexSettings.SYNTHETIC_ID.getKey(), useSyntheticId)
                            // Dimensions are not used, only added to make settings validation pass
                            .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), List.of("retained"))
                            .build()
                    )
                    .build(),
                Settings.EMPTY
            );
            final var mapperService = MapperTestUtils.newMapperService(
                new NamedXContentRegistry(
                    CollectionUtils.concatLists(ClusterModule.getNamedXWriteables(), IndicesModule.getNamedXContents())
                ),
                createTempFile(),
                indexSettings.getSettings(),
                indexSettings.getIndex().getName()
            );
            if (useSyntheticId) {
                iwc.setCodec(
                    new ES93TSDBDefaultCompressionLucene103Codec(
                        new LegacyPerFieldMapperCodec(Lucene103Codec.Mode.BEST_SPEED, mapperService, BigArrays.NON_RECYCLING_INSTANCE, null)
                    )
                );
            }

            // Disable merged segment warmer, otherwise it reads synthetic ids from a merge thread and triggers the assertion
            iwc.setMergedSegmentWarmer(reader -> {});

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                final int routingHash = randomIntBetween(0, 10);
                final var routingHashBytes = Uid.encodeId(TimeSeriesRoutingHashFieldMapper.encode(routingHash));
                final Instant now = Instant.now();

                for (int i = 0; i < 100; i++) {
                    if (i > 0 && randomBoolean()) {
                        writer.flush();
                    }

                    Document doc = new Document();

                    var hostname = "prod-" + randomInt(4);
                    var metricField = randomFrom("uptime", "load");
                    var metricValue = randomLongBetween(0, 1_000L);
                    long timestamp = now.plusMillis(i).toEpochMilli();

                    // Compute the _tsid
                    var tsid = new TsidBuilder().addStringDimension("hostname", hostname)
                        .addStringDimension("metric.field", metricField)
                        .addLongDimension("metric.value", metricValue)
                        .buildTsid();

                    // Add time-series document fields
                    doc.add(SortedDocValuesField.indexedField(TimeSeriesIdFieldMapper.NAME, tsid));
                    doc.add(SortedNumericDocValuesField.indexedField(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
                    doc.add(new SortedDocValuesField(TimeSeriesRoutingHashFieldMapper.NAME, routingHashBytes));
                    if (useSyntheticId) {
                        doc.add(syntheticIdField(TsidExtractingIdFieldMapper.createSyntheticId(tsid, timestamp, routingHash)));
                    } else {
                        doc.add(standardIdField(TsidExtractingIdFieldMapper.createId(routingHash, tsid, timestamp)));
                    }

                    // Time-series documents always have a synthetic source field, so it's not stored at all in _source but it
                    // may be retained and stored in _recovery_source until it is not necessary anymore for recoveries.
                    var source = String.format(Locale.ROOT, """
                        {"@timestamp": "%s", "hostname": "%s", "metric": {"field": "%s", "value": %d}}
                        """, now.plusMillis(i), hostname, metricField, metricValue);
                    if (syntheticRecoverySource) {
                        doc.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME, source.length()));
                    } else {
                        doc.add(new StoredField(SourceFieldMapper.RECOVERY_SOURCE_NAME, source));
                        doc.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_NAME, 1 /* default value */));
                    }
                    // Indicate that the doc is retained in the test
                    doc.add(new NumericDocValuesField("retained", randomInt(1)));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();

                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals(1, reader.leaves().size());

                    final var leafReader = reader.leaves().getFirst().reader();
                    // Doc values for _recovery_source field
                    final var recoverySourceDocValues = leafReader.getNumericDocValues(
                        syntheticRecoverySource ? SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME : SourceFieldMapper.RECOVERY_SOURCE_NAME
                    );
                    assertNotNull(recoverySourceDocValues);
                    // Doc values for "retained" field"
                    var retainedDocValues = leafReader.getNumericDocValues("retained");
                    assertNotNull(retainedDocValues);

                    // Stored fields reader
                    final var storedFields = reader.storedFields();

                    for (int docID = 0; docID < reader.maxDoc(); docID++) {
                        Document document = storedFields.document(docID);

                        var storedFieldsNames = document.getFields()
                            .stream()
                            .map(IndexableField::name)
                            .collect(Collectors.toUnmodifiableSet());

                        retainedDocValues.advance(docID);
                        boolean isRetained = retainedDocValues.longValue() == 1;

                        var expectedStoredFields = new HashSet<String>();
                        // The _id stored field value must be present if:
                        // - synthetic id is used, in which case the value is materialized;
                        // - the id is never pruned;
                        // - the id is pruned but retained for the document.
                        if (useSyntheticId || pruneIdField == false || isRetained) {
                            expectedStoredFields.add(IdFieldMapper.NAME);
                        }
                        // The _recovery_source stored field value must be present when synthetic source is not used and the
                        // document is retained.
                        if (syntheticRecoverySource == false && isRetained) {
                            expectedStoredFields.add(SourceFieldMapper.RECOVERY_SOURCE_NAME);
                        }
                        assertThat(storedFieldsNames, equalTo(expectedStoredFields));

                        // If the document is retained, we can check the recovery source doc values
                        if (isRetained) {
                            recoverySourceDocValues.advance(docID);
                            if (syntheticRecoverySource) {
                                assertThat(recoverySourceDocValues.longValue(), greaterThan(100L));
                            } else {
                                assertThat(recoverySourceDocValues.longValue(), equalTo(1L));
                            }
                        }
                    }
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, retainedDocValues.nextDoc());

                    if (recoverySourceDocValues.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                        assertEquals(DocIdSetIterator.NO_MORE_DOCS, recoverySourceDocValues.nextDoc());
                    }

                    // Check that _id are not stored at all when synthetic id is used
                    if (useSyntheticId) {
                        // Get the underlying stored fields reader
                        var tsdbStoredFieldsReader = asInstanceOf(
                            TSDBStoredFieldsFormat.TSDBStoredFieldsReader.class,
                            Lucene.segmentReader(leafReader).getFieldsReader()
                        );
                        // Extract the real (ie, non-synthetic id) stored field reader
                        final var defaultStoredFields = tsdbStoredFieldsReader.getStoredFieldsReader();
                        assertThat(defaultStoredFields, not(instanceOf(TSDBSyntheticIdStoredFieldsReader.class)));

                        // Re-create doc values
                        retainedDocValues = leafReader.getNumericDocValues("retained");
                        assertNotNull(retainedDocValues);

                        for (int docID = 0; docID < reader.maxDoc(); docID++) {
                            Document document = defaultStoredFields.document(docID);

                            var storedFieldsNames = document.getFields()
                                .stream()
                                .map(IndexableField::name)
                                .collect(Collectors.toUnmodifiableSet());

                            retainedDocValues.advance(docID);
                            boolean isRetained = retainedDocValues.longValue() == 1;

                            // When synthetic id is used, the _id field must never be stored
                            Set<String> expectedStoredFields = Set.of();

                            // The _recovery_source stored field value must be present when synthetic source is not used and the
                            // document is retained.
                            if (syntheticRecoverySource == false && isRetained) {
                                expectedStoredFields = Set.of(SourceFieldMapper.RECOVERY_SOURCE_NAME);
                            }
                            assertThat(storedFieldsNames, equalTo(expectedStoredFields));
                        }

                        assertEquals(DocIdSetIterator.NO_MORE_DOCS, retainedDocValues.nextDoc());
                    }
                }
            }
        }
    }
}
