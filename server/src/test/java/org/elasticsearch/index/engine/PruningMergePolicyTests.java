/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.lucene104.Lucene104Codec;
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
import org.apache.lucene.util.NumericUtils;
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
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.codec.tsdb.ES93TSDBDefaultCompressionLucene103Codec;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdStoredFieldsReader;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.IdFieldMapper.standardIdField;
import static org.elasticsearch.index.mapper.IdFieldMapper.syntheticIdField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PruningMergePolicyTests extends ESTestCase {

    public void testPruneAll() throws IOException {
        for (boolean pruneIdField : List.of(true, false)) {
            for (boolean pruneSequenceNumber : List.of(true, false)) {
                for (boolean syntheticRecoverySource : List.of(true, false)) {
                    try (Directory dir = newDirectory()) {
                        IndexWriterConfig iwc = newIndexWriterConfig();
                        PruningMergePolicy mp = new PruningMergePolicy(
                            syntheticRecoverySource ? null : "extra_source",
                            syntheticRecoverySource ? "extra_source_size" : "extra_source",
                            pruneIdField,
                            pruneSequenceNumber,
                            () -> Queries.NO_DOCS_INSTANCE,
                            newLogMergePolicy(),
                            false
                        );
                        iwc.setMergePolicy(new ShuffleForcedMergePolicy(mp));
                        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                            final int nbDocs = randomIntBetween(10, 100);
                            for (int i = 0; i < nbDocs; i++) {
                                if (i > 0 && randomBoolean()) {
                                    writer.flush();
                                }
                                Document doc = new Document();
                                doc.add(new StoredField(IdFieldMapper.NAME, "_id"));
                                doc.add(new StoredField("source", "hello world"));
                                doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, i));
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
                                assertThat(leafReader.numDocs(), equalTo(nbDocs));

                                NumericDocValues extra_source = leafReader.getNumericDocValues(
                                    syntheticRecoverySource ? "extra_source_size" : "extra_source"
                                );
                                if (extra_source != null) {
                                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, extra_source.nextDoc());
                                }

                                NumericDocValues seqNoDV = leafReader.getNumericDocValues(SeqNoFieldMapper.NAME);
                                if (pruneSequenceNumber) {
                                    if (seqNoDV != null) {
                                        assertEquals(DocIdSetIterator.NO_MORE_DOCS, seqNoDV.nextDoc());
                                    }
                                } else {
                                    assertNotNull(seqNoDV);
                                    for (int i = 0; i < nbDocs; i++) {
                                        assertEquals(i, seqNoDV.nextDoc());
                                    }
                                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, seqNoDV.nextDoc());
                                }

                                if (leafReader instanceof CodecReader codecReader && reader instanceof StandardDirectoryReader sdr) {
                                    SegmentInfos segmentInfos = sdr.getSegmentInfos();
                                    MergePolicy.MergeSpecification forcedMerges = mp.findForcedDeletesMerges(
                                        segmentInfos,
                                        newMergeContext()
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
    }

    public void testPruneSome() throws IOException {
        for (boolean pruneIdField : List.of(true, false)) {
            for (boolean pruneSequenceNumber : List.of(true, false)) {
                for (boolean syntheticRecoverySource : List.of(true, false)) {
                    try (Directory dir = newDirectory()) {
                        IndexWriterConfig iwc = newIndexWriterConfig();
                        iwc.setMergePolicy(
                            new PruningMergePolicy(
                                syntheticRecoverySource ? null : "extra_source",
                                syntheticRecoverySource ? "extra_source_size" : "extra_source",
                                pruneIdField,
                                pruneSequenceNumber,
                                () -> new TermQuery(new Term("even", "true")),
                                iwc.getMergePolicy(),
                                false
                            )
                        );
                        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                            final int nbDocs = randomIntBetween(10, 100);
                            for (int i = 0; i < nbDocs; i++) {
                                if (i > 0 && randomBoolean()) {
                                    writer.flush();
                                }
                                Document doc = new Document();
                                doc.add(new StoredField(IdFieldMapper.NAME, "_id"));
                                doc.add(new StringField("even", Boolean.toString(i % 2 == 0), Field.Store.YES));
                                doc.add(new StoredField("source", "hello world"));
                                doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, i));
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
                                LeafReader leafReader = reader.leaves().get(0).reader();
                                assertThat(leafReader.numDocs(), equalTo(nbDocs));
                                String extraSourceDVName = syntheticRecoverySource ? "extra_source_size" : "extra_source";
                                NumericDocValues extra_source = leafReader.getNumericDocValues(extraSourceDVName);
                                assertNotNull(extra_source);
                                NumericDocValues seqNoDV = leafReader.getNumericDocValues(SeqNoFieldMapper.NAME);
                                assertNotNull(seqNoDV);
                                StoredFields storedFields = reader.storedFields();
                                for (int i = 0; i < reader.maxDoc(); i++) {
                                    Document document = storedFields.document(i);
                                    Set<String> collect = document.getFields()
                                        .stream()
                                        .map(IndexableField::name)
                                        .collect(Collectors.toSet());
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
                                        if (seqNoDV.docID() < i) {
                                            seqNoDV.advance(i);
                                        }
                                        assertThat(seqNoDV.docID(), equalTo(i));
                                    } else {
                                        assertThat(collect.contains(IdFieldMapper.NAME), equalTo(pruneIdField == false));
                                        assertFalse(collect.contains("extra_source"));
                                        if (extra_source.docID() < i) {
                                            extra_source.advance(i);
                                        }
                                        assertNotEquals(i, extra_source.docID());
                                        if (pruneSequenceNumber) {
                                            if (seqNoDV.docID() < i) {
                                                seqNoDV.advance(i);
                                            }
                                            assertThat(seqNoDV.docID(), not(equalTo(i)));
                                        } else {
                                            if (seqNoDV.docID() < i) {
                                                seqNoDV.advance(i);
                                            }
                                            assertThat(seqNoDV.docID(), equalTo(i));
                                        }
                                    }
                                }
                                if (extra_source.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, extra_source.nextDoc());
                                }
                                if (seqNoDV.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, seqNoDV.nextDoc());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void testPruneNone() throws IOException {
        for (boolean pruneSequenceNumber : List.of(true, false)) {
            for (boolean syntheticRecoverySource : List.of(true, false)) {
                try (Directory dir = newDirectory()) {
                    IndexWriterConfig iwc = newIndexWriterConfig();
                    iwc.setMergePolicy(
                        new PruningMergePolicy(
                            syntheticRecoverySource ? null : "extra_source",
                            syntheticRecoverySource ? "extra_source_size" : "extra_source",
                            false,
                            pruneSequenceNumber,
                            () -> Queries.ALL_DOCS_INSTANCE,
                            iwc.getMergePolicy(),
                            false
                        )
                    );
                    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                        final int nbDocs = randomIntBetween(10, 100);
                        for (int i = 0; i < nbDocs; i++) {
                            if (i > 0 && randomBoolean()) {
                                writer.flush();
                            }
                            Document doc = new Document();
                            doc.add(new StoredField("source", "hello world"));
                            doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, i));
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
                            LeafReader leafReader = reader.leaves().get(0).reader();
                            assertThat(leafReader.numDocs(), equalTo(nbDocs));
                            String extraSourceDVName = syntheticRecoverySource ? "extra_source_size" : "extra_source";
                            NumericDocValues extra_source = leafReader.getNumericDocValues(extraSourceDVName);
                            assertNotNull(extra_source);
                            NumericDocValues seqNoDV = leafReader.getNumericDocValues(SeqNoFieldMapper.NAME);
                            assertNotNull(seqNoDV);
                            StoredFields storedFields = reader.storedFields();
                            for (int i = 0; i < reader.maxDoc(); i++) {
                                Document document = storedFields.document(i);
                                Set<String> collect = document.getFields().stream().map(IndexableField::name).collect(Collectors.toSet());
                                assertTrue(collect.contains("source"));
                                assertThat(collect.contains("extra_source"), equalTo(syntheticRecoverySource == false));
                                assertEquals(i, extra_source.nextDoc());
                                assertEquals(i, seqNoDV.nextDoc());
                            }
                            assertEquals(DocIdSetIterator.NO_MORE_DOCS, extra_source.nextDoc());
                            assertEquals(DocIdSetIterator.NO_MORE_DOCS, seqNoDV.nextDoc());
                        }
                    }
                }
            }
        }
    }

    public void testTimeSeriesForceMergeToOneSegment() throws IOException {
        final var values = List.of(true, false);
        for (boolean useSyntheticId : values) {
            for (boolean pruneId : values) {
                for (boolean pruneSequenceNumber : values) {
                    for (boolean useSyntheticRecoverySource : values) {
                        runTimeSeriesForceMergeToOneSegment(useSyntheticId, pruneId, pruneSequenceNumber, useSyntheticRecoverySource);
                    }
                }
            }
        }
    }

    private void runTimeSeriesForceMergeToOneSegment(
        final boolean useSyntheticId,
        final boolean pruneId,
        final boolean pruneSequenceNumber,
        final boolean useSyntheticRecoverySource
    ) throws IOException {
        assumeTrue("Synthetic id requires a feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG || useSyntheticId == false);
        assumeTrue(
            "Sequence number pruning requires a feature flag",
            IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG || pruneSequenceNumber == false
        );
        try (var dir = newDirectory()) {
            dir.setCheckIndexOnClose(false);

            final int nbDocs = randomIntBetween(50, 150);
            final long minRetainedSeqNo = randomInt(nbDocs);
            final long primaryTerm = randomNonNegativeLong();

            final var indexSettings = timeSeriesIndexSettings(useSyntheticId, pruneSequenceNumber);
            final var seqNoIndexOptions = IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.get(indexSettings.getSettings());
            var iwc = createIndexWriterConfig(indexSettings);
            iwc.setMergePolicy(
                new PruningMergePolicy(
                    useSyntheticRecoverySource ? null : SourceFieldMapper.RECOVERY_SOURCE_NAME,
                    useSyntheticRecoverySource ? SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME : SourceFieldMapper.RECOVERY_SOURCE_NAME,
                    pruneId,
                    pruneSequenceNumber,
                    () -> SeqNoFieldMapper.rangeQueryForSeqNo(seqNoIndexOptions, minRetainedSeqNo, Long.MAX_VALUE),
                    iwc.getMergePolicy(),
                    useSyntheticId
                )
            );

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                final int routingHash = randomIntBetween(0, 10);
                final Instant now = Instant.now();

                for (int seqNo = 0; seqNo < nbDocs; seqNo++) {
                    if (seqNo > 0 && randomBoolean()) {
                        writer.flush();
                    }
                    var doc = newDocument(
                        useSyntheticId,
                        useSyntheticRecoverySource,
                        seqNoIndexOptions,
                        seqNo,
                        primaryTerm,
                        routingHash,
                        now.plusMillis(seqNo).toEpochMilli()
                    );
                    doc.add(new NumericDocValuesField("retained", seqNo >= minRetainedSeqNo ? 1 : 0));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();

                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals(1, reader.leaves().size());

                    final var leafReader = reader.leaves().getFirst().reader();
                    assertThat(leafReader.numDocs(), equalTo(nbDocs));

                    // Points for _seq_no
                    final var seqNoPoints = leafReader.getPointValues(SeqNoFieldMapper.NAME);
                    if (seqNoIndexOptions == SeqNoFieldMapper.SeqNoIndexOptions.POINTS_AND_DOC_VALUES) {
                        assertThat(seqNoPoints, notNullValue());
                        assertThat(seqNoPoints.size(), equalTo((long) nbDocs));
                        assertThat(NumericUtils.sortableBytesToLong(seqNoPoints.getMinPackedValue(), 0), equalTo(0L));
                        assertThat(NumericUtils.sortableBytesToLong(seqNoPoints.getMaxPackedValue(), 0), equalTo((long) nbDocs - 1L));
                    } else {
                        assertThat(seqNoPoints, nullValue());
                    }

                    // Doc values for _seq_no
                    final var seqNoDocValues = leafReader.getNumericDocValues(SeqNoFieldMapper.NAME);
                    assertThat(seqNoDocValues, notNullValue());

                    // Doc values for _primary_term
                    final var primaryTermDocValues = leafReader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
                    assertNotNull(primaryTermDocValues);

                    // Doc values for _recovery_source field
                    final var recoverySourceDocValues = leafReader.getNumericDocValues(
                        useSyntheticRecoverySource ? SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME : SourceFieldMapper.RECOVERY_SOURCE_NAME
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
                        if (useSyntheticId || pruneId == false || isRetained) {
                            expectedStoredFields.add(IdFieldMapper.NAME);
                        }
                        // The _recovery_source stored field value must be present when synthetic source is not used and the
                        // document is retained.
                        if (useSyntheticRecoverySource == false && isRetained) {
                            expectedStoredFields.add(SourceFieldMapper.RECOVERY_SOURCE_NAME);
                        }
                        assertThat(storedFieldsNames, equalTo(expectedStoredFields));

                        // primary term must be present for all docs
                        assertThat(primaryTermDocValues.nextDoc(), equalTo(docID));
                        assertThat(primaryTermDocValues.longValue(), equalTo(primaryTerm));

                        // if _seq_no pruning is disabled, _seq_no must be present for all docs
                        if (pruneSequenceNumber == false) {
                            assertThat(seqNoDocValues.nextDoc(), equalTo(docID));
                            assertThat(seqNoDocValues.longValue(), greaterThanOrEqualTo(0L));
                            assertThat(seqNoDocValues.longValue(), lessThan((long) nbDocs));
                        }

                        if (isRetained) {
                            // document is retained, it should have a _seq_no doc value
                            if (seqNoDocValues.docID() < docID) {
                                seqNoDocValues.advance(docID);
                            }
                            assertThat(seqNoDocValues.docID(), equalTo(docID));
                            assertThat(seqNoDocValues.longValue(), greaterThanOrEqualTo(minRetainedSeqNo));

                            // document is retained, it should have a recovery source doc value
                            recoverySourceDocValues.advance(docID);
                            assertThat(recoverySourceDocValues.docID(), equalTo(docID));
                            if (useSyntheticRecoverySource) {
                                assertThat(recoverySourceDocValues.longValue(), greaterThan(100L));
                            } else {
                                assertThat(recoverySourceDocValues.longValue(), equalTo(1L));
                            }
                        } else if (pruneSequenceNumber) {
                            // document is not retained and _seq_no pruning is enabled, _seq_no doc value must be absent
                            if (seqNoDocValues.docID() < docID) {
                                seqNoDocValues.advance(docID);
                            }
                            assertThat(seqNoDocValues.docID(), not(equalTo(docID)));
                        }
                    }
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, primaryTermDocValues.nextDoc());
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, retainedDocValues.nextDoc());

                    if (recoverySourceDocValues.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                        assertEquals(DocIdSetIterator.NO_MORE_DOCS, recoverySourceDocValues.nextDoc());
                    }
                    if (seqNoDocValues != null && seqNoDocValues.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                        assertEquals(DocIdSetIterator.NO_MORE_DOCS, seqNoDocValues.nextDoc());
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
                            if (useSyntheticRecoverySource == false && isRetained) {
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

    public void testWrapForMergeUnwrapsSyntheticIdStoredFieldsReader() throws IOException {
        boolean syntheticRecoverySource = randomBoolean();
        boolean pruneIdField = randomBoolean();
        boolean pruneSequenceNumber = randomBoolean();
        assumeTrue("Synthetic id requires a feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        assumeTrue(
            "Sequence number pruning requires a feature flag",
            IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG || (pruneSequenceNumber == false && pruneIdField == false)
        );
        String pruneStoredFieldName = syntheticRecoverySource ? null : SourceFieldMapper.RECOVERY_SOURCE_NAME;
        String pruneNumericDVFieldName = syntheticRecoverySource
            ? SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME
            : SourceFieldMapper.RECOVERY_SOURCE_NAME;

        try (var dir = newDirectory()) {
            dir.setCheckIndexOnClose(false);

            final int nbDocs = randomIntBetween(50, 150);
            final long primaryTerm = randomNonNegativeLong();
            final int routingHash = randomIntBetween(0, 10);

            final var indexSettings = timeSeriesIndexSettings(true, pruneSequenceNumber);
            final var seqNoIndexOptions = IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.get(indexSettings.getSettings());

            var iwc = createIndexWriterConfig(indexSettings);
            iwc.setMergePolicy(
                new PruningMergePolicy(
                    pruneStoredFieldName,
                    pruneNumericDVFieldName,
                    pruneIdField,
                    pruneSequenceNumber,
                    () -> Queries.ALL_DOCS_INSTANCE, // Use ALL_DOCS so that all recovery source DV are retained during merges
                    iwc.getMergePolicy(),
                    true
                )
            );

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                final Instant now = Instant.now();

                for (int seqNo = 0; seqNo < nbDocs; seqNo++) {
                    if (seqNo > 0 && randomBoolean()) {
                        writer.flush();
                    }
                    writer.addDocument(
                        newDocument(
                            true,
                            syntheticRecoverySource,
                            seqNoIndexOptions,
                            seqNo,
                            primaryTerm,
                            routingHash,
                            now.plusMillis(seqNo).toEpochMilli()
                        )
                    );
                }
                writer.forceMerge(1);
                writer.commit();

                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals(1, reader.leaves().size());
                    final var leafReader = reader.leaves().getFirst().reader();
                    assertThat(leafReader.numDocs(), equalTo(nbDocs));
                    assertTrue(leafReader instanceof CodecReader);
                    final var codecReader = (CodecReader) leafReader;

                    // The segment has recovery source DV for all docs since we used ALL_DOCS as the retain query.
                    // A new merge policy with ALL_DOCS hits the "keep all source" early-return (cardinality == maxDoc).
                    {
                        var mp = new PruningMergePolicy(
                            pruneStoredFieldName,
                            pruneNumericDVFieldName,
                            pruneIdField,
                            pruneSequenceNumber,
                            () -> Queries.ALL_DOCS_INSTANCE,
                            newLogMergePolicy(),
                            true
                        );
                        var forcedMerges = mp.findForcedDeletesMerges(Lucene.readSegmentInfos(reader.getIndexCommit()), newMergeContext());
                        var wrappedForMerge = forcedMerges.merges.get(0).wrapForMerge(codecReader);
                        // Should Lucene90CompressingStoredFieldsReader or newer
                        assertThat(wrappedForMerge.getFieldsReader(), not(instanceOf(TSDBStoredFieldsFormat.TSDBStoredFieldsReader.class)));
                        assertThat(wrappedForMerge.getFieldsReader(), not(instanceOf(TSDBSyntheticIdStoredFieldsReader.class)));
                    }

                }
            }

            // To test the "no recovery source DV" early-return, we need a segment where DV have already been
            // pruned. Re-open the writer with a NO_DOCS policy and force merge again to produce that.
            var iwc2 = createIndexWriterConfig(indexSettings);
            iwc2.setMergePolicy(
                new PruningMergePolicy(
                    pruneStoredFieldName,
                    pruneNumericDVFieldName,
                    pruneIdField,
                    pruneSequenceNumber,
                    () -> Queries.NO_DOCS_INSTANCE,
                    iwc2.getMergePolicy(),
                    true
                )
            );
            try (IndexWriter writer2 = new IndexWriter(dir, iwc2)) {
                writer2.addDocument(
                    newDocument(
                        true,
                        syntheticRecoverySource,
                        seqNoIndexOptions,
                        nbDocs + 1L,
                        primaryTerm,
                        routingHash,
                        Instant.now().plusMillis(200).toEpochMilli()
                    )
                );
                writer2.forceMerge(1);
                writer2.commit();
            }

            // The merged segment now has no recovery source DV (all pruned by NO_DOCS policy).
            // A new merge policy hits the "no recovery source DV" early-return.
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                final var leafReader = reader.leaves().getFirst().reader();
                assertThat(leafReader.numDocs(), equalTo(nbDocs + 1));
                assertTrue(leafReader instanceof CodecReader);
                final var codecReader = (CodecReader) leafReader;

                var mp = new PruningMergePolicy(
                    pruneStoredFieldName,
                    pruneNumericDVFieldName,
                    pruneIdField,
                    pruneSequenceNumber,
                    () -> Queries.ALL_DOCS_INSTANCE,
                    newLogMergePolicy(),
                    true
                );
                var forcedMerges = mp.findForcedDeletesMerges(Lucene.readSegmentInfos(reader.getIndexCommit()), newMergeContext());
                var wrappedForMerge = forcedMerges.merges.get(0).wrapForMerge(codecReader);
                assertThat(wrappedForMerge.getFieldsReader(), not(instanceOf(TSDBStoredFieldsFormat.TSDBStoredFieldsReader.class)));
                assertThat(wrappedForMerge.getFieldsReader(), not(instanceOf(TSDBSyntheticIdStoredFieldsReader.class)));
            }
        }
    }

    private static IndexSettings timeSeriesIndexSettings(boolean useSyntheticId, boolean pruneSequenceNumber) {
        IndexVersion minVersion;
        if (pruneSequenceNumber) {
            minVersion = IndexVersions.DISABLE_SEQUENCE_NUMBERS;
        } else if (useSyntheticId) {
            minVersion = IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_94;
        } else {
            minVersion = IndexVersions.TIME_SERIES_ID_HASHING;
        }
        var indexVersion = IndexVersionUtils.randomVersionBetween(minVersion, IndexVersion.current());
        return new IndexSettings(
            IndexMetadata.builder(randomIdentifier())
                .settings(
                    indexSettings(indexVersion, 1, 0).put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
                        .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                        .put(IndexSettings.SYNTHETIC_ID.getKey(), useSyntheticId)
                        .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), List.of("retained"))
                        .build()
                )
                .build(),
            Settings.EMPTY
        );
    }

    /**
     * Creates an {@link IndexWriterConfig} configured for TSDB indices with the appropriate index sort, codec, and no-op segment warmer.
     * The caller is responsible for setting the merge policy.
     */
    private static IndexWriterConfig createIndexWriterConfig(final IndexSettings indexSettings) throws IOException {
        final var mapperService = MapperTestUtils.newMapperService(
            new NamedXContentRegistry(CollectionUtils.concatLists(ClusterModule.getNamedXWriteables(), IndicesModule.getNamedXContents())),
            createTempFile(),
            indexSettings.getSettings(),
            indexSettings.getIndex().getName()
        );
        final var iwc = newIndexWriterConfig();
        if (IndexSettings.SYNTHETIC_ID.get(indexSettings.getSettings())) {
            iwc.setCodec(
                new ES93TSDBDefaultCompressionLucene103Codec(
                    new LegacyPerFieldMapperCodec(Lucene104Codec.Mode.BEST_SPEED, mapperService, BigArrays.NON_RECYCLING_INSTANCE, null)
                )
            );
        }
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
        iwc.setMergedSegmentWarmer(reader -> {});
        return iwc;
    }

    private static LuceneDocument newDocument(
        boolean useSyntheticId,
        boolean syntheticRecoverySource,
        SeqNoFieldMapper.SeqNoIndexOptions seqNoIndexOptions,
        long seqNo,
        long primaryTerm,
        int routingHash,
        long timestamp
    ) {
        LuceneDocument doc = new LuceneDocument();
        var hostname = "prod-" + randomInt(4);
        var metricField = randomFrom("uptime", "load");
        var metricValue = randomLongBetween(0, 1_000L);
        var tsid = new TsidBuilder().addStringDimension("hostname", hostname)
            .addStringDimension("metric.field", metricField)
            .addLongDimension("metric.value", metricValue)
            .buildTsid(IndexVersion.current());
        var routingHashBytes = Uid.encodeId(TimeSeriesRoutingHashFieldMapper.encode(routingHash));
        doc.add(SortedDocValuesField.indexedField(TimeSeriesIdFieldMapper.NAME, tsid));
        doc.add(SortedNumericDocValuesField.indexedField(DataStreamTimestampFieldMapper.DEFAULT_PATH, timestamp));
        doc.add(new SortedDocValuesField(TimeSeriesRoutingHashFieldMapper.NAME, routingHashBytes));
        if (useSyntheticId) {
            doc.add(syntheticIdField(TsidExtractingIdFieldMapper.createSyntheticId(tsid, timestamp, routingHash)));
        } else {
            doc.add(standardIdField(TsidExtractingIdFieldMapper.createId(routingHash, tsid, timestamp)));
        }
        if (syntheticRecoverySource) {
            doc.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME, randomIntBetween(101, 10000)));
        } else {
            doc.add(new StoredField(SourceFieldMapper.RECOVERY_SOURCE_NAME, "test"));
            doc.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_NAME, 1));
        }
        var seqNoFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID(seqNoIndexOptions);
        seqNoFields.set(seqNo, primaryTerm);
        seqNoFields.addFields(doc);
        return doc;
    }

    private static MergePolicy.MergeContext newMergeContext() {
        return new MergePolicy.MergeContext() {
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
        };
    }
}
