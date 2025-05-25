/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.NullInfoStream;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
                        true,
                        MatchNoDocsQuery::new,
                        newLogMergePolicy()
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
                            doc.add(new LongPoint(SeqNoFieldMapper.NAME, 3L));
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

                            var pointValues = leafReader.getPointValues(SeqNoFieldMapper.NAME);
                            assertThat(pointValues, nullValue());

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
                            false,
                            () -> new TermQuery(new Term("even", "true")),
                            iwc.getMergePolicy()
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
                            doc.add(new LongPoint(SeqNoFieldMapper.NAME, 3L));
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
                                boolean isEven = Boolean.parseBoolean(document.getField("even").stringValue());
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

                            // Can only prune points for entire segment:
                            var pointValues = reader.leaves().get(0).reader().getPointValues(SeqNoFieldMapper.NAME);
                            assertThat(pointValues, notNullValue());
                            assertThat(pointValues.getDocCount(), equalTo(20));
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
                        false,
                        MatchAllDocsQuery::new,
                        iwc.getMergePolicy()
                    )
                );
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    for (int i = 0; i < 20; i++) {
                        if (i > 0 && randomBoolean()) {
                            writer.flush();
                        }
                        Document doc = new Document();
                        doc.add(new StoredField("source", "hello world"));
                        doc.add(new LongPoint(SeqNoFieldMapper.NAME, 3L));
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

                        var pointValues = reader.leaves().get(0).reader().getPointValues(SeqNoFieldMapper.NAME);
                        assertThat(pointValues, notNullValue());
                        assertThat(pointValues.getDocCount(), equalTo(20));
                    }
                }
            }
        }
    }
}
