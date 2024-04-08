/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.NullInfoStream;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class RecoverySourcePruneMergePolicyTests extends ESTestCase {

    public void testPruneAll() throws IOException {
        try (Directory dir = newDirectory()) {
            boolean pruneIdField = randomBoolean();
            IndexWriterConfig iwc = newIndexWriterConfig();
            RecoverySourcePruneMergePolicy mp = new RecoverySourcePruneMergePolicy(
                "extra_source",
                pruneIdField,
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
                    doc.add(new StoredField("extra_source", "hello world"));
                    doc.add(new NumericDocValuesField("extra_source", 1));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        Document document = reader.document(i);
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
                    NumericDocValues extra_source = leafReader.getNumericDocValues("extra_source");
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

    public void testPruneSome() throws IOException {
        try (Directory dir = newDirectory()) {
            boolean pruneIdField = randomBoolean();
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setMergePolicy(
                new RecoverySourcePruneMergePolicy(
                    "extra_source",
                    pruneIdField,
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
                    doc.add(new StoredField("extra_source", "hello world"));
                    doc.add(new NumericDocValuesField("extra_source", 1));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertEquals(1, reader.leaves().size());
                    NumericDocValues extra_source = reader.leaves().get(0).reader().getNumericDocValues("extra_source");
                    assertNotNull(extra_source);
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        Document document = reader.document(i);
                        Set<String> collect = document.getFields().stream().map(IndexableField::name).collect(Collectors.toSet());
                        assertTrue(collect.contains("source"));
                        assertTrue(collect.contains("even"));
                        if (collect.size() == 4) {
                            assertTrue(collect.contains("extra_source"));
                            assertTrue(collect.contains(IdFieldMapper.NAME));
                            assertEquals("true", document.getField("even").stringValue());
                            assertEquals(i, extra_source.nextDoc());
                        } else {
                            assertEquals(pruneIdField ? 2 : 3, document.getFields().size());
                        }
                    }
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, extra_source.nextDoc());
                }
            }
        }
    }

    public void testPruneNone() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setMergePolicy(new RecoverySourcePruneMergePolicy("extra_source", false, MatchAllDocsQuery::new, iwc.getMergePolicy()));
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 20; i++) {
                    if (i > 0 && randomBoolean()) {
                        writer.flush();
                    }
                    Document doc = new Document();
                    doc.add(new StoredField("source", "hello world"));
                    doc.add(new StoredField("extra_source", "hello world"));
                    doc.add(new NumericDocValuesField("extra_source", 1));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertEquals(1, reader.leaves().size());
                    NumericDocValues extra_source = reader.leaves().get(0).reader().getNumericDocValues("extra_source");
                    assertNotNull(extra_source);
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        Document document = reader.document(i);
                        Set<String> collect = document.getFields().stream().map(IndexableField::name).collect(Collectors.toSet());
                        assertTrue(collect.contains("source"));
                        assertTrue(collect.contains("extra_source"));
                        assertEquals(i, extra_source.nextDoc());
                    }
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, extra_source.nextDoc());
                }
            }
        }
    }
}
