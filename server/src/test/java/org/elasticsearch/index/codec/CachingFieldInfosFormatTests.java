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
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.store.FieldInfoCachingDirectory;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link CachingFieldInfosFormat}. We exercise the format directly via
 * {@code SegmentInfos.readLatestCommit()} + per-segment {@code format.read()} rather than going through
 * {@code DirectoryReader.open}, because the latter would require our test codec to be SPI-registered.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class CachingFieldInfosFormatTests extends ESTestCase {

    public void testReadAlwaysReturnsFieldInfosWithUsages() throws Exception {
        try (Directory raw = newDirectory()) {
            indexSegments(raw, 3);
            CachingFieldInfosFormat format = newFormat();
            SegmentInfos sis = SegmentInfos.readLatestCommit(raw);
            for (SegmentCommitInfo sci : sis) {
                FieldInfos fis = format.read(raw, sci.info, "", IOContext.DEFAULT);
                assertThat(fis, Matchers.instanceOf(FieldInfosWithUsages.class));
            }
        }
    }

    public void testCachingDirectoryDedupesFieldInfoAcrossSegments() throws Exception {
        try (Directory raw = newDirectory()) {
            FieldInfoCachingDirectory wrapped = new FieldInfoCachingDirectory(raw);
            indexSegments(wrapped, 3);
            CachingFieldInfosFormat format = newFormat();
            // readLatestCommit passes the wrapped directory through, so sci.info.dir == wrapped, and the format's
            // unwrap(segmentInfo.dir) finds our cache.
            SegmentInfos sis = SegmentInfos.readLatestCommit(wrapped);
            assertThat("test requires multiple segments", sis.size(), Matchers.greaterThan(1));

            Map<String, FieldInfo> firstSeen = new HashMap<>();
            for (SegmentCommitInfo sci : sis) {
                FieldInfos fis = format.read(wrapped, sci.info, "", IOContext.DEFAULT);
                for (FieldInfo fi : fis) {
                    if (fi.isSoftDeletesField()) {
                        // The soft-deletes field's dvGen can vary by segment after updates; not part of this assertion.
                        continue;
                    }
                    FieldInfo prior = firstSeen.putIfAbsent(fi.getName(), fi);
                    if (prior != null) {
                        assertSame("FieldInfo for [" + fi.getName() + "] must be reference-equal across segments under caching", prior, fi);
                    }
                }
            }
        }
    }

    public void testNamesAndAttributesAreSharedWithoutACachingDirectory() throws Exception {
        try (Directory raw = newDirectory()) {
            indexSegments(raw, 2);
            CachingFieldInfosFormat format = newFormat();
            SegmentInfos sis = SegmentInfos.readLatestCommit(raw);
            SegmentCommitInfo sci = sis.iterator().next();
            FieldInfos first = format.read(raw, sci.info, "", IOContext.DEFAULT);
            FieldInfos second = format.read(raw, sci.info, "", IOContext.DEFAULT);
            assertThat(first, Matchers.instanceOf(FieldInfosWithUsages.class));
            assertThat(second, Matchers.instanceOf(FieldInfosWithUsages.class));
            assertEquals("field count must match", first.size(), second.size());
            for (FieldInfo fi1 : first) {
                FieldInfo fi2 = second.fieldInfo(fi1.getName());
                assertNotNull(fi2);
                // No directory to hold them, so the instances differ; the parts that can be shared node-wide still are.
                assertNotSame("no cache, so instances are not shared", fi1, fi2);
                assertSame("field name must be interned", fi1.getName(), fi2.getName());
                if (fi1.attributes().isEmpty() == false) {
                    assertSame("attribute map must be interned", fi1.attributes(), fi2.attributes());
                }
            }
        }
    }

    public void testAttributesMapPaidOncePerFieldPerDirectory() throws Exception {
        // Within one Directory, the per-Directory cache must produce exactly one canonical FieldInfo per field name across
        // all segments, and the attributes Map inside that FieldInfo must be the same instance for every read (identity).
        try (Directory raw = newDirectory()) {
            FieldInfoCachingDirectory wrapped = new FieldInfoCachingDirectory(raw);
            indexSegments(wrapped, 3);
            CachingFieldInfosFormat format = newFormat();
            SegmentInfos sis = SegmentInfos.readLatestCommit(wrapped);
            assertThat("test requires multiple segments", sis.size(), Matchers.greaterThan(1));

            java.util.Map<String, FieldInfo> firstSeenFi = new java.util.HashMap<>();
            for (SegmentCommitInfo sci : sis) {
                FieldInfos fis = format.read(wrapped, sci.info, "", IOContext.DEFAULT);
                for (FieldInfo fi : fis) {
                    FieldInfo prior = firstSeenFi.putIfAbsent(fi.getName(), fi);
                    if (prior != null) {
                        // Same FieldInfo instance across segments.
                        assertSame("FieldInfo for [" + fi.getName() + "] must be reference-equal across segments", prior, fi);
                        // ...which transitively means the attributes Map is the same instance too.
                        assertSame(
                            "attributes Map for [" + fi.getName() + "] must be reference-equal across segments",
                            prior.attributes(),
                            fi.attributes()
                        );
                    }
                }
            }
        }
    }

    public void testAttributesMapSharedAcrossDirectories() throws Exception {
        // Across two Directories (simulating two shards of the same data stream), the per-Directory FieldInfo cache produces
        // distinct FieldInfo instances per shard (field numbering is per-IndexWriter), but the attributes Map MUST still be
        // shared by reference because it is interned node-wide.
        try (Directory rawA = newDirectory(); Directory rawB = newDirectory()) {
            FieldInfoCachingDirectory wrappedA = new FieldInfoCachingDirectory(rawA);
            FieldInfoCachingDirectory wrappedB = new FieldInfoCachingDirectory(rawB);
            indexSegments(wrappedA, 1);
            indexSegments(wrappedB, 1);

            CachingFieldInfosFormat format = newFormat();
            FieldInfos fisA = format.read(wrappedA, SegmentInfos.readLatestCommit(wrappedA).iterator().next().info, "", IOContext.DEFAULT);
            FieldInfos fisB = format.read(wrappedB, SegmentInfos.readLatestCommit(wrappedB).iterator().next().info, "", IOContext.DEFAULT);

            for (FieldInfo fiA : fisA) {
                FieldInfo fiB = fisB.fieldInfo(fiA.getName());
                assertNotNull("field [" + fiA.getName() + "] should appear in both shards", fiB);
                // Attribute Map is node-wide canonical regardless of which Directory it was loaded under.
                assertSame(
                    "attributes Map for [" + fiA.getName() + "] must be reference-equal across shards via the node-wide intern",
                    fiA.attributes(),
                    fiB.attributes()
                );
            }
        }
    }

    public void testCachingDirectoryRereadOfSameSegmentReusesCanonical() throws Exception {
        try (Directory raw = newDirectory()) {
            FieldInfoCachingDirectory wrapped = new FieldInfoCachingDirectory(raw);
            indexSegments(wrapped, 1);
            CachingFieldInfosFormat format = newFormat();
            SegmentInfos sis = SegmentInfos.readLatestCommit(wrapped);
            SegmentCommitInfo sci = sis.iterator().next();

            FieldInfos fis1 = format.read(wrapped, sci.info, "", IOContext.DEFAULT);
            FieldInfos fis2 = format.read(wrapped, sci.info, "", IOContext.DEFAULT);

            for (FieldInfo fi1 : fis1) {
                FieldInfo fi2 = fis2.fieldInfo(fi1.getName());
                assertNotNull(fi2);
                assertSame("re-reading the same segment must reuse canonical FieldInfo for [" + fi1.getName() + "]", fi1, fi2);
            }
        }
    }

    /**
     * The field infos this format returns are rebuilt rather than passed through, so every property a segment recorded has to
     * survive the rebuild, with and without a directory to cache them in.
     */
    public void testRebuiltFieldInfosMatchWhatWasRead() throws Exception {
        for (boolean caching : new boolean[] { false, true }) {
            try (Directory raw = newDirectory()) {
                Directory dir = caching ? new FieldInfoCachingDirectory(raw) : raw;
                indexVariedFields(dir);
                SegmentCommitInfo sci = SegmentInfos.readLatestCommit(dir).iterator().next();
                FieldInfos expected = new Lucene104Codec().fieldInfosFormat().read(dir, sci.info, "", IOContext.DEFAULT);
                FieldInfos actual = newFormat().read(dir, sci.info, "", IOContext.DEFAULT);

                String message = "caching=" + caching;
                assertEquals(message, expected.size(), actual.size());
                for (FieldInfo want : expected) {
                    FieldInfo got = actual.fieldInfo(want.getName());
                    assertNotNull(message + " field " + want.getName(), got);
                    assertEquals(message, want.getName(), got.getName());
                    assertEquals(message, want.number, got.number);
                    assertEquals(message, want.hasTermVectors(), got.hasTermVectors());
                    assertEquals(message, want.omitsNorms(), got.omitsNorms());
                    assertEquals(message, want.hasPayloads(), got.hasPayloads());
                    assertEquals(message, want.getIndexOptions(), got.getIndexOptions());
                    assertEquals(message, want.getDocValuesType(), got.getDocValuesType());
                    assertEquals(message, want.docValuesSkipIndexType(), got.docValuesSkipIndexType());
                    assertEquals(message, want.getDocValuesGen(), got.getDocValuesGen());
                    assertEquals(message, want.attributes(), got.attributes());
                    assertEquals(message, want.getPointDimensionCount(), got.getPointDimensionCount());
                    assertEquals(message, want.getPointIndexDimensionCount(), got.getPointIndexDimensionCount());
                    assertEquals(message, want.getPointNumBytes(), got.getPointNumBytes());
                    assertEquals(message, want.getVectorDimension(), got.getVectorDimension());
                    assertEquals(message, want.getVectorEncoding(), got.getVectorEncoding());
                    assertEquals(message, want.getVectorSimilarityFunction(), got.getVectorSimilarityFunction());
                    assertEquals(message, want.isSoftDeletesField(), got.isSoftDeletesField());
                    assertEquals(message, want.isParentField(), got.isParentField());
                }
            }
        }
    }

    /** Exercises the properties a FieldInfo can carry, so that the equivalence check above has something to compare. */
    private static void indexVariedFields(Directory directory) throws Exception {
        FieldType withVectorsAndPayloads = new FieldType(TextField.TYPE_NOT_STORED);
        withVectorsAndPayloads.setStoreTermVectors(true);
        withVectorsAndPayloads.setStoreTermVectorPositions(true);
        withVectorsAndPayloads.setStoreTermVectorPayloads(true);
        withVectorsAndPayloads.setOmitNorms(true);
        withVectorsAndPayloads.freeze();

        IndexWriterConfig iwc = baseIwc();
        try (IndexWriter w = new IndexWriter(directory, iwc)) {
            for (int d = 0; d < 3; d++) {
                Document doc = new Document();
                doc.add(new StringField("id", "d" + d, Field.Store.NO));
                doc.add(new Field("text", "term" + d + " other", withVectorsAndPayloads));
                // A postings payload, which is what FieldInfo#hasPayloads reflects; term vector payloads are a different flag.
                Token token = new Token("payloaded", 0, 9);
                token.setPayload(new BytesRef(new byte[] { 1, 2, 3 }));
                doc.add(new Field("with_payload", new CannedTokenStream(token), withVectorsAndPayloads));
                doc.add(new IntPoint("point", d, d + 1));
                doc.add(new SortedNumericDocValuesField("sorted_numeric", d));
                doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("s" + d)));
                doc.add(new BinaryDocValuesField("binary", new BytesRef("b" + d)));
                doc.add(new KnnFloatVectorField("vector", new float[] { d, d + 1.0f, d + 2.0f }));
                w.addDocument(doc);
            }
            w.commit();
        }
    }

    private static CachingFieldInfosFormat newFormat() {
        return new CachingFieldInfosFormat(new Lucene104Codec().fieldInfosFormat());
    }

    private static IndexWriterConfig baseIwc() {
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        // Use the plain Lucene codec (SPI-registered) so SegmentInfos.readLatestCommit works without registering our wrapper.
        iwc.setCodec(new Lucene104Codec());
        iwc.setUseCompoundFile(false);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        return iwc;
    }

    private static void indexSegments(Directory directory, int segments) throws Exception {
        IndexWriterConfig iwc = baseIwc();
        try (IndexWriter w = new IndexWriter(directory, iwc)) {
            for (int seg = 0; seg < segments; seg++) {
                for (int d = 0; d < 3; d++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", seg + "-" + d, Field.Store.NO));
                    doc.add(new StringField("color", "red", Field.Store.NO));
                    doc.add(new NumericDocValuesField("counter", d));
                    w.addDocument(doc);
                }
                w.commit();
            }
        }
    }
}
