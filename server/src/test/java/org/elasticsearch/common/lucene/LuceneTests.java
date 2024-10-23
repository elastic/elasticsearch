/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.lucene;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class LuceneTests extends ESTestCase {
    private static final NamedWriteableRegistry EMPTY_REGISTRY = new NamedWriteableRegistry(Collections.emptyList());

    public void testCleanIndex() throws IOException {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(2);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();

        doc = new Document();
        doc.add(new TextField("id", "2", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("id", "3", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);

        writer.commit();
        doc = new Document();
        doc.add(new TextField("id", "4", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);

        writer.deleteDocuments(new Term("id", "2"));
        writer.commit();
        try (DirectoryReader open = DirectoryReader.open(writer)) {
            assertEquals(3, open.numDocs());
            assertEquals(1, open.numDeletedDocs());
            assertEquals(4, open.maxDoc());
        }
        writer.close();
        if (random().nextBoolean()) {
            for (String file : dir.listAll()) {
                if (file.startsWith("_1")) {
                    // delete a random file
                    dir.deleteFile(file);
                    break;
                }
            }
        }
        Lucene.cleanLuceneIndex(dir);
        if (dir.listAll().length > 0) {
            for (String file : dir.listAll()) {
                if (file.startsWith("extra") == false) {
                    assertEquals(file, "write.lock");
                }
            }
        }
        dir.close();
    }

    public void testPruneUnreferencedFiles() throws IOException {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(2);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();

        doc = new Document();
        doc.add(new TextField("id", "2", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("id", "3", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);

        writer.commit();
        SegmentInfos segmentCommitInfos = Lucene.readSegmentInfos(dir);

        doc = new Document();
        doc.add(new TextField("id", "4", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);

        writer.deleteDocuments(new Term("id", "2"));
        writer.commit();
        DirectoryReader open = DirectoryReader.open(writer);
        assertEquals(3, open.numDocs());
        assertEquals(1, open.numDeletedDocs());
        assertEquals(4, open.maxDoc());
        open.close();
        writer.close();
        SegmentInfos si = Lucene.pruneUnreferencedFiles(segmentCommitInfos.getSegmentsFileName(), dir);
        assertEquals(si.getSegmentsFileName(), segmentCommitInfos.getSegmentsFileName());
        open = DirectoryReader.open(dir);
        assertEquals(3, open.numDocs());
        assertEquals(0, open.numDeletedDocs());
        assertEquals(3, open.maxDoc());

        IndexSearcher s = newSearcher(open);
        assertEquals(s.search(new TermQuery(new Term("id", "1")), 1).totalHits.value(), 1);
        assertEquals(s.search(new TermQuery(new Term("id", "2")), 1).totalHits.value(), 1);
        assertEquals(s.search(new TermQuery(new Term("id", "3")), 1).totalHits.value(), 1);
        assertEquals(s.search(new TermQuery(new Term("id", "4")), 1).totalHits.value(), 0);

        for (String file : dir.listAll()) {
            assertFalse("unexpected file: " + file, file.equals("segments_3") || file.startsWith("_2"));
        }
        open.close();
        dir.close();

    }

    public void testFiles() throws IOException {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(2);
        iwc.setUseCompoundFile(true);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        Set<String> files = new HashSet<>();
        for (String f : Lucene.files(Lucene.readSegmentInfos(dir))) {
            files.add(f);
        }
        final boolean simpleTextCFS = files.contains("_0.scf");
        assertTrue(files.toString(), files.contains("segments_1"));
        if (simpleTextCFS) {
            assertFalse(files.toString(), files.contains("_0.cfs"));
            assertFalse(files.toString(), files.contains("_0.cfe"));
        } else {
            assertTrue(files.toString(), files.contains("_0.cfs"));
            assertTrue(files.toString(), files.contains("_0.cfe"));
        }
        assertTrue(files.toString(), files.contains("_0.si"));

        doc = new Document();
        doc.add(new TextField("id", "2", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("id", "3", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();

        files.clear();
        for (String f : Lucene.files(Lucene.readSegmentInfos(dir))) {
            files.add(f);
        }
        assertFalse(files.toString(), files.contains("segments_1"));
        assertTrue(files.toString(), files.contains("segments_2"));
        if (simpleTextCFS) {
            assertFalse(files.toString(), files.contains("_0.cfs"));
            assertFalse(files.toString(), files.contains("_0.cfe"));
        } else {
            assertTrue(files.toString(), files.contains("_0.cfs"));
            assertTrue(files.toString(), files.contains("_0.cfe"));
        }
        assertTrue(files.toString(), files.contains("_0.si"));

        if (simpleTextCFS) {
            assertFalse(files.toString(), files.contains("_1.cfs"));
            assertFalse(files.toString(), files.contains("_1.cfe"));
        } else {
            assertTrue(files.toString(), files.contains("_1.cfs"));
            assertTrue(files.toString(), files.contains("_1.cfe"));
        }
        assertTrue(files.toString(), files.contains("_1.si"));
        writer.close();
        dir.close();
    }

    public void testNumDocs() throws IOException {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        SegmentInfos segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(1, Lucene.getNumDocs(segmentCommitInfos));

        doc = new Document();
        doc.add(new TextField("id", "2", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("id", "3", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(1, Lucene.getNumDocs(segmentCommitInfos));
        writer.commit();
        segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(3, Lucene.getNumDocs(segmentCommitInfos));
        writer.deleteDocuments(new Term("id", "2"));
        writer.commit();
        segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(2, Lucene.getNumDocs(segmentCommitInfos));

        int numDocsToIndex = randomIntBetween(10, 50);
        List<Term> deleteTerms = new ArrayList<>();
        for (int i = 0; i < numDocsToIndex; i++) {
            doc = new Document();
            doc.add(new TextField("id", "extra_" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            deleteTerms.add(new Term("id", "extra_" + i));
            writer.addDocument(doc);
        }
        int numDocsToDelete = randomIntBetween(0, numDocsToIndex);
        Collections.shuffle(deleteTerms, random());
        for (int i = 0; i < numDocsToDelete; i++) {
            Term remove = deleteTerms.remove(0);
            writer.deleteDocuments(remove);
        }
        writer.commit();
        segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(2 + deleteTerms.size(), Lucene.getNumDocs(segmentCommitInfos));
        writer.close();
        dir.close();
    }

    public void testCount() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);

        try (DirectoryReader reader = w.getReader()) {
            // match_all does not match anything on an empty index
            IndexSearcher searcher = newSearcher(reader);
            assertFalse(Lucene.exists(searcher, new MatchAllDocsQuery()));
        }

        Document doc = new Document();
        w.addDocument(doc);

        doc.add(new StringField("foo", "bar", Store.NO));
        w.addDocument(doc);

        try (DirectoryReader reader = w.getReader()) {
            IndexSearcher searcher = newSearcher(reader);
            assertTrue(Lucene.exists(searcher, new MatchAllDocsQuery()));
            assertFalse(Lucene.exists(searcher, new TermQuery(new Term("baz", "bar"))));
            assertTrue(Lucene.exists(searcher, new TermQuery(new Term("foo", "bar"))));
        }

        w.deleteDocuments(new Term("foo", "bar"));
        try (DirectoryReader reader = w.getReader()) {
            IndexSearcher searcher = newSearcher(reader);
            assertFalse(Lucene.exists(searcher, new TermQuery(new Term("foo", "bar"))));
        }

        w.close();
        dir.close();
    }

    public void testAsSequentialAccessBits() throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new KeywordAnalyzer()));

        Document doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        w.addDocument(doc);

        doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        w.addDocument(doc);

        try (DirectoryReader reader = DirectoryReader.open(w)) {
            IndexSearcher searcher = newSearcher(reader);
            Weight termWeight = new TermQuery(new Term("foo", "bar")).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
            assertEquals(1, reader.leaves().size());
            LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
            Bits bits = Lucene.asSequentialAccessBits(leafReaderContext.reader().maxDoc(), termWeight.scorerSupplier(leafReaderContext));

            expectThrows(IndexOutOfBoundsException.class, () -> bits.get(-1));
            expectThrows(IndexOutOfBoundsException.class, () -> bits.get(leafReaderContext.reader().maxDoc()));
            assertTrue(bits.get(0));
            assertTrue(bits.get(0));
            assertFalse(bits.get(1));
            assertFalse(bits.get(1));
            expectThrows(IllegalArgumentException.class, () -> bits.get(0));
            assertTrue(bits.get(2));
            assertTrue(bits.get(2));
            expectThrows(IllegalArgumentException.class, () -> bits.get(1));
        }

        w.close();
        dir.close();
    }

    private static class UnsupportedQuery extends Query {

        @Override
        public String toString(String field) {
            return "Unsupported";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof UnsupportedQuery;
        }

        @Override
        public int hashCode() {
            return 42;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new Weight(this) {

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }

                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    return new ScorerSupplier() {

                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public long cost() {
                            return context.reader().maxDoc();
                        }

                    };
                }

            };
        }

    }

    public void testAsSequentialBitsUsesRandomAccess() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new KeywordAnalyzer()))) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField("foo", 5L));
                // we need more than 8 documents because doc values are artificially penalized by IndexOrDocValuesQuery
                for (int i = 0; i < 10; ++i) {
                    w.addDocument(doc);
                }
                w.forceMerge(1);
                try (IndexReader indexReader = DirectoryReader.open(w)) {
                    IndexSearcher searcher = newSearcher(indexReader);
                    IndexReader reader = searcher.getIndexReader();
                    searcher.setQueryCache(null);
                    Query query = new IndexOrDocValuesQuery(new UnsupportedQuery(), NumericDocValuesField.newSlowRangeQuery("foo", 3L, 5L));
                    Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);

                    // Random access by default
                    ScorerSupplier scorerSupplier = weight.scorerSupplier(reader.leaves().get(0));
                    Bits bits = Lucene.asSequentialAccessBits(reader.maxDoc(), scorerSupplier);
                    assertNotNull(bits);
                    assertTrue(bits.get(0));

                    // Moves to sequential access if Bits#get is called more than the number of matches
                    ScorerSupplier scorerSupplier2 = weight.scorerSupplier(reader.leaves().get(0));
                    expectThrows(
                        UnsupportedOperationException.class,
                        () -> Lucene.asSequentialAccessBits(reader.maxDoc(), scorerSupplier2, reader.maxDoc())
                    );
                }
            }
        }
    }

    public void testWrapAllDocsLive() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setMergePolicy(new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETES_FIELD, MatchAllDocsQuery::new, newMergePolicy()));
        IndexWriter writer = new IndexWriter(dir, config);
        int numDocs = between(1, 10);
        Set<String> liveDocs = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            Document doc = new Document();
            doc.add(new StringField("id", id, Store.YES));
            writer.addDocument(doc);
            liveDocs.add(id);
        }
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                String id = Integer.toString(i);
                Document doc = new Document();
                doc.add(new StringField("id", "v2-" + id, Store.YES));
                if (randomBoolean()) {
                    doc.add(Lucene.newSoftDeletesField());
                }
                writer.softUpdateDocument(new Term("id", id), doc, Lucene.newSoftDeletesField());
                liveDocs.add("v2-" + id);
            }
        }
        try (DirectoryReader unwrapped = DirectoryReader.open(writer)) {
            DirectoryReader reader = Lucene.wrapAllDocsLive(unwrapped);
            assertThat(reader.numDocs(), equalTo(liveDocs.size()));
            IndexSearcher searcher = newSearcher(reader);
            Set<String> actualDocs = new HashSet<>();
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            StoredFields storedFields = reader.storedFields();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                actualDocs.add(storedFields.document(scoreDoc.doc).get("id"));
            }
            assertThat(actualDocs, equalTo(liveDocs));
        }
        IOUtils.close(writer, dir);
    }

    public void testWrapLiveDocsNotExposeAbortedDocuments() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setMergePolicy(new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETES_FIELD, MatchAllDocsQuery::new, newMergePolicy()))
            // disable merges on refresh as we will verify the deleted documents
            .setMaxFullFlushMergeWaitMillis(-1);
        IndexWriter writer = new IndexWriter(dir, config);
        int numDocs = between(1, 10);
        List<String> liveDocs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            Document doc = new Document();
            doc.add(new StringField("id", id, Store.YES));
            if (randomBoolean()) {
                doc.add(Lucene.newSoftDeletesField());
            }
            writer.addDocument(doc);
            liveDocs.add(id);
        }
        int abortedDocs = between(1, 10);
        for (int i = 0; i < abortedDocs; i++) {
            try {
                Document doc = new Document();
                doc.add(new StringField("id", "aborted-" + i, Store.YES));
                StringReader reader = new StringReader("");
                doc.add(new TextField("other", reader));
                reader.close(); // mark the indexing hit non-aborting error
                writer.addDocument(doc);
                fail("index should have failed");
            } catch (Exception ignored) {}
        }
        try (DirectoryReader unwrapped = DirectoryReader.open(writer)) {
            DirectoryReader reader = Lucene.wrapAllDocsLive(unwrapped);
            assertThat(reader.maxDoc(), equalTo(numDocs + abortedDocs));
            assertThat(reader.numDocs(), equalTo(liveDocs.size()));
            IndexSearcher searcher = newSearcher(reader);
            List<String> actualDocs = new ArrayList<>();
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            StoredFields storedFields = reader.storedFields();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                actualDocs.add(storedFields.document(scoreDoc.doc).get("id"));
            }
            assertThat(actualDocs, equalTo(liveDocs));
        }
        IOUtils.close(writer, dir);
    }

    public void testSortFieldSerialization() throws IOException {
        Tuple<SortField, SortField> sortFieldTuple = randomSortField();
        SortField deserialized = copyInstance(
            sortFieldTuple.v1(),
            EMPTY_REGISTRY,
            Lucene::writeSortField,
            Lucene::readSortField,
            TransportVersionUtils.randomVersion(random())
        );
        assertEquals(sortFieldTuple.v2(), deserialized);
    }

    public void testSortValueSerialization() throws IOException {
        Object sortValue = randomSortValue();
        Object deserialized = copyInstance(
            sortValue,
            EMPTY_REGISTRY,
            Lucene::writeSortValue,
            Lucene::readSortValue,
            TransportVersionUtils.randomVersion(random())
        );
        assertEquals(sortValue, deserialized);
    }

    public static Object randomSortValue() {
        return switch (randomIntBetween(0, 9)) {
            case 0 -> null;
            case 1 -> randomAlphaOfLengthBetween(3, 10);
            case 2 -> randomInt();
            case 3 -> randomLong();
            case 4 -> randomFloat();
            case 5 -> randomDouble();
            case 6 -> randomByte();
            case 7 -> randomShort();
            case 8 -> randomBoolean();
            case 9 -> new BytesRef(randomAlphaOfLengthBetween(3, 10));
            default -> throw new UnsupportedOperationException();
        };
    }

    public static Tuple<SortField, SortField> randomSortField() {
        switch (randomIntBetween(0, 2)) {
            case 0:
                return randomSortFieldCustomComparatorSource();
            case 1:
                return randomCustomSortField();
            case 2:
                String field = randomAlphaOfLengthBetween(3, 10);
                SortField.Type type = randomFrom(SortField.Type.values());
                if ((type == SortField.Type.SCORE || type == SortField.Type.DOC) && randomBoolean()) {
                    field = null;
                }
                SortField sortField = new SortField(field, type, randomBoolean());
                Object missingValue = randomMissingValue(sortField.getType());
                if (missingValue != null) {
                    sortField.setMissingValue(missingValue);
                }
                return Tuple.tuple(sortField, sortField);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static Tuple<SortField, SortField> randomSortFieldCustomComparatorSource() {
        String field = randomAlphaOfLengthBetween(3, 10);
        IndexFieldData.XFieldComparatorSource comparatorSource;
        boolean reverse = randomBoolean();
        Object missingValue = null;
        switch (randomIntBetween(0, 3)) {
            case 0 -> comparatorSource = new LongValuesComparatorSource(
                null,
                randomBoolean() ? randomLong() : null,
                randomFrom(MultiValueMode.values()),
                null,
                null
            );
            case 1 -> comparatorSource = new DoubleValuesComparatorSource(
                null,
                randomBoolean() ? randomDouble() : null,
                randomFrom(MultiValueMode.values()),
                null
            );
            case 2 -> comparatorSource = new FloatValuesComparatorSource(
                null,
                randomBoolean() ? randomFloat() : null,
                randomFrom(MultiValueMode.values()),
                null
            );
            case 3 -> {
                comparatorSource = new BytesRefFieldComparatorSource(
                    null,
                    randomBoolean() ? "_first" : "_last",
                    randomFrom(MultiValueMode.values()),
                    null
                );
                missingValue = comparatorSource.missingValue(reverse);
            }
            default -> throw new UnsupportedOperationException();
        }
        SortField sortField = new SortField(field, comparatorSource, reverse);
        SortField expected = new SortField(field, comparatorSource.reducedType(), reverse);
        expected.setMissingValue(missingValue);
        return Tuple.tuple(sortField, expected);
    }

    private static Tuple<SortField, SortField> randomCustomSortField() {
        String field = randomAlphaOfLengthBetween(3, 10);
        switch (randomIntBetween(0, 3)) {
            case 0 -> {
                SortField sortField = LatLonDocValuesField.newDistanceSort(field, 0, 0);
                SortField expected = new SortField(field, SortField.Type.DOUBLE);
                expected.setMissingValue(Double.POSITIVE_INFINITY);
                return Tuple.tuple(sortField, expected);
            }
            case 1 -> {
                SortedSetSortField sortField = new SortedSetSortField(field, randomBoolean(), randomFrom(SortedSetSelector.Type.values()));
                SortField expected = new SortField(sortField.getField(), SortField.Type.STRING, sortField.getReverse());
                Object missingValue = randomMissingValue(SortField.Type.STRING);
                sortField.setMissingValue(missingValue);
                expected.setMissingValue(missingValue);
                return Tuple.tuple(sortField, expected);
            }
            case 2 -> {
                SortField.Type type = randomFrom(SortField.Type.DOUBLE, SortField.Type.INT, SortField.Type.FLOAT, SortField.Type.LONG);
                SortedNumericSortField sortField = new SortedNumericSortField(field, type, randomBoolean());
                SortField expected = new SortField(sortField.getField(), sortField.getNumericType(), sortField.getReverse());
                Object missingValue = randomMissingValue(type);
                if (missingValue != null) {
                    sortField.setMissingValue(missingValue);
                    expected.setMissingValue(missingValue);
                }
                return Tuple.tuple(sortField, expected);
            }
            case 3 -> {
                ShardDocSortField sortField = new ShardDocSortField(randomIntBetween(0, 100), randomBoolean());
                SortField expected = new SortField(ShardDocSortField.NAME, SortField.Type.LONG, sortField.getReverse());
                return Tuple.tuple(sortField, expected);
            }
            default -> throw new UnsupportedOperationException();
        }
    }

    private static Object randomMissingValue(SortField.Type type) {
        return switch (type) {
            case INT -> randomInt();
            case FLOAT -> randomFloat();
            case DOUBLE -> randomDouble();
            case LONG -> randomLong();
            case STRING -> randomBoolean() ? SortField.STRING_FIRST : SortField.STRING_LAST;
            default -> null;
        };
    }
}
