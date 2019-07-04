/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DocumentSubsetReaderTests extends ESTestCase {

    private Directory directory;
    private DirectoryReader directoryReader;
    private DocumentSubsetBitsetCache bitsetCache;
    private boolean strictTermsEnum;

    @Before
    public void setUpDirectory() {
        // We check it is empty at the end of the test, so make sure it is empty in the
        // beginning as well so that we can easily distinguish from garbage added by
        // this test and garbage not cleaned up by other tests.
        assertTrue(DocumentSubsetReader.NUM_DOCS_CACHE.toString(),
                DocumentSubsetReader.NUM_DOCS_CACHE.isEmpty());
        directory = newDirectory();
        bitsetCache = new DocumentSubsetBitsetCache(Settings.EMPTY);
    }

    @After
    public void cleanDirectory() throws Exception {
        if (directoryReader != null) {
            directoryReader.close();
        }
        assertTrue(DocumentSubsetReader.NUM_DOCS_CACHE.toString(),
                DocumentSubsetReader.NUM_DOCS_CACHE.isEmpty());
        directory.close();
        bitsetCache.close();
    }

    public void testSearch() throws Exception {
        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random())));

        Document document = new Document();
        document.add(new StringField("field", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field", "value2", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field", "value3", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field", "value4", Field.Store.NO));
        iw.addDocument(document);

        iw.forceMerge(1);
        iw.deleteDocuments(new Term("field", "value3"));
        iw.close();
        openDirectoryReader();

        IndexSearcher indexSearcher = new IndexSearcher(DocumentSubsetReader.wrap(directoryReader, bitsetCache,
                new TermQuery(new Term("field", "value1")), strictTermsEnum));
        assertThat(indexSearcher.getIndexReader().numDocs(), equalTo(1));
        TopDocs result = indexSearcher.search(new MatchAllDocsQuery(), 1);
        assertThat(result.totalHits.value, equalTo(1L));
        assertThat(result.scoreDocs[0].doc, equalTo(0));

        indexSearcher = new IndexSearcher(DocumentSubsetReader.wrap(directoryReader, bitsetCache,
                new TermQuery(new Term("field", "value2")), strictTermsEnum));
        assertThat(indexSearcher.getIndexReader().numDocs(), equalTo(1));
        result = indexSearcher.search(new MatchAllDocsQuery(), 1);
        assertThat(result.totalHits.value, equalTo(1L));
        assertThat(result.scoreDocs[0].doc, equalTo(1));

        // this doc has been marked as deleted:
        indexSearcher = new IndexSearcher(DocumentSubsetReader.wrap(directoryReader, bitsetCache,
                new TermQuery(new Term("field", "value3")), strictTermsEnum));
        assertThat(indexSearcher.getIndexReader().numDocs(), equalTo(0));
        result = indexSearcher.search(new MatchAllDocsQuery(), 1);
        assertThat(result.totalHits.value, equalTo(0L));

        indexSearcher = new IndexSearcher(DocumentSubsetReader.wrap(directoryReader, bitsetCache,
                new TermQuery(new Term("field", "value4")), strictTermsEnum));
        assertThat(indexSearcher.getIndexReader().numDocs(), equalTo(1));
        result = indexSearcher.search(new MatchAllDocsQuery(), 1);
        assertThat(result.totalHits.value, equalTo(1L));
        assertThat(result.scoreDocs[0].doc, equalTo(3));
    }

    public void testTermsAgg() throws IOException {
        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig());

        Document document = new Document();
        document.add(new StringField("field", "value1", Field.Store.NO));
        document.add(new SortedSetDocValuesField("field", new BytesRef("value1")));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field", "value2", Field.Store.NO));
        document.add(new SortedSetDocValuesField("field", new BytesRef("value2")));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field", "value3", Field.Store.NO));
        document.add(new SortedSetDocValuesField("field", new BytesRef("value3")));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field", "value2", Field.Store.NO));
        document.add(new SortedSetDocValuesField("field", new BytesRef("value2")));
        iw.addDocument(document);

        iw.forceMerge(1);
        iw.deleteDocuments(new Term("field", "value3"));
        iw.close();
        openDirectoryReader();

        IndexSearcher indexSearcher = new IndexSearcher(DocumentSubsetReader.wrap(directoryReader, bitsetCache,
            new TermQuery(new Term("field", "value2")), false));

        TermsAggCollector collector = new TermsAggCollector("field", false);
        indexSearcher.search(new MatchAllDocsQuery(), collector);
        Map<String, Long> counts = collector.getCounts();
        assertEquals(Collections.singletonMap("value2", 2L), counts);

        UnsupportedOperationException uoe = expectThrows(UnsupportedOperationException.class,
            () -> {
                TermsAggCollector collector2 = new TermsAggCollector("field", true);
                indexSearcher.search(new MatchAllDocsQuery(), collector2);
                collector2.getCounts();
            });
        // TODO: assert on the error message
    }

    public void testTermsAggOnUnindexedField() throws IOException {
        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig());

        Document document = new Document();
        document.add(new SortedSetDocValuesField("field", new BytesRef("value1")));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("keep_me", "yes", Field.Store.NO));
        document.add(new SortedSetDocValuesField("field", new BytesRef("value2")));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("delete", "yes", Field.Store.NO));
        document.add(new SortedSetDocValuesField("field", new BytesRef("value3")));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("keep_me", "yes", Field.Store.NO));
        document.add(new SortedSetDocValuesField("field", new BytesRef("value2")));
        iw.addDocument(document);

        iw.forceMerge(1);
        iw.deleteDocuments(new Term("delete", "yes"));
        iw.close();
        openDirectoryReader();

        IndexSearcher indexSearcher = new IndexSearcher(DocumentSubsetReader.wrap(directoryReader, bitsetCache,
            new TermQuery(new Term("keep_me", "yes")), false));

        UnsupportedOperationException uoe = expectThrows(UnsupportedOperationException.class,
            () -> {
                TermsAggCollector collector = new TermsAggCollector("field", false);
                indexSearcher.search(new MatchAllDocsQuery(), collector);
                collector.getCounts();
            });
        // TODO: assert on the error message

        uoe = expectThrows(UnsupportedOperationException.class,
            () -> {
                TermsAggCollector collector = new TermsAggCollector("field", true);
                indexSearcher.search(new MatchAllDocsQuery(), collector);
                collector.getCounts();
            });
        // TODO: assert on the error message
    }

    /**
     * Simplified version of the collector for terms aggs.
     */
    static class TermsAggCollector extends SimpleCollector {

        private final String field;
        private final boolean includeZeroCounts;
        private final Map<String, Long> counts = new HashMap<>();
        private SortedSetDocValues values;
        private long[] countsByOrd;

        TermsAggCollector(String field, boolean includeZeroCounts) {
            this.field = field;
            this.includeZeroCounts = includeZeroCounts;
        }

        private void mergeCounts() throws IOException {
            if (countsByOrd != null) {
                for (int ord = 0; ord < countsByOrd.length; ++ord) {
                    final long count = countsByOrd[ord];
                    if (includeZeroCounts || count != 0) {
                        BytesRef key = values.lookupOrd(ord);
                        counts.compute(key.utf8ToString(), (k, v) -> count + (v == null ? 0 : v));
                    }
                }
                countsByOrd = null;
                values = null;
            }
        }

        Map<String, Long> getCounts() throws IOException {
            mergeCounts();
            return Collections.unmodifiableMap(counts);
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        protected void doSetNextReader(LeafReaderContext context) throws IOException {
            mergeCounts();
            values = context.reader().getSortedSetDocValues(field);
            if (values != null) {
                countsByOrd = new long[Math.toIntExact(values.getValueCount())];
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            if (values.advanceExact(doc) == false) {
                return;
            }
            for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                countsByOrd[Math.toIntExact(ord)]++;
            }
        }

    }

    public void testLiveDocs() throws Exception {
        int numDocs = scaledRandomIntBetween(16, 128);
        IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
        );

        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("field", "value" + i, Field.Store.NO));
            iw.addDocument(document);
        }

        iw.forceMerge(1);
        iw.close();

        openDirectoryReader();
        assertThat("should have one segment after force merge", directoryReader.leaves().size(), equalTo(1));

        for (int i = 0; i < numDocs; i++) {
            Query roleQuery = new TermQuery(new Term("field", "value" + i));
            DirectoryReader wrappedReader = DocumentSubsetReader.wrap(directoryReader, bitsetCache, roleQuery, strictTermsEnum);

            LeafReader leafReader = wrappedReader.leaves().get(0).reader();
            assertThat(leafReader.hasDeletions(), is(true));
            assertThat(leafReader.numDocs(), equalTo(1));
            Bits liveDocs = leafReader.getLiveDocs();
            assertThat(liveDocs.length(), equalTo(numDocs));
            for (int docId = 0; docId < numDocs; docId++) {
                if (docId == i) {
                    assertThat("docId [" + docId +"] should match", liveDocs.get(docId), is(true));
                } else {
                    assertThat("docId [" + docId +"] should not match", liveDocs.get(docId), is(false));
                }
            }
        }
    }

    public void testWrapTwice() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        iw.close();
        DirectoryReader directoryReader =
            DocumentSubsetReader.wrap(DirectoryReader.open(dir), bitsetCache, new MatchAllDocsQuery(), strictTermsEnum);
        try {
            DocumentSubsetReader.wrap(directoryReader, bitsetCache, new MatchAllDocsQuery(), strictTermsEnum);
            fail("shouldn't be able to wrap DocumentSubsetDirectoryReader twice");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't wrap [class org.elasticsearch.xpack.core.security.authz.accesscontrol" +
                    ".DocumentSubsetReader$DocumentSubsetDirectoryReader] twice"));
        }

        bitsetCache.close();
        directoryReader.close();
        dir.close();
    }

    /** Same test as in FieldSubsetReaderTests, test that core cache key (needed for NRT) is working */
    public void testCoreCacheKey() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMaxBufferedDocs(100);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);

        // add two docs, id:0 and id:1
        Document doc = new Document();
        Field idField = new StringField("id", "", Field.Store.NO);
        doc.add(idField);
        idField.setStringValue("0");
        iw.addDocument(doc);
        idField.setStringValue("1");
        iw.addDocument(doc);

        // open reader
        DirectoryReader ir = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(iw), new ShardId("_index", "_na_", 0));
        ir = DocumentSubsetReader.wrap(ir, bitsetCache, new MatchAllDocsQuery(), strictTermsEnum);
        assertEquals(2, ir.numDocs());
        assertEquals(1, ir.leaves().size());

        // delete id:0 and reopen
        iw.deleteDocuments(new Term("id", "0"));
        DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);

        // we should have the same cache key as before
        assertEquals(1, ir2.numDocs());
        assertEquals(1, ir2.leaves().size());
        assertSame(ir.leaves().get(0).reader().getCoreCacheHelper().getKey(),
                ir2.leaves().get(0).reader().getCoreCacheHelper().getKey());
        // However we don't support caching on the reader cache key since we override deletes
        assertNull(ir.leaves().get(0).reader().getReaderCacheHelper());
        assertNull(ir2.leaves().get(0).reader().getReaderCacheHelper());

        DirectoryReader finalIr = ir;
        UnsupportedOperationException uoe = expectThrows(UnsupportedOperationException.class, () -> {
            TestUtil.checkReader(finalIr);
        });
        IOUtils.close(ir, ir2, iw, dir);
    }

    private void openDirectoryReader() throws IOException {
        directoryReader = DirectoryReader.open(directory);
        directoryReader = ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId("_index", "_na_", 0));
    }
}
