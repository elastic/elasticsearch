/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.lucene;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class LuceneTests extends ESTestCase {
    public void testWaitForIndex() throws Exception {
        final MockDirectoryWrapper dir = newMockDirectory();

        final AtomicBoolean succeeded = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);

        // Create a shadow Engine, which will freak out because there is no
        // index yet
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await();
                    if (Lucene.waitForIndex(dir, 5000)) {
                        succeeded.set(true);
                    } else {
                        fail("index should have eventually existed!");
                    }
                } catch (InterruptedException e) {
                    // ignore interruptions
                } catch (Exception e) {
                    fail("should have been able to create the engine! " + e.getMessage());
                }
            }
        });
        t.start();

        // count down latch
        // now shadow engine should try to be created
        latch.countDown();

        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(2);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();

        t.join();

        writer.close();
        dir.close();
        assertTrue("index should have eventually existed", succeeded.get());
    }

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

        IndexSearcher s = new IndexSearcher(open);
        assertEquals(s.search(new TermQuery(new Term("id", "1")), 1).totalHits, 1);
        assertEquals(s.search(new TermQuery(new Term("id", "2")), 1).totalHits, 1);
        assertEquals(s.search(new TermQuery(new Term("id", "3")), 1).totalHits, 1);
        assertEquals(s.search(new TermQuery(new Term("id", "4")), 1).totalHits, 0);

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
            Weight termWeight = new TermQuery(new Term("foo", "bar")).createWeight(searcher, false, 1f);
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

    /**
     * Test that the "unmap hack" is detected as supported by lucene.
     * This works around the following bug: https://bugs.openjdk.java.net/browse/JDK-4724038
     * <p>
     * While not guaranteed, current status is "Critical Internal API": http://openjdk.java.net/jeps/260
     * Additionally this checks we did not screw up the security logic around the hack.
     */
    public void testMMapHackSupported() throws Exception {
        // add assume's here if needed for certain platforms, but we should know if it does not work.
        assertTrue("MMapDirectory does not support unmapping: " + MMapDirectory.UNMAP_NOT_SUPPORTED_REASON, MMapDirectory.UNMAP_SUPPORTED);
    }
}
