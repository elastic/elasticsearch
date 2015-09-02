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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Version;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 */
public class LuceneTests extends ESTestCase {


    /*
     * simple test that ensures that we bump the version on Upgrade
     */
    @Test
    public void testVersion() {
        // note this is just a silly sanity check, we test it in lucene, and we point to it this way
        assertEquals(Lucene.VERSION, Version.LATEST);
    }

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

        dir.setEnableVirusScanner(false);
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
        dir.setEnableVirusScanner(false);
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
        try (DirectoryReader open = DirectoryReader.open(writer, true)) {
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
        dir.setEnableVirusScanner(false);
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
        DirectoryReader open = DirectoryReader.open(writer, true);
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
        dir.setEnableVirusScanner(false);
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
        dir.setEnableVirusScanner(false);
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
}
