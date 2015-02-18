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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Version;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.Test;

import java.io.IOException;
/**
 * 
 */
public class LuceneTest extends ElasticsearchLuceneTestCase {


    /*
     * simple test that ensures that we bump the version on Upgrade
     */
    @Test
    public void testVersion() {
        // note this is just a silly sanity check, we test it in lucene, and we point to it this way
        assertEquals(Lucene.VERSION, Version.LATEST);
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
}
