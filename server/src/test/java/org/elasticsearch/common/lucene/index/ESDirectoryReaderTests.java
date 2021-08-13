/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

/** Simple tests for this filterreader */
public class ESDirectoryReaderTests extends ESTestCase {

    /** Test that core cache key (needed for NRT) is working */
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
        ShardId shardId = new ShardId("fake", "_na_", 1);
        DirectoryReader ir = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(iw), shardId);
        assertEquals(2, ir.numDocs());
        assertEquals(1, ir.leaves().size());

        // delete id:0 and reopen
        iw.deleteDocuments(new Term("id", "0"));
        DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);

        // we should have the same cache key as before
        assertEquals(1, ir2.numDocs());
        assertEquals(1, ir2.leaves().size());
        assertSame(ir.leaves().get(0).reader().getCoreCacheHelper().getKey(), ir2.leaves().get(0).reader().getCoreCacheHelper().getKey());
        IOUtils.close(ir, ir2, iw, dir);
    }
}
