/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;


import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ShardUtilsTests extends ESTestCase {

    public void testExtractShardId() throws IOException {
        BaseDirectoryWrapper dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        writer.commit();
        ShardId id = new ShardId("foo", "_na_", random().nextInt());
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            ElasticsearchDirectoryReader wrap = ElasticsearchDirectoryReader.wrap(reader, id);
            assertEquals(id, ShardUtils.extractShardId(wrap));
        }
        final int numDocs = 1 + random().nextInt(5);
        for (int i = 0; i < numDocs; i++) {
            Document d = new Document();
            d.add(newField("name", "foobar", StringField.TYPE_STORED));
            writer.addDocument(d);
            if (random().nextBoolean()) {
                writer.commit();
            }
        }

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            ElasticsearchDirectoryReader wrap = ElasticsearchDirectoryReader.wrap(reader, id);
            assertEquals(id, ShardUtils.extractShardId(wrap));
            CompositeReaderContext context = wrap.getContext();
            for (LeafReaderContext leaf : context.leaves()) {
                assertEquals(id, ShardUtils.extractShardId(leaf.reader()));
            }
        }
        IOUtils.close(writer, dir);
    }
}
