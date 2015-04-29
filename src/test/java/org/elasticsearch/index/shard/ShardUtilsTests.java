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
package org.elasticsearch.index.shard;


import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.IOException;

public class ShardUtilsTests extends ElasticsearchTestCase {

    public void testExtractShardId() throws IOException {
        BaseDirectoryWrapper dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        writer.commit();
        ShardId id = new ShardId("foo", random().nextInt());
        try (DirectoryReader reader = DirectoryReader.open(writer, random().nextBoolean())) {
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

        try (DirectoryReader reader = DirectoryReader.open(writer, random().nextBoolean())) {
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
