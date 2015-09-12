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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ShardCoreKeyMapTests extends ESTestCase {

    public void testMissingShard() throws IOException {
        try (Directory dir = newDirectory();
                RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            w.addDocument(new Document());
            try (IndexReader reader = w.getReader()) {
                ShardCoreKeyMap map = new ShardCoreKeyMap();
                for (LeafReaderContext ctx : reader.leaves()) {
                    try {
                        map.add(ctx.reader());
                        fail();
                    } catch (IllegalArgumentException expected) {
                        // ok
                    }
                }
            }
        }
    }

    public void testBasics() throws IOException {
        Directory dir1 = newDirectory();
        RandomIndexWriter w1 = new RandomIndexWriter(random(), dir1);
        w1.addDocument(new Document());

        Directory dir2 = newDirectory();
        RandomIndexWriter w2 = new RandomIndexWriter(random(), dir2);
        w2.addDocument(new Document());

        Directory dir3 = newDirectory();
        RandomIndexWriter w3 = new RandomIndexWriter(random(), dir3);
        w3.addDocument(new Document());

        ShardId shardId1 = new ShardId("index1", 1);
        ShardId shardId2 = new ShardId("index1", 3);
        ShardId shardId3 = new ShardId("index2", 2);

        ElasticsearchDirectoryReader reader1 = ElasticsearchDirectoryReader.wrap(w1.getReader(), shardId1);
        ElasticsearchDirectoryReader reader2 = ElasticsearchDirectoryReader.wrap(w2.getReader(), shardId2);
        ElasticsearchDirectoryReader reader3 = ElasticsearchDirectoryReader.wrap(w3.getReader(), shardId3);

        ShardCoreKeyMap map = new ShardCoreKeyMap();
        for (DirectoryReader reader : Arrays.asList(reader1, reader2, reader3)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                map.add(ctx.reader());
            }
        }
        assertEquals(3, map.size());

        // Adding them back is a no-op
        for (LeafReaderContext ctx : reader1.leaves()) {
            map.add(ctx.reader());
        }
        assertEquals(3, map.size());

        for (LeafReaderContext ctx : reader2.leaves()) {
            assertEquals(shardId2, map.getShardId(ctx.reader().getCoreCacheKey()));
        }

        w1.addDocument(new Document());
        ElasticsearchDirectoryReader newReader1 = ElasticsearchDirectoryReader.wrap(w1.getReader(), shardId1);
        reader1.close();
        reader1 = newReader1;

        // same for reader2, but with a force merge to trigger evictions
        w2.addDocument(new Document());
        w2.forceMerge(1);
        ElasticsearchDirectoryReader newReader2 = ElasticsearchDirectoryReader.wrap(w2.getReader(), shardId2);
        reader2.close();
        reader2 = newReader2;

        for (DirectoryReader reader : Arrays.asList(reader1, reader2, reader3)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                map.add(ctx.reader());
            }
        }

        final Set<Object> index1Keys = new HashSet<>();
        for (DirectoryReader reader : Arrays.asList(reader1, reader2)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                index1Keys.add(ctx.reader().getCoreCacheKey());
            }
        }
        index1Keys.removeAll(map.getCoreKeysForIndex("index1"));
        assertEquals(Collections.emptySet(), index1Keys);

        reader1.close();
        w1.close();
        reader2.close();
        w2.close();
        reader3.close();
        w3.close();
        assertEquals(0, map.size());

        dir1.close();
        dir2.close();
        dir3.close();
    }

}
