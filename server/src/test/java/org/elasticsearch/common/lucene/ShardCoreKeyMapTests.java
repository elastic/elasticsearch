/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ShardCoreKeyMapTests extends ESTestCase {

    public void testMissingShard() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
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

    public void testAddingAClosedReader() throws Exception {
        LeafReader reader;
        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(new Document());
            try (DirectoryReader dirReader = ElasticsearchDirectoryReader.wrap(writer.getReader(), new ShardId("index1", "_na_", 1))) {
                reader = dirReader.leaves().get(0).reader();
            }
        }
        ShardCoreKeyMap map = new ShardCoreKeyMap();
        try {
            map.add(reader);
            fail("Expected AlreadyClosedException");
        } catch (AlreadyClosedException e) {
            // What we wanted
        }
        assertEquals(0, map.size());
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

        ShardId shardId1 = new ShardId("index1", "_na_", 1);
        ShardId shardId2 = new ShardId("index1", "_na_", 3);
        ShardId shardId3 = new ShardId("index2", "_na_", 2);

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
            assertEquals(shardId2, map.getShardId(ctx.reader().getCoreCacheHelper().getKey()));
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
                index1Keys.add(ctx.reader().getCoreCacheHelper().getKey());
            }
        }
        index1Keys.removeAll(map.getCoreKeysForIndex(new Index("index1", "_na_")));
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

    /** The shards of a renamed index carry two names, and are one index. */
    public void testShardsUnderTwoNamesOfOneIndexAreFoundTogether() throws IOException {
        final String uuid = randomAlphaOfLength(22);
        Directory dir1 = newDirectory();
        RandomIndexWriter w1 = new RandomIndexWriter(random(), dir1);
        w1.addDocument(new Document());
        Directory dir2 = newDirectory();
        RandomIndexWriter w2 = new RandomIndexWriter(random(), dir2);
        w2.addDocument(new Document());
        Directory dir3 = newDirectory();
        RandomIndexWriter w3 = new RandomIndexWriter(random(), dir3);
        w3.addDocument(new Document());

        ElasticsearchDirectoryReader beforeRename = ElasticsearchDirectoryReader.wrap(w1.getReader(), new ShardId("old", uuid, 0));
        ElasticsearchDirectoryReader afterRename = ElasticsearchDirectoryReader.wrap(w2.getReader(), new ShardId("new", uuid, 1));
        ElasticsearchDirectoryReader other = ElasticsearchDirectoryReader.wrap(w3.getReader(), new ShardId("old", "another-uuid", 0));

        ShardCoreKeyMap map = new ShardCoreKeyMap();
        final Set<Object> expected = new HashSet<>();
        for (ElasticsearchDirectoryReader reader : Arrays.asList(beforeRename, afterRename)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                map.add(ctx.reader());
                expected.add(ctx.reader().getCoreCacheHelper().getKey());
            }
        }
        for (LeafReaderContext ctx : other.leaves()) {
            map.add(ctx.reader());
        }

        // by either name, and by the UUID alone, and not the index that only shares a name
        assertEquals(expected, map.getCoreKeysForIndex(new Index("old", uuid)));
        assertEquals(expected, map.getCoreKeysForIndex(new Index("new", uuid)));
        assertEquals(expected, map.getCoreKeysForIndex(new Index("anything", uuid)));

        beforeRename.close();
        afterRename.close();
        other.close();
        w1.close();
        w2.close();
        w3.close();
        assertEquals(0, map.size());
        IOUtils.close(dir1, dir2, dir3);
    }
}
