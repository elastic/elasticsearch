package org.elasticsearch.cluster.routing;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;

public class PlainShardIteratorTests extends ESTestCase {

    public void testEquals() {
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardId shardId2 = new ShardId(index, 2);
        PlainShardIterator iterator1 = new PlainShardIterator(shardId, new ArrayList<>());
        PlainShardIterator iterator2 = new PlainShardIterator(shardId, new ArrayList<>());
        PlainShardIterator iterator3 = new PlainShardIterator(shardId2, new ArrayList<>());
        String s = "Some other random object";
        assertEquals(iterator1, iterator1);
        assertEquals(iterator1, iterator2);
        assertNotEquals(iterator1, null);
        assertNotEquals(iterator1, s);
        assertNotEquals(iterator1, iterator3);
    }
}
