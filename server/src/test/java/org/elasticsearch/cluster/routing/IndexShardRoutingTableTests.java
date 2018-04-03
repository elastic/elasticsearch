package org.elasticsearch.cluster.routing;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;

public class IndexShardRoutingTableTests extends ESTestCase {
    public void testEqualsAttributesKey() {
        String[] attr1 = {"a"};
        String[] attr2 = {"b"};
        IndexShardRoutingTable.AttributesKey attributesKey1 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey2 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey3 = new IndexShardRoutingTable.AttributesKey(attr2);
        String s = "Some random other object";
        assertEquals(attributesKey1, attributesKey1);
        assertEquals(attributesKey1, attributesKey2);
        assertNotEquals(attributesKey1, null);
        assertNotEquals(attributesKey1, s);
        assertNotEquals(attributesKey1, attributesKey3);
    }

    public void testEquals() {
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardId shardId2 = new ShardId(index, 2);
        IndexShardRoutingTable table1 = new IndexShardRoutingTable(shardId, new ArrayList<>());
        IndexShardRoutingTable table2 = new IndexShardRoutingTable(shardId, new ArrayList<>());
        IndexShardRoutingTable table3 = new IndexShardRoutingTable(shardId2, new ArrayList<>());
        String s = "Some other random object";
        assertEquals(table1, table1);
        assertEquals(table1, table2);
        assertNotEquals(table1, null);
        assertNotEquals(table1, s);
        assertNotEquals(table1, table3);
    }
}
