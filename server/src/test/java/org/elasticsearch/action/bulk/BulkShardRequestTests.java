/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.hamcrest.Matchers.equalTo;

public class BulkShardRequestTests extends ESTestCase {
    public void testToString() {
        String index = randomSimpleString(random(), 10);
        int count = between(2, 100);
        final ShardId shardId = new ShardId(index, "ignored", 0);
        BulkShardRequest r = new BulkShardRequest(shardId, RefreshPolicy.NONE, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0]", r.getDescription());

        r = new BulkShardRequest(shardId, RefreshPolicy.IMMEDIATE, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests and a refresh", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[IMMEDIATE]", r.getDescription());

        r = new BulkShardRequest(shardId, RefreshPolicy.WAIT_UNTIL, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests blocking until refresh", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[WAIT_UNTIL]", r.getDescription());

        r = new BulkShardRequest(shardId, RefreshPolicy.WAIT_UNTIL, new BulkItemRequest[count], true);
        assertEquals(
            "BulkShardRequest [" + shardId + "] containing [" + count + "] requests blocking until refresh, simulated",
            r.toString()
        );
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[WAIT_UNTIL]", r.getDescription());

        r = new BulkShardRequest(shardId, RefreshPolicy.WAIT_UNTIL, new BulkItemRequest[count], false);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests blocking until refresh", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[WAIT_UNTIL]", r.getDescription());
    }

    public void testSerialization() throws IOException {
        // Note: BulkShardRequest does not implement equals or hashCode, so we can't test serialization in the usual way for a Writable
        BulkShardRequest bulkShardRequest = randomBulkShardRequest();
        BulkShardRequest copy = copyWriteable(bulkShardRequest, null, BulkShardRequest::new);
        assertThat(bulkShardRequest.items().length, equalTo(copy.items().length));
        assertThat(bulkShardRequest.isSimulated(), equalTo(copy.isSimulated()));
        assertThat(bulkShardRequest.getRefreshPolicy(), equalTo(copy.getRefreshPolicy()));
    }

    protected BulkShardRequest randomBulkShardRequest() {
        String indexName = randomAlphaOfLength(100);
        ShardId shardId = new ShardId(indexName, randomAlphaOfLength(50), randomInt());
        RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        BulkItemRequest[] items = new BulkItemRequest[randomIntBetween(0, 100)];
        for (int i = 0; i < items.length; i++) {
            final DocWriteRequest<?> request = switch (randomFrom(DocWriteRequest.OpType.values())) {
                case INDEX -> new IndexRequest(indexName).id("id_" + i);
                case CREATE -> new IndexRequest(indexName).id("id_" + i).create(true);
                case UPDATE -> new UpdateRequest(indexName, "id_" + i);
                case DELETE -> new DeleteRequest(indexName, "id_" + i);
            };
            items[i] = new BulkItemRequest(i, request);
        }
        return new BulkShardRequest(shardId, refreshPolicy, items, randomBoolean());
    }
}
