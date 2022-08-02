/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;

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
    }
}
