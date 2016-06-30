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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static org.apache.lucene.util.TestUtil.randomSimpleString;

public class BulkShardRequestTests extends ESTestCase {
    public void testToString() {
        String index = randomSimpleString(random(), 10);
        int count = between(1, 100);
        BulkShardRequest r = new BulkShardRequest(null, new ShardId(index, "ignored", 0), RefreshPolicy.NONE, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest to [" + index + "] containing [" + count + "] requests", r.toString());
        r = new BulkShardRequest(null, new ShardId(index, "ignored", 0), RefreshPolicy.IMMEDIATE, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest to [" + index + "] containing [" + count + "] requests and a refresh", r.toString());
        r = new BulkShardRequest(null, new ShardId(index, "ignored", 0), RefreshPolicy.WAIT_UNTIL, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest to [" + index + "] containing [" + count + "] requests blocking until refresh", r.toString());
    }
}
