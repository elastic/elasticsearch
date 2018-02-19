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

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

public class DefaultShardOperationFailedExceptionTests extends ESTestCase {

    public void testToString() {
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
                new ElasticsearchException("foo", new IllegalArgumentException("bar", new RuntimeException("baz"))));
            assertEquals("[null][-1] failed, reason [ElasticsearchException[foo]; nested: " +
                "IllegalArgumentException[bar]; nested: RuntimeException[baz]; ]", exception.toString());
        }
        {
            ElasticsearchException elasticsearchException = new ElasticsearchException("foo");
            elasticsearchException.setIndex(new Index("index1", "_na_"));
            elasticsearchException.setShard(new ShardId("index1", "_na_", 1));
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(elasticsearchException);
            assertEquals("[index1][1] failed, reason [ElasticsearchException[foo]]", exception.toString());
        }
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException("index2", 2, new Exception("foo"));
            assertEquals("[index2][2] failed, reason [Exception[foo]]", exception.toString());
        }
    }

    public void testToXContent() {

    }
}
