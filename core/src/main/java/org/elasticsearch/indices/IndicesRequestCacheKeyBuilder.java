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

package org.elasticsearch.indices;

import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;

public interface IndicesRequestCacheKeyBuilder {
    /**
     * The key into the request cache for a {@link SearchRequest}. This lookup is already scoped to a single shard so it is safe to ignore
     * that part of the request.
     */
    BytesReference searchRequestKey(ShardSearchRequest request) throws IOException;

    /**
     * The key into the request cache for {@link FieldStats} for a particular field. It is safe to just use the field name as the request
     * is already scoped to the shard but the shard information is provided in case it is useful.
     */
    BytesReference fieldStatsKey(ShardId shardId, String field) throws IOException;

    public class Default implements IndicesRequestCacheKeyBuilder {
        @Override
        public BytesReference searchRequestKey(ShardSearchRequest request) throws IOException {
            return request.cacheKey();
        }

        @Override
        public BytesReference fieldStatsKey(ShardId shardId, String field) {
            return new BytesArray("fieldstats:" + field);
        }
    }
}
