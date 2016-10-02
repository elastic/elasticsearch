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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.index.ElasticsearchLeafReader;

public final class ShardUtils {

    private ShardUtils() {}

    /**
     * Tries to extract the shard id from a reader if possible, when its not possible,
     * will return null.
     */
    @Nullable
    public static ShardId extractShardId(LeafReader reader) {
        final ElasticsearchLeafReader esReader = ElasticsearchLeafReader.getElasticsearchLeafReader(reader);
        if (esReader != null) {
            assert reader.getRefCount() > 0 : "ElasticsearchLeafReader is already closed";
            return esReader.shardId();
        }
        return null;
    }

    /**
     * Tries to extract the shard id from a reader if possible, when its not possible,
     * will return null.
     */
    @Nullable
    public static ShardId extractShardId(DirectoryReader reader) {
        final ElasticsearchDirectoryReader esReader = ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(reader);
        if (esReader != null) {
            return esReader.shardId();
        }
        throw new IllegalArgumentException("can't extract shard ID, can't unwrap ElasticsearchDirectoryReader");
    }



}
