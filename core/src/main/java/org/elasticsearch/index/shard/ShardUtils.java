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

import org.apache.lucene.index.*;
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
        final ElasticsearchLeafReader esReader = getElasticsearchLeafReader(reader);
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
    public static ShardId extractShardId(IndexReader reader) {
        final ElasticsearchDirectoryReader esReader = getElasticsearchDirectoryReader(reader);
        if (esReader != null) {
            return esReader.shardId();
        }
        if (!reader.leaves().isEmpty()) {
            return extractShardId(reader.leaves().get(0).reader());
        }
        return null;
    }

    private static ElasticsearchLeafReader getElasticsearchLeafReader(LeafReader reader) {
        if (reader instanceof FilterLeafReader) {
            if (reader instanceof ElasticsearchLeafReader) {
                return (ElasticsearchLeafReader) reader;
            } else {
                // We need to use FilterLeafReader#getDelegate and not FilterLeafReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of ElasticsearchLeafReader. This can cause us to miss the shardId.
                return getElasticsearchLeafReader(((FilterLeafReader) reader).getDelegate());
            }
        }
        return null;
    }

    private static ElasticsearchDirectoryReader getElasticsearchDirectoryReader(IndexReader reader) {
        if (reader instanceof FilterDirectoryReader) {
            if (reader instanceof ElasticsearchDirectoryReader) {
                return (ElasticsearchDirectoryReader) reader;
            } else {
                // We need to use FilterDirectoryReader#getDelegate and not FilterDirectoryReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of ElasticsearchLeafReader. This can cause us to miss the shardId.
                return getElasticsearchDirectoryReader(((FilterDirectoryReader) reader).getDelegate());
            }
        }
        return null;
    }

}
