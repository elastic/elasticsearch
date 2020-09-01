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
package org.elasticsearch.common.lucene.index;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.shard.ShardId;

/**
 * A {@link org.apache.lucene.index.FilterLeafReader} that exposes
 * Elasticsearch internal per shard / index information like the shard ID.
 */
public final class ElasticsearchCodecReader extends FilterCodecReader {

    private final ShardId shardId;

    /**
     * <p>Construct a FilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     * @param in specified base reader.
     */
    public ElasticsearchCodecReader(CodecReader in, ShardId shardId) {
        super(in);
        this.shardId = shardId;
    }

    /**
     * Returns the shard id this segment belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    public static ElasticsearchCodecReader getElasticsearchCodecReader(LeafReader reader) {
        reader = FilterLeafReader.unwrap(reader);
        if (reader instanceof FilterCodecReader) {
            if (reader instanceof ElasticsearchCodecReader) {
                return (ElasticsearchCodecReader) reader;
            } else {
                // We need to use FilterCodecReader#getDelegate and not FilterCodecReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered codec reader that
                // may be instance of ElasticsearchCodecReader. This can cause us to miss the shardId.
                return getElasticsearchCodecReader(((FilterCodecReader) reader).getDelegate());
            }
        }
        return null;
    }
}
