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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that exposes
 * Elasticsearch internal per shard / index information like the shard ID.
 */
public final class ElasticsearchDirectoryReader extends FilterDirectoryReader {

    private final ShardId shardId;
    private final FilterDirectoryReader.SubReaderWrapper wrapper;

    private ElasticsearchDirectoryReader(DirectoryReader in, FilterDirectoryReader.SubReaderWrapper wrapper, ShardId shardId) throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
    }

    /**
     * Returns the shard id this index belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new ElasticsearchDirectoryReader(in, wrapper, shardId);
    }

    /**
     * Wraps the given reader in a {@link org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader} as
     * well as all it's sub-readers in {@link org.elasticsearch.common.lucene.index.ElasticsearchLeafReader} to
     * expose the given shard Id.
     *
     * @param reader the reader to wrap
     * @param shardId the shard ID to expose via the elasticsearch internal reader wrappers.
     */
    public static ElasticsearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId) throws IOException {
        return new ElasticsearchDirectoryReader(reader, new SubReaderWrapper(shardId), shardId);
    }

    private final static class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
        private final ShardId shardId;
        SubReaderWrapper(ShardId shardId) {
            this.shardId = shardId;
        }
        @Override
        public LeafReader wrap(LeafReader reader) {
            return new ElasticsearchLeafReader(reader, shardId);
        }
    }

}
