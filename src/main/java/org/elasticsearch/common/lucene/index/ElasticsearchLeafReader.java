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

/**
 * A {@link org.apache.lucene.index.FilterLeafReader} that exposes
 * Elasticsearch internal per shard / index information like the shard ID.
 */
public final class ElasticsearchLeafReader extends FilterLeafReader {

    private final ShardId shardId;

    /**
     * <p>Construct a FilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     * @param in specified base reader.
     */
    ElasticsearchLeafReader(LeafReader in, ShardId shardId) {
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
    public Object getCoreCacheKey() {
        return in.getCoreCacheKey();
    }

    @Override
    public Object getCombinedCoreAndDeletesKey() {
        return in.getCombinedCoreAndDeletesKey();
    }
}
