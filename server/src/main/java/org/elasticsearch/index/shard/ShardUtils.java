/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.core.Nullable;
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
