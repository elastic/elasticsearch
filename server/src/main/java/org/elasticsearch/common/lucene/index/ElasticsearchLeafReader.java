/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.lucene.index;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.shard.ShardId;

/**
 * A {@link org.apache.lucene.index.FilterLeafReader} that exposes
 * Elasticsearch internal per shard / index information like the shard ID.
 */
public final class ElasticsearchLeafReader extends SequentialStoredFieldsLeafReader {

    private final ShardId shardId;

    /**
     * <p>Construct a FilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     * @param in specified base reader.
     */
    public ElasticsearchLeafReader(LeafReader in, ShardId shardId) {
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

    public static ElasticsearchLeafReader getElasticsearchLeafReader(LeafReader reader) {
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

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }
}
