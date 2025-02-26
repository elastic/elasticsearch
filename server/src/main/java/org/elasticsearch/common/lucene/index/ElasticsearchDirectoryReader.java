/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.ESCacheHelper;

import java.io.IOException;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that exposes
 * Elasticsearch internal per shard / index information like the shard ID.
 */
public final class ElasticsearchDirectoryReader extends FilterDirectoryReader {

    private final ShardId shardId;
    private final FilterDirectoryReader.SubReaderWrapper wrapper;
    @Nullable
    private final ESCacheHelper esCacheHelper;

    private ElasticsearchDirectoryReader(
        DirectoryReader in,
        FilterDirectoryReader.SubReaderWrapper wrapper,
        ShardId shardId,
        @Nullable ESCacheHelper esCacheHelper
    ) throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
        this.esCacheHelper = esCacheHelper;
    }

    /**
     * Returns the shard id this index belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // safe to delegate since this reader does not alter the index
        return in.getReaderCacheHelper();
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new ElasticsearchDirectoryReader(in, wrapper, shardId, esCacheHelper);
    }

    /**
     * Wraps the given reader in a {@link ElasticsearchDirectoryReader} as
     * well as all it's sub-readers in {@link ElasticsearchLeafReader} to
     * expose the given shard Id.
     *
     * @param reader the reader to wrap
     * @param shardId the shard ID to expose via the elasticsearch internal reader wrappers.
     */
    public static ElasticsearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId) throws IOException {
        return wrap(reader, shardId, null);
    }

    /**
     * Wraps the given reader in a {@link ElasticsearchDirectoryReader} as
     * well as all it's sub-readers in {@link ElasticsearchLeafReader} to
     * expose the given shard Id.
     * @param reader        the reader to wrap
     * @param shardId       the shard ID to expose via the elasticsearch internal reader wrappers.
     * @param esCacheHelper the custom {@link ESCacheHelper} implementation that doesn't tie
     *                      its lifecycle to that of the underlying reader
     */
    public static ElasticsearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId, @Nullable ESCacheHelper esCacheHelper)
        throws IOException {
        return new ElasticsearchDirectoryReader(reader, new SubReaderWrapper(shardId), shardId, esCacheHelper);
    }

    /**
     * Retrieves Elasticsearch's version of the reader cache helper (see {@link ESCacheHelper})
     */
    public static ESCacheHelper getESReaderCacheHelper(DirectoryReader reader) {
        ElasticsearchDirectoryReader esReader = getElasticsearchDirectoryReader(reader);
        assert esReader != null;
        // even though we assert that the reader is non-null, we are a bit lenient here,
        // as falling back to the underlying cache helper does not affect correctness
        if (esReader == null || esReader.esCacheHelper == null) {
            return new ESCacheHelper.Wrapper(reader.getReaderCacheHelper());
        }
        return esReader.esCacheHelper;
    }

    private static final class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
        private final ShardId shardId;

        SubReaderWrapper(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return new ElasticsearchLeafReader(reader, shardId);
        }
    }

    /**
     * Adds the given listener to the provided directory reader. The reader
     * must contain an {@link ElasticsearchDirectoryReader} in it's hierarchy
     * otherwise we can't safely install the listener.
     *
     * @throws IllegalArgumentException if the reader doesn't contain an
     *     {@link ElasticsearchDirectoryReader} in it's hierarchy
     */
    @SuppressForbidden(reason = "This is the only sane way to add a ReaderClosedListener")
    public static void addReaderCloseListener(DirectoryReader reader, IndexReader.ClosedListener listener) {
        ElasticsearchDirectoryReader elasticsearchDirectoryReader = getElasticsearchDirectoryReader(reader);
        if (elasticsearchDirectoryReader == null) {
            throw new IllegalArgumentException(
                "Can't install close listener reader is not an ElasticsearchDirectoryReader/ElasticsearchLeafReader"
            );
        }
        IndexReader.CacheHelper cacheHelper = elasticsearchDirectoryReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + elasticsearchDirectoryReader + " does not support caching");
        }
        assert cacheHelper.getKey() == reader.getReaderCacheHelper().getKey();
        cacheHelper.addClosedListener(listener);
    }

    /**
     * Tries to unwrap the given reader until the first
     * {@link ElasticsearchDirectoryReader} instance is found or {@code null}
     * if no instance is found.
     */
    public static ElasticsearchDirectoryReader getElasticsearchDirectoryReader(DirectoryReader reader) {
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
