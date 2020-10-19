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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.function.BooleanSupplier;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that exposes
 * Elasticsearch internal per shard / index information like the shard ID.
 */
public final class ElasticsearchDirectoryReader extends FilterDirectoryReader {

    private final ShardId shardId;
    private final FilterDirectoryReader.SubReaderWrapper wrapper;
    private final CheckedFunction<DirectoryReader, String, IOException> readerIdGenerator;
    private final String readerId;

    private ElasticsearchDirectoryReader(DirectoryReader in, FilterDirectoryReader.SubReaderWrapper wrapper, ShardId shardId,
                                         CheckedFunction<DirectoryReader, String, IOException> readerIdGenerator) throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
        this.readerIdGenerator = readerIdGenerator;
        this.readerId = readerIdGenerator.apply(in);
    }

    /**
     * If two readers have the same reader id, then their underlying reader must consist of the same list of segments.
     */
    @Nullable
    public String getReaderId() {
        return readerId;
    }

    public static String getReaderIdFromSegmentInfos(SegmentInfos segmentInfos, BooleanSupplier fullyCommitted) {
        // Here we prefer using an id composed of the ids of the underlying segments instead of the id of the commit because
        // the commit id changes whenever IndexWriter#commit is called although the segment files stay unchanged. A file-based
        // recovery creates a new commit to associate the new translog uuid. Hence, the commit ids of the primary and replicas
        // are always different although they can have the identical segment files.
        final MessageDigest md = MessageDigests.sha256();
        for (SegmentCommitInfo sci : segmentInfos) {
            final byte[] segmentId = sci.getId();
            if (segmentId != null) {
                md.update(segmentId);
            } else {
                // old segment
                if (fullyCommitted.getAsBoolean()) {
                    md.reset();
                    md.update(segmentInfos.getId());
                    break;
                } else {
                    return null;
                }
            }
        }
        return MessageDigests.toHexString(md.digest());
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
        return new ElasticsearchDirectoryReader(in, wrapper, shardId, readerIdGenerator);
    }

    // Remove this helpers
    public static ElasticsearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId) throws IOException {
        return new ElasticsearchDirectoryReader(reader, new SubReaderWrapper(shardId), shardId, r -> null);
    }

    /**
     * Wraps the given reader in a {@link ElasticsearchDirectoryReader} as
     * well as all it's sub-readers in {@link ElasticsearchLeafReader} to
     * expose the given shard Id.
     *
     * @param reader            the reader to wrap
     * @param shardId           the shard ID to expose via the elasticsearch internal reader wrappers
     * @param readerIdGenerator a function that returns the id of the reader to wrap
     */
    public static ElasticsearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId,
                                                    CheckedFunction<DirectoryReader, String, IOException> readerIdGenerator)
        throws IOException {
        return new ElasticsearchDirectoryReader(reader, new SubReaderWrapper(shardId), shardId, readerIdGenerator);
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
                    "Can't install close listener reader is not an ElasticsearchDirectoryReader/ElasticsearchLeafReader");
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
