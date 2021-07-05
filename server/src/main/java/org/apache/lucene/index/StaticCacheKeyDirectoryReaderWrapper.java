/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.index;

import org.elasticsearch.common.lucene.Lucene;

import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class StaticCacheKeyDirectoryReaderWrapper extends FilterDirectoryReader {

    private final CacheHelper readerCacheHelper;

    public StaticCacheKeyDirectoryReaderWrapper(DirectoryReader in, Map<String, CacheKey> cachedKeys,
                                                List<Closeable> onCloseCallbacks) throws IOException {
        super(in, new StaticCacheKeySubReaderWrapper(cachedKeys, onCloseCallbacks));
        readerCacheHelper = in.getReaderCacheHelper() == null ? null :
            createCacheHelper(cachedKeys, onCloseCallbacks, "$$dir_reader$$", in.getReaderCacheHelper().getKey());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return readerCacheHelper;
    }

    private static class StaticCacheKeySubReaderWrapper extends SubReaderWrapper {
        private final Map<String, CacheKey> cachedKeys;
        private final List<Closeable> onCloseCallbacks;

        StaticCacheKeySubReaderWrapper(Map<String, CacheKey> cachedKeys, List<Closeable> onCloseCallbacks) {
            this.cachedKeys = cachedKeys;
            this.onCloseCallbacks = onCloseCallbacks;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return StaticCacheKeyDirectoryReaderWrapper.wrap(reader, cachedKeys, onCloseCallbacks);
        }
    }

    private static LeafReader wrap(LeafReader reader, Map<String, CacheKey> cachedKeys,
                                   List<Closeable> onCloseCallbacks) {
        final SegmentReader segmentReader = Lucene.segmentReader(reader);
        assert segmentReader.isNRT == false : "expected non-NRT reader";
        final SegmentCommitInfo segmentInfo = segmentReader.getSegmentInfo();
        final byte[] id = segmentInfo.getId();
        final String idString = Base64.getEncoder().encodeToString(id);
        final CacheHelper adaptedCoreCacheHelper =
            createCacheHelper(cachedKeys, onCloseCallbacks, idString + "_core", segmentReader.getCoreCacheHelper().getKey());
        final CacheHelper adaptedReaderCacheHelper =
            createCacheHelper(cachedKeys, onCloseCallbacks, idString + "_reader", segmentReader.getReaderCacheHelper().getKey());
        return reader instanceof CodecReader ?
            new StaticCacheKeyFilterCodecReader((CodecReader) reader, adaptedCoreCacheHelper, adaptedReaderCacheHelper) :
            new StaticCacheKeyFilterLeafReader(reader, adaptedCoreCacheHelper, adaptedReaderCacheHelper);
    }

    private static CacheHelper createCacheHelper(Map<String, CacheKey> cachedKeys, List<Closeable> onCloseCallbacks, String idString,
                                                 CacheKey originalCacheKey) {
        final CacheKey coreCacheKey = cachedKeys.computeIfAbsent(idString, ignore -> originalCacheKey);
        return new CacheHelper() {

            @Override
            public CacheKey getKey() {
                return coreCacheKey;
            }

            @Override
            public void addClosedListener(ClosedListener listener) {
                onCloseCallbacks.add(() -> listener.onClose(coreCacheKey));
            }
        };
    }

    public static final class StaticCacheKeyFilterCodecReader extends FilterCodecReader {

        private final CacheHelper coreCacheHelper;
        private final CacheHelper readerCacheHelper;

        public StaticCacheKeyFilterCodecReader(CodecReader in, CacheHelper coreCacheHelper, CacheHelper readerCacheHelper) {
            super(in);
            this.coreCacheHelper = coreCacheHelper;
            this.readerCacheHelper = readerCacheHelper;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return coreCacheHelper;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return readerCacheHelper;
        }
    }

    public static final class StaticCacheKeyFilterLeafReader extends FilterLeafReader {

        private final CacheHelper coreCacheHelper;
        private final CacheHelper readerCacheHelper;

        public StaticCacheKeyFilterLeafReader(LeafReader in, CacheHelper coreCacheHelper, CacheHelper readerCacheHelper) {
            super(in);
            this.coreCacheHelper = coreCacheHelper;
            this.readerCacheHelper = readerCacheHelper;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return coreCacheHelper;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return readerCacheHelper;
        }
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Tries to unwrap the given reader until the first
     * {@link StaticCacheKeyDirectoryReaderWrapper} instance is found or {@code null}
     * if no instance is found.
     */
    public static StaticCacheKeyDirectoryReaderWrapper getStaticCacheKeyDirectoryReaderWrapper(DirectoryReader reader) {
        if (reader instanceof FilterDirectoryReader) {
            if (reader instanceof StaticCacheKeyDirectoryReaderWrapper) {
                return (StaticCacheKeyDirectoryReaderWrapper) reader;
            } else {
                // We need to use FilterDirectoryReader#getDelegate and not FilterDirectoryReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of StaticCacheKeyDirectoryReaderWrapper.
                return getStaticCacheKeyDirectoryReaderWrapper(((FilterDirectoryReader) reader).getDelegate());
            }
        }
        return null;
    }
}
