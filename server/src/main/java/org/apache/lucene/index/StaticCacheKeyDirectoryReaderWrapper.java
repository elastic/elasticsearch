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
            createCacheHelper(cachedKeys, onCloseCallbacks, "$$dir_reader$$", in.getReaderCacheHelper());
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
            createCacheHelper(cachedKeys, onCloseCallbacks, idString + "_core", segmentReader.getCoreCacheHelper());
        final CacheHelper adaptedReaderCacheHelper =
            createCacheHelper(cachedKeys, onCloseCallbacks, idString + "_reader", segmentReader.getReaderCacheHelper());
        return reader instanceof CodecReader ?
            new StaticCacheKeyFilterCodecReader((CodecReader) reader, adaptedCoreCacheHelper, adaptedReaderCacheHelper) :
            new StaticCacheKeyFilterLeafReader(reader, adaptedCoreCacheHelper, adaptedReaderCacheHelper);
    }

    private static CacheHelper createCacheHelper(Map<String, CacheKey> cachedKeys, List<Closeable> onCloseCallbacks, String idString,
                                                 CacheHelper cacheHelper) {
        final CacheKey originalCacheKey = cacheHelper.getKey();
        final CacheKey newCacheKey = cachedKeys.computeIfAbsent(idString, ignore -> originalCacheKey);
        return new StaticCacheKeyHelper(newCacheKey, cacheHelper, onCloseCallbacks);
    }

    static final class StaticCacheKeyFilterCodecReader extends FilterCodecReader {

        private final CacheHelper coreCacheHelper;
        private final CacheHelper readerCacheHelper;

        StaticCacheKeyFilterCodecReader(CodecReader in, CacheHelper coreCacheHelper, CacheHelper readerCacheHelper) {
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

    static final class StaticCacheKeyFilterLeafReader extends FilterLeafReader {

        private final CacheHelper coreCacheHelper;
        private final CacheHelper readerCacheHelper;

        StaticCacheKeyFilterLeafReader(LeafReader in, CacheHelper coreCacheHelper, CacheHelper readerCacheHelper) {
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

    public static class StaticCacheKeyHelper implements CacheHelper {

        private final CacheKey coreCacheKey;
        private final List<Closeable> onCloseCallbacks;
        private final CacheHelper originalCacheHelper;

        public StaticCacheKeyHelper(CacheKey coreCacheKey, CacheHelper originalCacheHelper, List<Closeable> onCloseCallbacks) {
            this.coreCacheKey = coreCacheKey;
            this.onCloseCallbacks = onCloseCallbacks;
            this.originalCacheHelper = originalCacheHelper;
        }

        @Override
        public CacheKey getKey() {
            return coreCacheKey;
        }

        @Override
        public void addClosedListener(ClosedListener listener) {
            onCloseCallbacks.add(() -> listener.onClose(coreCacheKey));
        }

        /** Allows setting a ClosedListener on the actual CacheHelper instance, to be notified about closing of the actual reader **/
        public void addTrueClosedListener(ClosedListener listener) {
            originalCacheHelper.addClosedListener(listener);
        }
    }
}
