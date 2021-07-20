/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.index;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.lucene.Lucene;

import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class StaticCacheKeyDirectoryReaderWrapper extends FilterDirectoryReader {

    private final CacheHelper readerCacheHelper;
    private final ThreadLocal<Boolean> useStatic = ThreadLocal.withInitial(() -> false);

    public StaticCacheKeyDirectoryReaderWrapper(DirectoryReader in, Map<String, CacheKey> cachedKeys,
                                                List<Closeable> onCloseCallbacks) throws IOException {
        super(in, new StaticCacheKeySubReaderWrapper(cachedKeys, onCloseCallbacks));
        readerCacheHelper = in.getReaderCacheHelper() == null ? null :
            createCacheHelper(cachedKeys, onCloseCallbacks, "$$dir_reader$$", in.getReaderCacheHelper());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return useStatic.get() ? readerCacheHelper : in.getReaderCacheHelper();
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
        return new StaticCacheKeyHelper(newCacheKey, onCloseCallbacks);
    }

    interface StaticCacheKeyLeafReader {
        void useStatic(boolean b);
        boolean useStatic();
    }

    static final class StaticCacheKeyFilterCodecReader extends FilterCodecReader implements StaticCacheKeyLeafReader {

        private final CacheHelper coreCacheHelper;
        private final CacheHelper readerCacheHelper;
        private final ThreadLocal<Boolean> useStatic = ThreadLocal.withInitial(() -> false);

        StaticCacheKeyFilterCodecReader(CodecReader in, CacheHelper coreCacheHelper, CacheHelper readerCacheHelper) {
            super(in);
            this.coreCacheHelper = coreCacheHelper;
            this.readerCacheHelper = readerCacheHelper;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return useStatic.get() ? coreCacheHelper : in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return useStatic.get() ? readerCacheHelper : in.getReaderCacheHelper();
        }

        @Override
        public void useStatic(boolean b) {
            useStatic.set(b);
        }

        @Override
        public boolean useStatic() {
            return useStatic.get();
        }
    }

    static final class StaticCacheKeyFilterLeafReader extends FilterLeafReader implements StaticCacheKeyLeafReader {

        private final CacheHelper coreCacheHelper;
        private final CacheHelper readerCacheHelper;
        private final ThreadLocal<Boolean> useStatic = ThreadLocal.withInitial(() -> false);

        StaticCacheKeyFilterLeafReader(LeafReader in, CacheHelper coreCacheHelper, CacheHelper readerCacheHelper) {
            super(in);
            this.coreCacheHelper = coreCacheHelper;
            this.readerCacheHelper = readerCacheHelper;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return useStatic.get() ? coreCacheHelper : in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return useStatic.get() ? readerCacheHelper : in.getReaderCacheHelper();
        }

        @Override
        public void useStatic(boolean b) {
            useStatic.set(b);
        }

        @Override
        public boolean useStatic() {
            return useStatic.get();
        }
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        throw new UnsupportedOperationException();
    }

    public static <T, E extends Exception> T withStaticCacheHelper(DirectoryReader directoryReader, CheckedSupplier<T, E> r) throws E {
        StaticCacheKeyDirectoryReaderWrapper readerWrapper = getStaticCacheKeyDirectoryReaderWrapper(directoryReader);
        if (readerWrapper != null) {
            assert readerWrapper.useStatic.get() == false : "withStaticCacheHelper can't be nested";
            readerWrapper.useStatic.set(true);
            try {
                return r.get();
            } finally {
                readerWrapper.useStatic.set(false);
            }
        } else {
            return r.get();
        }
    }

    public static <T, E extends Exception> T withStaticCacheHelper(LeafReader leafReader, CheckedSupplier<T, E> r) throws E {
        StaticCacheKeyLeafReader staticCacheKeyLeafReader = getStaticCacheKeyLeafReader(leafReader);
        if (staticCacheKeyLeafReader != null) {
            assert staticCacheKeyLeafReader.useStatic() == false : "withStaticCacheHelper can't be nested";
            staticCacheKeyLeafReader.useStatic(true);
            try {
                return r.get();
            } finally {
                staticCacheKeyLeafReader.useStatic(false);
            }
        } else {
            return r.get();
        }
    }


    private static StaticCacheKeyDirectoryReaderWrapper getStaticCacheKeyDirectoryReaderWrapper(DirectoryReader reader) {
        if (reader instanceof StaticCacheKeyDirectoryReaderWrapper) {
            return (StaticCacheKeyDirectoryReaderWrapper) reader;
        } else if (reader instanceof FilterDirectoryReader) {
            return getStaticCacheKeyDirectoryReaderWrapper(((FilterDirectoryReader) reader).getDelegate());
        }
        return null;
    }

    private static StaticCacheKeyLeafReader getStaticCacheKeyLeafReader(LeafReader reader) {
        if (reader instanceof StaticCacheKeyLeafReader) {
            return (StaticCacheKeyLeafReader) reader;
        } else if (reader instanceof FilterLeafReader) {
            return getStaticCacheKeyLeafReader(((FilterLeafReader) reader).getDelegate());
        } else if (reader instanceof FilterCodecReader) {
            return getStaticCacheKeyLeafReader(((FilterCodecReader) reader).getDelegate());
        }
        return null;
    }

    private static class StaticCacheKeyHelper implements CacheHelper {

        private final CacheKey coreCacheKey;
        private final List<Closeable> onCloseCallbacks;

        StaticCacheKeyHelper(CacheKey coreCacheKey, List<Closeable> onCloseCallbacks) {
            this.coreCacheKey = coreCacheKey;
            this.onCloseCallbacks = onCloseCallbacks;
        }

        @Override
        public CacheKey getKey() {
            return coreCacheKey;
        }

        @Override
        public void addClosedListener(ClosedListener listener) {
            onCloseCallbacks.add(() -> listener.onClose(coreCacheKey));
        }
    }
}
