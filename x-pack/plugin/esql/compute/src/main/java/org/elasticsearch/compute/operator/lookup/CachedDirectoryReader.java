/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

/**
 * A DirectoryReader that caches NumericDocValues per field.
 */
class CachedDirectoryReader extends FilterDirectoryReader {
    CachedDirectoryReader(DirectoryReader in) throws IOException {
        super(in, new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new CachedLeafReader(reader);
            }
        });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new CachedDirectoryReader(in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    static class CachedLeafReader extends FilterLeafReader {
        final Map<String, NumericDocValues> docValues = new HashMap<>();

        CachedLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            NumericDocValues dv = super.getNumericDocValues(field);
            if (dv == null) {
                // It's important to return null here if the field doesn't have doc values - and the only way
                // to get that consistently is to call super.getNumericDocValues. There are other ways to try,
                // but I don't believe they'll work consistently. So that means we prepare the reader each time,
                // but we don't use it. This still is faster than not caching at all.
                return null;
            }
            return new CachedNumericDocValues(dv::cost, docId -> docValues.compute(field, (k, curr) -> {
                if (curr == null || curr.docID() > docId) {
                    return dv;
                }
                return curr;
            }));
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getCoreCacheHelper();
        }
    }

    static class CachedNumericDocValues extends NumericDocValues {
        private NumericDocValues delegate = null;
        private final LongSupplier cost;
        private final IntFunction<NumericDocValues> fromCache;

        CachedNumericDocValues(LongSupplier cost, IntFunction<NumericDocValues> fromCache) {
            this.cost = cost;
            this.fromCache = fromCache;
        }

        NumericDocValues getOrOverwriteDelegate(int docID) {
            if (delegate == null) {
                // This will return the cached delegate if present
                // However, it could return a new one if the current one is ahead of docID
                // Sometimes, we call with -1 docID to specifically request a new one
                delegate = fromCache.apply(docID);
            }
            return delegate;
        }

        @Override
        public long longValue() throws IOException {
            return getOrOverwriteDelegate(-1).longValue();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return getOrOverwriteDelegate(target).advanceExact(target);
        }

        @Override
        public int advance(int target) throws IOException {
            return getOrOverwriteDelegate(target).advance(target);
        }

        /**
         * If there is a delegate, we will return its docId,
         * otherwise we return -1 to indicate there is no delegate
         */
        @Override
        public int docID() {
            return delegate == null ? -1 : delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return getOrOverwriteDelegate(-1).nextDoc();
        }

        @Override
        public long cost() {
            return cost.getAsLong();
        }
    }
}
