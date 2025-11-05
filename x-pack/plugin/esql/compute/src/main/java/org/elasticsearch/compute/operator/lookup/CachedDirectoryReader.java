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
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;

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
        final Map<String, Terms> termsCache = new HashMap<>();

        CachedLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            NumericDocValues dv = super.getNumericDocValues(field);
            if (dv == null) {
                return null;
            }
            return new CachedNumericDocValues(docId -> docValues.compute(field, (k, curr) -> {
                if (curr == null || curr.docID() > docId) {
                    return dv;
                }
                return curr;
            }));
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = termsCache.computeIfAbsent(field, k -> {
                try {
                    return super.terms(k);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            if (terms == null) {
                return null;
            }
            // Return a FilterTerms that always creates a fresh TermsEnum iterator
            // We cache the Terms object itself for performance, but always create fresh TermsEnum
            // instances because TermsEnum maintains position state and reusing it causes incorrect
            // results when the same field is accessed multiple times with different conditions
            // (e.g., in OR NOT expressions like: OR NOT (other1 != "omicron" AND other1 != "nu"))
            return new FilterTerms(terms) {
                @Override
                public TermsEnum iterator() throws IOException {
                    return in.iterator();
                }
            };
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
        private final IntFunction<NumericDocValues> fromCache;

        CachedNumericDocValues(IntFunction<NumericDocValues> fromCache) {
            this.fromCache = fromCache;
        }

        NumericDocValues getDelegate(int docID) {
            if (delegate == null) {
                delegate = fromCache.apply(docID);
            }
            return delegate;
        }

        @Override
        public long longValue() throws IOException {
            return getDelegate(-1).longValue();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return getDelegate(target).advanceExact(target);
        }

        @Override
        public int advance(int target) throws IOException {
            return getDelegate(target).nextDoc();
        }

        @Override
        public int docID() {
            return getDelegate(-1).docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return getDelegate(-1).nextDoc();
        }

        @Override
        public long cost() {
            return fromCache.apply(DocIdSetIterator.NO_MORE_DOCS).cost();
        }
    }
}
