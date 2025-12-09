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
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
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

    void resetTermsEnumCache() {
        for (LeafReaderContext leafContext : leaves()) {
            CachedLeafReader cachedLeafReader = (CachedLeafReader) leafContext.reader();
            cachedLeafReader.termEnums.values().forEach(termEnum -> termEnum.inUsed = false);
        }
    }

    static class CachedLeafReader extends FilterLeafReader {
        final Map<String, NumericDocValues> docValues = new HashMap<>();
        final Map<String, SharedTermEnum> termEnums = new HashMap<>();

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
            Terms terms = super.terms(field);
            if (terms == null) {
                return null;
            }
            return new FilterTerms(terms) {
                @Override
                public TermsEnum iterator() throws IOException {
                    return new CachedTermsEnum((reuse) -> {
                        return termEnums.compute(field, (k, curr) -> {
                            if (curr == null || reuse == false || curr.inUsed) {
                                try {
                                    curr = new SharedTermEnum(in.iterator());
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }
                            curr.inUsed = true;
                            return curr;
                        });
                    });
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

    static final class SharedTermEnum extends FilterLeafReader.FilterTermsEnum {
        boolean inUsed = false;

        SharedTermEnum(TermsEnum delegate) {
            super(delegate);
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

    static class CachedTermsEnum extends TermsEnum {
        private TermsEnum delegate = null;
        private final Function<Boolean, TermsEnum> fromCache;

        CachedTermsEnum(Function<Boolean, TermsEnum> fromCache) {
            this.fromCache = fromCache;
        }

        TermsEnum getDelegate(boolean reuse) {
            if (delegate == null) {
                delegate = fromCache.apply(reuse);
            }
            return delegate;
        }

        @Override
        public AttributeSource attributes() {
            return getDelegate(false).attributes();
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            return getDelegate(true).seekExact(text);
        }

        @Override
        public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
            return getDelegate(false).prepareSeekExact(text);
        }

        @Override
        public void seekExact(long ord) throws IOException {
            getDelegate(true).seekExact(ord);
        }

        @Override
        public void seekExact(BytesRef term, TermState state) throws IOException {
            getDelegate(true).seekExact(term, state);
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            return getDelegate(false).seekCeil(text);
        }

        @Override
        public BytesRef term() throws IOException {
            return getDelegate(false).term();
        }

        @Override
        public long ord() throws IOException {
            return getDelegate(false).ord();
        }

        @Override
        public int docFreq() throws IOException {
            return getDelegate(false).docFreq();
        }

        @Override
        public long totalTermFreq() throws IOException {
            return getDelegate(false).totalTermFreq();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            return getDelegate(false).postings(reuse, flags);
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            return getDelegate(false).impacts(flags);
        }

        @Override
        public TermState termState() throws IOException {
            return getDelegate(false).termState();
        }

        @Override
        public BytesRef next() throws IOException {
            return getDelegate(false).next();
        }
    }
}
