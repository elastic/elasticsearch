/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.snapshots.sourceonly;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * This filter reader fakes sequence ID, primary term and version
 * for a source only index.
 */
final class SeqIdGeneratingFilterReader extends FilterDirectoryReader {
    private final long primaryTerm;

    private SeqIdGeneratingFilterReader(DirectoryReader in, SeqIdGeneratingSubReaderWrapper wrapper) throws IOException {
        super(in, wrapper);
        primaryTerm = wrapper.primaryTerm;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return wrap(in, primaryTerm);
    }

    static DirectoryReader wrap(DirectoryReader in, long primaryTerm) throws IOException {
        Map<LeafReader, LeafReaderContext> ctxMap = new IdentityHashMap<>();
        for (LeafReaderContext leave : in.leaves()) {
            ctxMap.put(leave.reader(), leave);
        }
        return new SeqIdGeneratingFilterReader(in, new SeqIdGeneratingSubReaderWrapper(ctxMap, primaryTerm));
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    private abstract static class FakeNumericDocValues extends NumericDocValues {
        private final int maxDoc;
        int docID = -1;

        FakeNumericDocValues(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public int nextDoc() {
            if (docID+1 < maxDoc) {
                docID++;
            } else {
                docID = NO_MORE_DOCS;
            }
            return docID;
        }

        @Override
        public int advance(int target) {
            if (target >= maxDoc) {
                docID = NO_MORE_DOCS;
            } else {
                docID = target;
            }
            return docID;
        }

        @Override
        public long cost() {
            return maxDoc;
        }

        @Override
        public boolean advanceExact(int target) {
            advance(target);
            return docID != NO_MORE_DOCS;
        }
    }

    private static class SeqIdGeneratingSubReaderWrapper extends SubReaderWrapper {
        private final Map<LeafReader, LeafReaderContext> ctxMap;
        private final long primaryTerm;

        SeqIdGeneratingSubReaderWrapper(Map<LeafReader, LeafReaderContext> ctxMap, long primaryTerm) {
            this.ctxMap = ctxMap;
            this.primaryTerm = primaryTerm;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            LeafReaderContext leafReaderContext = ctxMap.get(reader);
            final int docBase = leafReaderContext.docBase;
            return new SequentialStoredFieldsLeafReader(reader) {

                @Override
                protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                    return reader;
                }

                @Override
                public NumericDocValues getNumericDocValues(String field) throws IOException {
                    if (SeqNoFieldMapper.NAME.equals(field)) {
                        return new FakeNumericDocValues(maxDoc()) {
                            @Override
                            public long longValue() {
                                return docBase + docID;
                            }
                        };
                    } else if (SeqNoFieldMapper.PRIMARY_TERM_NAME.equals(field)) {
                        return new FakeNumericDocValues(maxDoc()) {
                            @Override
                            public long longValue() {
                                return primaryTerm;
                            }
                        };
                    } else if (VersionFieldMapper.NAME.equals(field)) {
                        return new FakeNumericDocValues(maxDoc()) {
                            @Override
                            public long longValue() {
                                return 1;
                            }
                        };
                    }
                    return super.getNumericDocValues(field);
                }

                @Override
                public CacheHelper getCoreCacheHelper() {
                    return reader.getCoreCacheHelper();
                }

                @Override
                public CacheHelper getReaderCacheHelper() {
                    return reader.getReaderCacheHelper();
                }

                @Override
                public Terms terms(String field) {
                    throw new UnsupportedOperationException("_source only indices can't be searched or filtered");
                }

                @Override
                public PointValues getPointValues(String field) {
                    throw new UnsupportedOperationException("_source only indices can't be searched or filtered");
                }
            };
        }
    }
}
