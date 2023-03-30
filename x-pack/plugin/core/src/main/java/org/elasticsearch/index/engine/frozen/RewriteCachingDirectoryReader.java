/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.index.engine.frozen;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This special DirectoryReader is used to handle can_match requests against frozen indices.
 * It' caches all relevant point value data for every point value field ie. min/max packed values etc.
 * to hold enough information to rewrite a date range query and make a decisions if an index can match or not.
 * This allows frozen indices to be searched with wildcards in a very efficient way without opening a reader on them.
 */
final class RewriteCachingDirectoryReader extends DirectoryReader {

    RewriteCachingDirectoryReader(Directory directory, List<LeafReaderContext> segmentReaders, Comparator<LeafReader> leafSorter)
        throws IOException {
        super(directory, wrap(segmentReaders), leafSorter);
    }

    private static LeafReader[] wrap(List<LeafReaderContext> readers) throws IOException {
        LeafReader[] wrapped = new LeafReader[readers.size()];
        int i = 0;
        for (LeafReaderContext ctx : readers) {
            LeafReader wrap = new RewriteCachingLeafReader(ctx.reader());
            wrapped[i++] = wrap;
        }
        return wrapped;
    }

    @Override
    protected DirectoryReader doOpenIfChanged() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCurrent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexCommit getIndexCommit() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doClose() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // this reader is used for fast operations that don't require caching
        return null;
    }

    // except of a couple of selected methods everything else will
    // throw a UOE which causes a can_match phase to just move to the actual phase
    // later such that we never false exclude a shard if something else is used to rewrite.
    private static final class RewriteCachingLeafReader extends LeafReader {

        private final int maxDoc;
        private final int numDocs;
        private final Map<String, PointValues> pointValuesMap;
        private final FieldInfos fieldInfos;

        private RewriteCachingLeafReader(LeafReader original) throws IOException {
            this.maxDoc = original.maxDoc();
            this.numDocs = original.numDocs();
            fieldInfos = original.getFieldInfos();
            Map<String, PointValues> valuesMap = new HashMap<>();
            for (FieldInfo info : fieldInfos) {
                if (info.getPointIndexDimensionCount() != 0) {
                    PointValues pointValues = original.getPointValues(info.name);
                    if (pointValues != null) { // might not be in this reader
                        byte[] minPackedValue = pointValues.getMinPackedValue();
                        byte[] maxPackedValue = pointValues.getMaxPackedValue();
                        int numIndexDimensions = pointValues.getNumIndexDimensions();
                        int bytesPerDimension = pointValues.getBytesPerDimension();
                        int numDimensions = pointValues.getNumDimensions();
                        long size = pointValues.size();
                        int docCount = pointValues.getDocCount();
                        valuesMap.put(info.name, new PointValues() {
                            @Override
                            public PointTree getPointTree() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public byte[] getMinPackedValue() {
                                return minPackedValue;
                            }

                            @Override
                            public byte[] getMaxPackedValue() {
                                return maxPackedValue;
                            }

                            @Override
                            public int getNumDimensions() {
                                return numDimensions;
                            }

                            @Override
                            public int getNumIndexDimensions() {
                                return numIndexDimensions;
                            }

                            @Override
                            public int getBytesPerDimension() {
                                return bytesPerDimension;
                            }

                            @Override
                            public long size() {
                                return size;
                            }

                            @Override
                            public int getDocCount() {
                                return docCount;
                            }
                        });
                    }
                }
            }
            pointValuesMap = valuesMap;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Terms terms(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumericDocValues getNormValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public TopDocs searchNearestVectors(String field, float[] target, int k, Bits acceptDocs, int visitedLimit) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public TopDocs searchNearestVectors(String field, byte[] target, int k, Bits acceptDocs, int visitedLimit) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldInfos getFieldInfos() {
            return fieldInfos;
        }

        @Override
        public Bits getLiveDocs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PointValues getPointValues(String field) {
            return pointValuesMap.get(field);
        }

        @Override
        public void checkIntegrity() {}

        @Override
        public LeafMetaData getMetaData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Fields getTermVectors(int docId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TermVectors termVectors() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public StoredFields storedFields() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public int maxDoc() {
            return maxDoc;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doClose() {}

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }
}
