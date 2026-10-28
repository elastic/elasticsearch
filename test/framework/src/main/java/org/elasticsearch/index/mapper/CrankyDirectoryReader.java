/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class CrankyDirectoryReader extends FilterDirectoryReader {
    public CrankyDirectoryReader(DirectoryReader in) throws IOException {
        super(in, new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader leafReader) {
                return new CrankyLeafReader(leafReader);
            }
        });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader directoryReader) {
        return directoryReader;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    private static class CrankyLeafReader extends FilterLeafReader {
        protected CrankyLeafReader(LeafReader in) {
            super(in);
        }

        private void beCranky() throws IOException {
            if (ESTestCase.random().nextInt(20) == 0) {
                throw new IOException("cranky reader");
            }
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            beCranky();
            return super.getSortedSetDocValues(field);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            beCranky();
            return super.getSortedDocValues(field);
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            beCranky();
            return super.getBinaryDocValues(field);
        }

        @Override
        public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
            beCranky();
            return super.getDocValuesSkipper(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            beCranky();
            return super.getSortedNumericDocValues(field);
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            beCranky();
            return super.getNumericDocValues(field);
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            beCranky();
            return super.getByteVectorValues(field);
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            beCranky();
            return super.getFloatVectorValues(field);
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            beCranky();
            return super.getNormValues(field);
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            beCranky();
            return super.getPointValues(field);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

    }
}
