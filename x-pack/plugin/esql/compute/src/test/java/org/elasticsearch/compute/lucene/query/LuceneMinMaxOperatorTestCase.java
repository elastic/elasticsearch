/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.compute.test.SourceOperatorTestCase;

import java.io.IOException;

abstract class LuceneMinMaxOperatorTestCase extends SourceOperatorTestCase {

    /**
     * Wraps the given reader so that any call to {@link LeafReader#getSortedNumericDocValues} throws
     * {@link AssertionError}. Use this to verify that a fast path avoids per-document iteration entirely.
     */
    protected static IndexReader wrapWithNoDocValuesIteration(IndexReader reader) throws IOException {
        return new FilterDirectoryReader((DirectoryReader) reader, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader leaf) {
                return new FilterLeafReader(leaf) {
                    @Override
                    public SortedNumericDocValues getSortedNumericDocValues(String field) {
                        throw new AssertionError(
                            "getSortedNumericDocValues called - per-doc iteration should not occur on the skipper fast path"
                        );
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return in.getCoreCacheHelper();
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return in.getReaderCacheHelper();
                    }
                };
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
                return in;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };
    }
}
