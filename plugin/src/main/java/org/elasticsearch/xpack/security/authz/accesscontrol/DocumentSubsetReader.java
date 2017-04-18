/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;

import java.io.IOException;

/**
 * A reader that only exposes documents via {@link #getLiveDocs()} that matches with the provided role query.
 */
public final class DocumentSubsetReader extends FilterLeafReader {

    public static DocumentSubsetDirectoryReader wrap(DirectoryReader in, BitsetFilterCache bitsetFilterCache,
            Query roleQuery) throws IOException {
        return new DocumentSubsetDirectoryReader(in, bitsetFilterCache, roleQuery);
    }

    static final class DocumentSubsetDirectoryReader extends FilterDirectoryReader {

        private final Query roleQuery;
        private final BitsetFilterCache bitsetFilterCache;

        DocumentSubsetDirectoryReader(final DirectoryReader in, final BitsetFilterCache bitsetFilterCache, final Query roleQuery)
                throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new DocumentSubsetReader(reader, bitsetFilterCache, roleQuery);
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToElastic(e);
                    }
                }
            });
            this.bitsetFilterCache = bitsetFilterCache;
            this.roleQuery = roleQuery;

            verifyNoOtherDocumentSubsetDirectoryReaderIsWrapped(in);
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new DocumentSubsetDirectoryReader(in, bitsetFilterCache, roleQuery);
        }

        private static void verifyNoOtherDocumentSubsetDirectoryReaderIsWrapped(DirectoryReader reader) {
            if (reader instanceof FilterDirectoryReader) {
                FilterDirectoryReader filterDirectoryReader = (FilterDirectoryReader) reader;
                if (filterDirectoryReader instanceof DocumentSubsetDirectoryReader) {
                    throw new IllegalArgumentException(LoggerMessageFormat.format("Can't wrap [{}] twice",
                            DocumentSubsetDirectoryReader.class));
                } else {
                    verifyNoOtherDocumentSubsetDirectoryReaderIsWrapped(filterDirectoryReader.getDelegate());
                }
            }
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private final BitSet roleQueryBits;
    private volatile int numDocs = -1;

    private DocumentSubsetReader(final LeafReader in, BitsetFilterCache bitsetFilterCache, final Query roleQuery) throws Exception {
        super(in);
        this.roleQueryBits = bitsetFilterCache.getBitSetProducer(roleQuery).getBitSet(in.getContext());
    }

    @Override
    public Bits getLiveDocs() {
        final Bits actualLiveDocs = in.getLiveDocs();
        if (roleQueryBits == null) {
            // If we would a <code>null</code> liveDocs then that would mean that no docs are marked as deleted,
            // but that isn't the case. No docs match with the role query and therefor all docs are marked as deleted
            return new Bits.MatchNoBits(in.maxDoc());
        } else if (actualLiveDocs == null) {
            return roleQueryBits;
        } else {
            // apply deletes when needed:
            return new Bits() {

                @Override
                public boolean get(int index) {
                    return roleQueryBits.get(index) && actualLiveDocs.get(index);
                }

                @Override
                public int length() {
                    return roleQueryBits.length();
                }
            };
        }
    }

    @Override
    public int numDocs() {
        // The reason the implement this method is that numDocs should be equal to the number of set bits in liveDocs. (would be weird
        // otherwise)
        // for the security DSL use case this get invoked in the QueryPhase class (in core ES) if match_all query is used as main query
        // and this is also invoked in tests.
        if (numDocs == -1) {
            final Bits liveDocs = in.getLiveDocs();
            if (roleQueryBits == null) {
                numDocs = 0;
            } else if (liveDocs == null) {
                numDocs = roleQueryBits.cardinality();
            } else {
                // this is slow, but necessary in order to be correct:
                try {
                    DocIdSetIterator iterator = new FilteredDocIdSetIterator(new BitSetIterator(roleQueryBits, roleQueryBits
                            .approximateCardinality())) {
                        @Override
                        protected boolean match(int doc) {
                            return liveDocs.get(doc);
                        }
                    };
                    int counter = 0;
                    for (int docId = iterator.nextDoc(); docId < DocIdSetIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
                        counter++;
                    }
                    numDocs = counter;
                } catch (IOException e) {
                    throw ExceptionsHelper.convertToElastic(e);
                }
            }
        }
        return numDocs;
    }

    @Override
    public boolean hasDeletions() {
        // we always return liveDocs and hide docs:
        return true;
    }

    // Don't delegate getCombinedCoreAndDeletesKey(), because we change the live docs here.

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    BitSet getRoleQueryBits() {
        return roleQueryBits;
    }

    Bits getWrappedLiveDocs() {
        return in.getLiveDocs();
    }

}
