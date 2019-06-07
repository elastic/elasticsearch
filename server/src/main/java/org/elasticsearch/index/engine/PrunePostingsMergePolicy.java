/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * This merge policy drops id field postings for all delete documents this can be
 * useful to guarantee consistent update performance even if a large number of deleted / updated documents
 * are retained. Merging postings away is efficient since lucene visits postings term by term and
 * with the original live-docs being available we are adding a negotiable overhead such that we can
 * prune soft-deletes by default. Yet, using this merge policy will cause loosing all search capabilities on top of
 * soft deleted documents independent of the retention policy. Note, in order for this merge policy to be effective it needs to be added
 * before the {@link org.apache.lucene.index.SoftDeletesRetentionMergePolicy} because otherwise only documents that are deleted / removed
 * anyways will be pruned.
 */
final class PrunePostingsMergePolicy extends OneMergeWrappingMergePolicy {

    PrunePostingsMergePolicy(MergePolicy in, String idField, Supplier<Query> retentionQuery) {
        super(in, toWrap -> new OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrapReader(wrapped, idField, retentionQuery);
            }
        });
    }

    private static CodecReader wrapReader(CodecReader reader, String idField, Supplier<Query> retentionQuery) throws IOException {
        if (reader.getLiveDocs() == null) {
            return reader; // no deleted docs - we are good!
        }
        final Tuple<Bits, Boolean> liveDocsAndFullyDeletedSegment = applyRetentionQuery(reader, retentionQuery.get());
        final Bits liveDocs = liveDocsAndFullyDeletedSegment.v1();
        final boolean fullyDeletedSegment = liveDocsAndFullyDeletedSegment.v2();
        return new FilterCodecReader(reader) {

            @Override
            public FieldsProducer getPostingsReader() {
                FieldsProducer postingsReader = super.getPostingsReader();
                if (postingsReader == null) {
                    return null;
                }
                return new FieldsProducer() {
                    @Override
                    public void close() throws IOException {
                        postingsReader.close();
                    }

                    @Override
                    public void checkIntegrity() throws IOException {
                        postingsReader.checkIntegrity();
                    }

                    @Override
                    public Iterator<String> iterator() {
                        return postingsReader.iterator();
                    }

                    @Override
                    public Terms terms(String field) throws IOException {
                        Terms in = postingsReader.terms(field);
                        if (idField.equals(field) && in != null) {
                            return new FilterLeafReader.FilterTerms(in) {
                                @Override
                                public TermsEnum iterator() throws IOException {
                                    TermsEnum iterator = super.iterator();
                                    return new FilteredTermsEnum(iterator, false) {
                                        private PostingsEnum internal;

                                        @Override
                                        protected AcceptStatus accept(BytesRef term) throws IOException {
                                            if (fullyDeletedSegment) {
                                                return AcceptStatus.END; // short-cut this if we don't match anything
                                            }
                                            internal = postings(internal, PostingsEnum.NONE);
                                            if (internal.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                                return AcceptStatus.YES;
                                            }
                                            return AcceptStatus.NO;
                                        }

                                        @Override
                                        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                                            if (reuse instanceof OnlyLiveDocsPostingsEnum) {
                                                OnlyLiveDocsPostingsEnum reuseInstance = (OnlyLiveDocsPostingsEnum) reuse;
                                                reuseInstance.reset(super.postings(reuseInstance.in, flags));
                                                return reuseInstance;
                                            }
                                            return new OnlyLiveDocsPostingsEnum(super.postings(null, flags), liveDocs);
                                        }

                                        @Override
                                        public ImpactsEnum impacts(int flags) throws IOException {
                                            throw new UnsupportedOperationException();
                                        }
                                    };
                                }
                            };
                        } else {
                            return in;
                        }
                    }

                    @Override
                    public int size() {
                        return postingsReader.size();
                    }

                    @Override
                    public long ramBytesUsed() {
                        return postingsReader.ramBytesUsed();
                    }
                };
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    private static final class OnlyLiveDocsPostingsEnum extends PostingsEnum {

        private final Bits liveDocs;
        private PostingsEnum in;

        OnlyLiveDocsPostingsEnum(PostingsEnum in, Bits liveDocs) {
            this.liveDocs = liveDocs;
            reset(in);
        }

        void reset(PostingsEnum in) {
            this.in = in;
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int docId;
            do {
                docId = in.nextDoc();
            } while (docId != DocIdSetIterator.NO_MORE_DOCS && liveDocs.get(docId) == false);
            return docId;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public int freq() throws IOException {
            return in.freq();
        }

        @Override
        public int nextPosition() throws IOException {
            return in.nextPosition();
        }

        @Override
        public int startOffset() throws IOException {
            return in.startOffset();
        }

        @Override
        public int endOffset() throws IOException {
            return in.endOffset();
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return in.getPayload();
        }
    }

    private static Tuple<Bits, Boolean> applyRetentionQuery(CodecReader reader, Query retentionQuery) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(new FilterCodecReader(reader) {
            private final Bits liveDocs = reader.getLiveDocs();

            @Override
            public CacheHelper getCoreCacheHelper() {
                return reader.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null; // we are altering live docs
            }

            @Override
            public Bits getLiveDocs() {
                return new Bits() {
                    @Override
                    public boolean get(int index) {
                        return liveDocs.get(index) == false;
                    }

                    @Override
                    public int length() {
                        return liveDocs.length();
                    }
                };
            }

            @Override
            public int numDocs() {
                return reader.maxDoc() - reader.numDocs();
            }
        });
        searcher.setQueryCache(null);
        final Weight weight = searcher.createWeight(searcher.rewrite(retentionQuery), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        final Scorer scorer = weight.scorer(reader.getContext());
        if (scorer == null) {
            return Tuple.tuple(reader.getLiveDocs(), reader.numDocs() == 0);
        } else {
            final FixedBitSet liveDocs = FixedBitSet.copyOf(reader.getLiveDocs());
            final DocIdSetIterator iterator = scorer.iterator();
            while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                liveDocs.set(iterator.docID());
            }
            return Tuple.tuple(liveDocs, false);
        }
    }
}
