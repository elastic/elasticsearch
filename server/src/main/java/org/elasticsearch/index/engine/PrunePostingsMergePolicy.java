/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Iterator;

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

    PrunePostingsMergePolicy(MergePolicy in, String idField) {
        super(in, toWrap -> new OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrapReader(wrapped, idField);
            }
        });
    }

    private static CodecReader wrapReader(CodecReader reader, String idField) {
        Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
            return reader; // no deleted docs - we are good!
        }
        final boolean fullyDeletedSegment = reader.numDocs() == 0;
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
                                            if (reuse instanceof OnlyLiveDocsPostingsEnum reuseInstance) {
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
}
