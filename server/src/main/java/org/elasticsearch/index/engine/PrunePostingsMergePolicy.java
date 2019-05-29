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
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * This merge policy drops id field postings for all delete documents as well as all docs within the provided retention policy this can be
 * useful to guarantee consistent search and update performance even if a large number of deleted / updated documents
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
                FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(idField);
                if (fieldInfo != null
                    && (fieldInfo.hasNorms() || fieldInfo.hasVectors() || fieldInfo.getDocValuesType() != DocValuesType.NONE)) {
                    // TODO can we guarantee this?
                    throw new IllegalStateException(idField + " must not have norms, vectors or doc-values");
                }
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrapReader(wrapped, idField, retentionQuery);
            }
        });
    }

    private static int skipDeletedDocs(DocIdSetIterator iterator, Bits liveDocs) throws IOException {
        int docId;
        do {
            docId = iterator.nextDoc();
        } while (docId != DocIdSetIterator.NO_MORE_DOCS && liveDocs.get(docId) == false);
        return docId;
    }

    private static Bits processLiveDocs(Bits liveDocs, Supplier<Query> retentionQuery, CodecReader reader) throws IOException {
        Scorer scorer = getScorer(retentionQuery.get(), reader);
        if (scorer != null) {
            BitSet retentionDocs = BitSet.of(scorer.iterator(), reader.maxDoc());
            if (liveDocs == null) {
                return retentionDocs;
            }
            return new Bits() {
                @Override
                public boolean get(int index) {
                    return liveDocs.get(index) && retentionDocs.get(index);
                }

                @Override
                public int length() {
                    return reader.maxDoc();
                }
            };
        } else {
            return new Bits.MatchNoBits(reader.maxDoc());
        }
    }

    private static Scorer getScorer(Query query, CodecReader reader) throws IOException {
        IndexSearcher s = new IndexSearcher(reader);
        s.setQueryCache(null);
        Weight weight = s.createWeight(s.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        return weight.scorer(reader.getContext());
    }


    private static CodecReader wrapReader(CodecReader reader, String idField, Supplier<Query> retentionQuery) throws IOException {
        Bits liveDocs = processLiveDocs(reader.getLiveDocs(), retentionQuery, reader);
        if (liveDocs == null) {
            return reader; // no deleted docs - we are good!
        }
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
                        if (in == null || idField.equals(field) == false) {
                            return in;
                        }
                        return new FilterLeafReader.FilterTerms(in) {
                            @Override
                            public TermsEnum iterator() throws IOException {
                                TermsEnum iterator = super.iterator();
                                return new FilteredTermsEnum(iterator) {
                                    private PostingsEnum internal;
                                    @Override
                                    protected AcceptStatus accept(BytesRef term) {
                                        return AcceptStatus.YES;
                                    }

                                    @Override
                                    public BytesRef next() throws IOException {
                                        if (liveDocs instanceof Bits.MatchNoBits) {
                                            return null; // short-cut this if we don't match anything
                                        }
                                        BytesRef ref;
                                        while ((ref = iterator.next()) != null) {
                                            internal = super.postings(internal, PostingsEnum.NONE);
                                            if (skipDeletedDocs(internal, liveDocs) != DocIdSetIterator.NO_MORE_DOCS) {
                                                break;
                                            }
                                        }
                                        return ref;
                                    }

                                    @Override
                                    public boolean seekExact(BytesRef text) {
                                       throw new UnsupportedOperationException("This TermsEnum can not seek");
                                    }

                                    @Override
                                    public SeekStatus seekCeil(BytesRef text) {
                                        throw new UnsupportedOperationException("This TermsEnum can not seek");
                                    }

                                    @Override
                                    public void seekExact(long ord) {
                                        throw new UnsupportedOperationException("This TermsEnum can not seek");
                                    }

                                    @Override
                                    public void seekExact(BytesRef term, TermState state) {
                                        throw new UnsupportedOperationException("This TermsEnum can not seek");
                                    }

                                    @Override
                                    public PostingsEnum postings(PostingsEnum reuse, int flags) {
                                        assert internal != null;
                                        return new FilterLeafReader.FilterPostingsEnum(internal) {

                                            @Override
                                            public int nextDoc() throws IOException {
                                                int currentDocId = in.docID();
                                                if (currentDocId != NO_MORE_DOCS) {
                                                    skipDeletedDocs(in, liveDocs);
                                                }
                                                return currentDocId;
                                            }

                                            @Override
                                            public int advance(int target) throws IOException {
                                                throw new UnsupportedEncodingException();
                                            }
                                        };
                                    }

                                    @Override
                                    public ImpactsEnum impacts(int flags) throws IOException {
                                        throw new UnsupportedEncodingException();
                                    }


                                };
                            }
                        };
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

}
