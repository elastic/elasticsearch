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
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

/**
 * This merge policy drops postings for all deleted documents which is useful to guarantee
 * consistent search and update performance even if a large number of deleted / updated documents
 * are retained. Merging postings away is efficient since lucene visits postings term by term and
 * with the original live-docs being available we are adding a negotiable overhead such that we can
 * prune soft-deletes by default. Yet, using this merge policy will cause loosing all search capabilities on top of
 * soft deleted documents independent of the retention policy. Note, in order for this merge policy to be effective it needs to be added
 * before the {@link org.apache.lucene.index.SoftDeletesRetentionMergePolicy} because otherwise only documents that are deleted / removed
 * anyways will be pruned.
 */
final class PrunePostingsMergePolicy extends OneMergeWrappingMergePolicy {

    PrunePostingsMergePolicy(MergePolicy in) {
        super(in, toWrap -> new OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrapReader(wrapped);
            }
        });
        assert in instanceof SoftDeletesRetentionMergePolicy == false :
            "wrapped merge policy should not be a SoftDeletesRetentionMergePolicy";
    }

    private static int skipDeletedDocs(DocIdSetIterator iterator, Bits liveDocs) throws IOException {
        int docId;
        do {
            docId = iterator.nextDoc();
        } while (docId != DocIdSetIterator.NO_MORE_DOCS && liveDocs.get(docId) == false);
        return docId;
    }

    private static CodecReader wrapReader(CodecReader reader) {
        Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
            return reader; // no deleted docs - we are good!
        }
        return new FilterCodecReader(reader) {

            @Override
            public NormsProducer getNormsReader() {
                NormsProducer normsReader = reader.getNormsReader();
                if (normsReader == null) {
                    return null;
                }
                return new NormsProducer() {
                    @Override
                    public NumericDocValues getNorms(FieldInfo field) throws IOException {
                        return new FilterNumericDocValues(normsReader.getNorms(field)) {
                            @Override
                            public int nextDoc() throws IOException {
                                return skipDeletedDocs(in, liveDocs);
                            }

                            @Override
                            public int advance(int target) throws IOException {
                                throw new UnsupportedEncodingException();
                            }

                            @Override
                            public boolean advanceExact(int target) throws IOException {
                                throw new UnsupportedEncodingException();
                            }
                        };
                    }

                    @Override
                    public void checkIntegrity() throws IOException {
                        normsReader.checkIntegrity();;
                    }

                    @Override
                    public void close() throws IOException {
                        normsReader.close();
                    }

                    @Override
                    public long ramBytesUsed() {
                        return normsReader.ramBytesUsed();
                    }
                };
            }

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
                        Terms terms = postingsReader.terms(field);
                        if (terms == null) {
                            return null;
                        }
                        return new FilterLeafReader.FilterTerms(terms) {
                            @Override
                            public TermsEnum iterator() throws IOException {
                                TermsEnum iterator = super.iterator();
                                return new FilteredTermsEnum(iterator) {
                                    @Override
                                    protected AcceptStatus accept(BytesRef term) {
                                        return AcceptStatus.YES;
                                    }

                                    @Override
                                    public BytesRef next() throws IOException {
                                        return iterator.next();
                                    }

                                    @Override
                                    public boolean seekExact(BytesRef text) throws IOException {
                                        return iterator.seekExact(text);
                                    }

                                    @Override
                                    public SeekStatus seekCeil(BytesRef text) throws IOException {
                                        return iterator.seekCeil(text);
                                    }

                                    @Override
                                    public void seekExact(long ord) throws IOException {
                                        iterator.seekExact(ord);
                                    }

                                    @Override
                                    public void seekExact(BytesRef term, TermState state) throws IOException {
                                        iterator.seekExact(term, state);
                                    }

                                    @Override
                                    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                                        return new FilterLeafReader.FilterPostingsEnum(super.postings(reuse, flags)) {
                                            @Override
                                            public int nextDoc() throws IOException {
                                                return skipDeletedDocs(in, liveDocs);
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

            @Override
            public TermVectorsReader getTermVectorsReader() {
                TermVectorsReader termVectorsReader = super.getTermVectorsReader();
                if (termVectorsReader == null) {
                    return null;
                }
                return new FilteredTermVectorsReader(liveDocs, termVectorsReader);
            }
        };
    }

    private static class FilteredTermVectorsReader extends TermVectorsReader {
        private final Bits liveDocs;
        private final TermVectorsReader termVectorsReader;

        public FilteredTermVectorsReader(Bits liveDocs, TermVectorsReader termVectorsReader) {
            this.liveDocs = liveDocs;
            this.termVectorsReader = termVectorsReader;
        }

        @Override
        public Fields get(int doc) throws IOException {
            if (liveDocs.get(doc)) { // we can drop the entire TV for a deleted document.
                return termVectorsReader.get(doc);
            } else {
                return null;
            }
        }

        @Override
        public void checkIntegrity() throws IOException {
            termVectorsReader.checkIntegrity();
        }

        @Override
        public TermVectorsReader clone() {
            return new FilteredTermVectorsReader(liveDocs, termVectorsReader.clone());
        }

        @Override
        public void close() throws IOException {
            termVectorsReader.close();
        }

        @Override
        public long ramBytesUsed() {
            return termVectorsReader.ramBytesUsed();
        }

        @Override
        public TermVectorsReader getMergeInstance() throws IOException {
            return new FilteredTermVectorsReader(liveDocs, termVectorsReader.getMergeInstance());
        }
    }
}
