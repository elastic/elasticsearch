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

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.*;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.search.EmptyScorer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Holds all information on a particular term in a field.
 * */
public class IndexFieldTerm implements Iterable<TermPosition> {

    // The posting list for this term. Is null if the term or field does not
    // exist.
    PostingsEnum postings;

    // Stores if positions, offsets and payloads are requested.
    private final int flags;

    private final String fieldName;

    private final String term;

    private final PositionIterator iterator;

    // for lucene calls
    private final Term identifier;

    private final TermStatistics termStats;

    static private EmptyScorer EMPTY_SCORER = new EmptyScorer(null);

    // get the document frequency of the term
    public long df() throws IOException {
        return termStats.docFreq();
    }

    // get the total term frequency of the term, that is, how often does the
    // term appear in any document?
    public long ttf() throws IOException {
        return termStats.totalTermFreq();
    }

    // when the reader changes, we have to get the posting list for this term
    // and reader
    void setNextReader(LeafReader reader) {
        try {
            // Get the posting list for a specific term.

            if (!shouldRetrieveFrequenciesOnly()) {
                postings = getPostings(getLucenePositionsFlags(flags), reader);
            }
            
            if (postings == null) {
                postings = getPostings(getLuceneFrequencyFlag(flags), reader);
                if (postings != null) {
                    final PostingsEnum p = postings;
                    postings = new PostingsEnum() {

                        @Override
                        public int freq() throws IOException {
                            return p.freq();
                        }

                        @Override
                        public int nextPosition() throws IOException {
                            return -1;
                        }

                        @Override
                        public int startOffset() throws IOException {
                            return -1;
                        }

                        @Override
                        public int endOffset() throws IOException {
                            return -1;
                        }

                        @Override
                        public BytesRef getPayload() throws IOException {
                            return null;
                        }

                        @Override
                        public int docID() {
                            return p.docID();
                        }

                        @Override
                        public int nextDoc() throws IOException {
                            return p.nextDoc();
                        }

                        @Override
                        public int advance(int target) throws IOException {
                            return p.advance(target);
                        }

                        @Override
                        public long cost() {
                            return p.cost();
                        }
                    };
                }
            }

            if (postings == null) {
                postings = EMPTY_SCORER;
            }

        } catch (IOException e) {
            throw new ElasticsearchException("Unable to get posting list for field " + fieldName + " and term " + term, e);
        }

    }

    private boolean shouldRetrieveFrequenciesOnly() {
        return (flags & ~IndexLookup.FLAG_FREQUENCIES) == 0;
    }

    private int getLuceneFrequencyFlag(int flags) {
        return (flags & IndexLookup.FLAG_FREQUENCIES) > 0 ? PostingsEnum.FREQS : PostingsEnum.NONE;
    }

    private int getLucenePositionsFlags(int flags) {
        int lucenePositionsFlags = PostingsEnum.POSITIONS;
        lucenePositionsFlags |= (flags & IndexLookup.FLAG_PAYLOADS) > 0 ? PostingsEnum.PAYLOADS : 0x0;
        lucenePositionsFlags |= (flags & IndexLookup.FLAG_OFFSETS) > 0 ? PostingsEnum.OFFSETS : 0x0;
        return lucenePositionsFlags;
    }

    private PostingsEnum getPostings(int luceneFlags, LeafReader reader) throws IOException {
        assert identifier.field() != null;
        assert identifier.bytes() != null;
        final Fields fields = reader.fields();
        PostingsEnum newPostings = null;
        if (fields != null) {
            final Terms terms = fields.terms(identifier.field());
            if (terms != null) {
                TermsEnum termsEnum = terms.iterator(null);
                if (termsEnum.seekExact(identifier.bytes())) {
                    newPostings = termsEnum.postings(reader.getLiveDocs(), postings, luceneFlags);
                }
            }
        }
        return newPostings;
    }

    private int freq = 0;

    public void setNextDoc(int docId) {
        assert (postings != null);
        try {
            // we try to advance to the current document.
            int currentDocPos = postings.docID();
            if (currentDocPos < docId) {
                currentDocPos = postings.advance(docId);
            }
            if (currentDocPos == docId) {
                freq = postings.freq();
            } else {
                freq = 0;
            }
            iterator.nextDoc();
        } catch (IOException e) {
            throw new ElasticsearchException("While trying to initialize term positions in IndexFieldTerm.setNextDoc() ", e);
        }
    }

    public IndexFieldTerm(String term, String fieldName, IndexLookup indexLookup, int flags) {
        assert fieldName != null;
        this.fieldName = fieldName;
        assert term != null;
        this.term = term;
        assert indexLookup != null;
        identifier = new Term(fieldName, (String) term);
        this.flags = flags;
        boolean doRecord = ((flags & IndexLookup.FLAG_CACHE) > 0);
        if (withPositions()) {
            if (!doRecord) {
                iterator = new PositionIterator(this);
            } else {
                iterator = new CachedPositionIterator(this);
            }
        } else {
            iterator = new PositionIterator(this);
        }
        setNextReader(indexLookup.getReader());
        setNextDoc(indexLookup.getDocId());
        try {
            termStats = indexLookup.getIndexSearcher().termStatistics(identifier,
                    TermContext.build(indexLookup.getReaderContext(), identifier));
        } catch (IOException e) {
            throw new ElasticsearchException("Cannot get term statistics: ", e);
        }
    }

    private boolean withPositions() {
        return shouldRetrievePositions() || shouldRetrieveOffsets() || shouldRetrievePayloads();
    }

    protected boolean shouldRetrievePositions() {
        return (flags & IndexLookup.FLAG_POSITIONS) > 0;
    }

    protected boolean shouldRetrieveOffsets() {
        return (flags & IndexLookup.FLAG_OFFSETS) > 0;
    }

    protected boolean shouldRetrievePayloads() {
        return (flags & IndexLookup.FLAG_PAYLOADS) > 0;
    }

    public int tf() throws IOException {
        return freq;
    }

    @Override
    public Iterator<TermPosition> iterator() {
        return iterator.reset();
    }

    /*
     * A user might decide inside a script to call get with _POSITIONS and then
     * a second time with _PAYLOADS. If the positions were recorded but the
     * payloads were not, the user will not have access to them. Therfore, throw
     * exception here explaining how to call get().
     */
    public void validateFlags(int flags2) {
        if ((this.flags & flags2) < flags2) {
            throw new ElasticsearchException("You must call get with all required flags! Instead of " + getCalledStatement(flags2)
                    + "call " + getCallStatement(flags2 | this.flags) + " once");
        }
    }

    private String getCalledStatement(int flags2) {
        String calledFlagsCall1 = getFlagsString(flags);
        String calledFlagsCall2 = getFlagsString(flags2);
        String callStatement1 = getCallStatement(calledFlagsCall1);
        String callStatement2 = getCallStatement(calledFlagsCall2);
        return " " + callStatement1 + " and " + callStatement2 + " ";
    }

    private String getCallStatement(String calledFlags) {
        return "_index['" + this.fieldName + "'].get('" + this.term + "', " + calledFlags + ")";
    }

    private String getFlagsString(int flags2) {
        String flagsString = null;
        if ((flags2 & IndexLookup.FLAG_FREQUENCIES) != 0) {
            flagsString = anddToFlagsString(flagsString, "_FREQUENCIES");
        }
        if ((flags2 & IndexLookup.FLAG_POSITIONS) != 0) {
            flagsString = anddToFlagsString(flagsString, "_POSITIONS");
        }
        if ((flags2 & IndexLookup.FLAG_OFFSETS) != 0) {
            flagsString = anddToFlagsString(flagsString, "_OFFSETS");
        }
        if ((flags2 & IndexLookup.FLAG_PAYLOADS) != 0) {
            flagsString = anddToFlagsString(flagsString, "_PAYLOADS");
        }
        if ((flags2 & IndexLookup.FLAG_CACHE) != 0) {
            flagsString = anddToFlagsString(flagsString, "_CACHE");
        }
        return flagsString;
    }

    private String anddToFlagsString(String flagsString, String flag) {
        if (flagsString != null) {
            flagsString += " | ";
        } else {
            flagsString = "";
        }
        flagsString += flag;
        return flagsString;
    }

    private String getCallStatement(int flags2) {
        String calledFlags = getFlagsString(flags2);
        String callStatement = getCallStatement(calledFlags);
        return " " + callStatement + " ";

    }

}
