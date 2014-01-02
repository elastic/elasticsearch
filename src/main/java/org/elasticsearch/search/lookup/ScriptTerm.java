/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.lucene.search.EmptyScorer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Holds all information on a particular term in a field.
 * */
public class ScriptTerm implements Iterable<TermPosition> {

    // The posting list for this term. Is null if the term or field does not
    // exist. Can be DocsEnum or DocsAndPositionsEnum.
    DocsEnum docsEnum;

    // Stores if positions, offsets and payloads are requested.
    private final int flags;

    private final String fieldName;

    private final String term;

    private final PositionIterator iterator;

    // for lucene calls
    private final Term identifier;

    private final TermStatistics termStats;

    static private EmptyScorer EMPTY_DOCS_ENUM = new EmptyScorer(null);

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
    void setNextReader(AtomicReader reader) {
        try {
            // Get the posting list for a specific term. Depending on the flags,
            // this
            // will either get a DocsEnum or a DocsAndPositionsEnum if
            // available.

            // get lucene frequency flag
            int luceneFrequencyFlag = getLuceneFrequencyFlag(flags);
            if (shouldRetrieveFrequenciesOnly()) {
                docsEnum = getOnlyDocsEnum(luceneFrequencyFlag, reader);
            } else {
                int lucenePositionsFlags = getLucenePositionsFlags(flags);
                docsEnum = getDocsAndPosEnum(lucenePositionsFlags, reader);
                if (docsEnum == null) {// no pos available
                    docsEnum = getOnlyDocsEnum(luceneFrequencyFlag, reader);
                }
            }
        } catch (IOException e) {
            throw new ElasticSearchException("Unable to get posting list for field " + fieldName + " and term " + term, e);
        }

    }

    private boolean shouldRetrieveFrequenciesOnly() {
        return (flags & ~ShardTermsLookup.FLAG_FREQUENCIES) == 0;
    }

    private int getLuceneFrequencyFlag(int flags) {
        return (flags & ShardTermsLookup.FLAG_FREQUENCIES) > 0 ? DocsEnum.FLAG_FREQS : DocsEnum.FLAG_NONE;
    }

    private int getLucenePositionsFlags(int flags) {
        int lucenePositionsFlags = (flags & ShardTermsLookup.FLAG_PAYLOADS) > 0 ? DocsAndPositionsEnum.FLAG_PAYLOADS : 0x0;
        lucenePositionsFlags |= (flags & ShardTermsLookup.FLAG_OFFSETS) > 0 ? DocsAndPositionsEnum.FLAG_OFFSETS : 0x0;
        return lucenePositionsFlags;
    }

    // get the DocsAndPositionsEnum from the reader.
    private DocsEnum getDocsAndPosEnum(int luceneFlags, AtomicReader reader) throws IOException {
        assert identifier.field() != null;
        assert identifier.bytes() != null;
        final Fields fields = reader.fields();
        DocsEnum newDocsEnum = null;
        if (fields != null) {
            final Terms terms = fields.terms(identifier.field());
            if (terms != null) {
                if (terms.hasPositions()) {
                    final TermsEnum termsEnum = terms.iterator(null);
                    if (termsEnum.seekExact(identifier.bytes())) {
                        newDocsEnum = termsEnum.docsAndPositions(reader.getLiveDocs(),
                                docsEnum instanceof DocsAndPositionsEnum ? (DocsAndPositionsEnum) docsEnum : null, luceneFlags);
                    }
                }
            }
        }
        return newDocsEnum;
    }

    // get the DocsEnum from the reader.
    private DocsEnum getOnlyDocsEnum(int luceneFlags, AtomicReader reader) throws IOException {
        assert identifier.field() != null;
        assert identifier.bytes() != null;
        final Fields fields = reader.fields();
        DocsEnum newDocsEnum = null;
        if (fields != null) {
            final Terms terms = fields.terms(identifier.field());
            if (terms != null) {
                TermsEnum termsEnum = terms.iterator(null);
                if (termsEnum.seekExact(identifier.bytes())) {
                    newDocsEnum = termsEnum.docs(reader.getLiveDocs(), docsEnum, luceneFlags);
                }
            }
        }
        if (newDocsEnum == null) {
            newDocsEnum = EMPTY_DOCS_ENUM;
        }
        return newDocsEnum;
    }

    private int freq = 0;

    public void setNextDoc(int docId) {
        assert (docsEnum != null);
        try {
            // we try to advance to the current document.
            int currentDocPos = docsEnum.docID();
            if (currentDocPos < docId) {
                currentDocPos = docsEnum.advance(docId);
            }
            if (currentDocPos == docId) {
                freq = docsEnum.freq();
            } else {
                freq = 0;
            }
            iterator.nextDoc();
        } catch (IOException e) {
            throw new ElasticSearchException("While trying to initialize term positions in ScriptTerm.setNextDoc() ", e);
        }
    }

    public ScriptTerm(String term, String fieldName, ShardTermsLookup shardTermsLookup, int flags) {
        assert fieldName != null;
        this.fieldName = fieldName;
        assert term != null;
        this.term = term;
        assert shardTermsLookup != null;
        identifier = new Term(fieldName, (String) term);
        this.flags = flags;
        boolean doRecord = ((flags & ShardTermsLookup.FLAG_CACHE) > 0);
        if (withPositions()) {
            if (!doRecord) {
                iterator = new PositionIterator(this);
            } else {
                iterator = new CachedPositionIterator(this);
            }
        } else {
            iterator = new PositionIterator(this);
        }
        setNextReader(shardTermsLookup.getReader());
        setNextDoc(shardTermsLookup.getDocId());
        try {
            termStats = shardTermsLookup.getIndexSearcher().termStatistics(identifier,
                    TermContext.build(shardTermsLookup.getReaderContext(), identifier));
        } catch (IOException e) {
            throw new ElasticSearchException("Cannot get term statistics: ", e);
        }
    }

    private boolean withPositions() {
        return shouldRetrievePositions() || shouldRetrieveOffsets() || shouldRetrievePayloads();
    }

    protected boolean shouldRetrievePositions() {
        return (flags & ShardTermsLookup.FLAG_POSITIONS) > 0;
    }

    protected boolean shouldRetrieveOffsets() {
        return (flags & ShardTermsLookup.FLAG_OFFSETS) > 0;
    }

    protected boolean shouldRetrievePayloads() {
        return (flags & ShardTermsLookup.FLAG_PAYLOADS) > 0;
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
            throw new ElasticSearchException("You must call get with all required flags! Instead of " + getCalledStatement(flags2)
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
        return "_shard['" + this.fieldName + "'].get('" + this.term + "', " + calledFlags + ")";
    }

    private String getFlagsString(int flags2) {
        String flagsString = null;
        if ((flags2 & ShardTermsLookup.FLAG_FREQUENCIES) != 0) {
            flagsString = anddToFlagsString(flagsString, "_FREQUENCIES");
        }
        if ((flags2 & ShardTermsLookup.FLAG_POSITIONS) != 0) {
            flagsString = anddToFlagsString(flagsString, "_POSITIONS");
        }
        if ((flags2 & ShardTermsLookup.FLAG_OFFSETS) != 0) {
            flagsString = anddToFlagsString(flagsString, "_OFFSETS");
        }
        if ((flags2 & ShardTermsLookup.FLAG_PAYLOADS) != 0) {
            flagsString = anddToFlagsString(flagsString, "_PAYLOADS");
        }
        if ((flags2 & ShardTermsLookup.FLAG_CACHE) != 0) {
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
