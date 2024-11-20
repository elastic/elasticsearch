/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.termvectors;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.termvectors.TermVectorsRequest.Flag;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

// package only - this is an internal class!
final class TermVectorsWriter {
    final List<String> fields = new ArrayList<>();
    final List<Long> fieldOffset = new ArrayList<>();
    final BytesStreamOutput output = new BytesStreamOutput(1); // can we somehow
    // predict the
    // size here?
    private static final String HEADER = "TV";
    private static final int CURRENT_VERSION = -1;
    final TermVectorsResponse response;

    TermVectorsWriter(TermVectorsResponse termVectorsResponse) {
        response = termVectorsResponse;
    }

    void setFields(
        Fields termVectorsByField,
        Set<String> selectedFields,
        EnumSet<Flag> flags,
        Fields topLevelFields,
        @Nullable TermVectorsFilter termVectorsFilter
    ) throws IOException {
        int numFieldsWritten = 0;
        PostingsEnum docsAndPosEnum = null;
        PostingsEnum docsEnum = null;
        boolean hasScores = termVectorsFilter != null;

        for (String field : termVectorsByField) {
            if (selectedFields != null && selectedFields.contains(field) == false) {
                continue;
            }

            Terms fieldTermVector = termVectorsByField.terms(field);
            Terms topLevelTerms = topLevelFields.terms(field);

            // if no terms found, take the retrieved term vector fields for stats
            if (fieldTermVector == null) {
                fieldTermVector = EMPTY_TERMS;
            }

            if (topLevelTerms == null) {
                topLevelTerms = EMPTY_TERMS;
            }

            TermsEnum topLevelIterator = topLevelTerms.iterator();
            boolean positions = flags.contains(Flag.Positions) && fieldTermVector.hasPositions();
            boolean offsets = flags.contains(Flag.Offsets) && fieldTermVector.hasOffsets();
            boolean payloads = flags.contains(Flag.Payloads) && fieldTermVector.hasPayloads();

            long termsSize = fieldTermVector.size();
            if (hasScores) {
                termsSize = Math.min(termsSize, termVectorsFilter.size(field));
            }
            startField(field, termsSize, positions, offsets, payloads);

            if (flags.contains(Flag.FieldStatistics)) {
                writeFieldStatistics(topLevelTerms);
            }
            TermsEnum iterator = fieldTermVector.iterator();
            final boolean useDocsAndPos = positions || offsets || payloads;
            while (iterator.next() != null) { // iterate all terms of the current field
                BytesRef termBytesRef = iterator.term();
                Term term = new Term(field, termBytesRef);

                // with filtering we only keep the best terms
                if (hasScores && termVectorsFilter.hasScoreTerm(term) == false) {
                    continue;
                }

                startTerm(termBytesRef);
                if (flags.contains(Flag.TermStatistics)) {
                    // get the doc frequency
                    boolean foundTerm = topLevelIterator.seekExact(termBytesRef);
                    if (foundTerm) {
                        writeTermStatistics(topLevelIterator);
                    } else {
                        writeMissingTermStatistics();
                    }
                }
                if (useDocsAndPos) {
                    // given we have pos or offsets
                    docsAndPosEnum = writeTermWithDocsAndPos(iterator, docsAndPosEnum, positions, offsets, payloads);
                } else {
                    // if we do not have the positions stored, we need to
                    // get the frequency from a PostingsEnum.
                    docsEnum = writeTermWithDocsOnly(iterator, docsEnum);
                }
                if (hasScores) {
                    writeScoreTerm(termVectorsFilter.getScoreTerm(term));
                }
            }
            numFieldsWritten++;
        }
        response.setTermVectorsField(output);
        response.setHeader(
            writeHeader(numFieldsWritten, flags.contains(Flag.TermStatistics), flags.contains(Flag.FieldStatistics), hasScores)
        );
    }

    private BytesReference writeHeader(int numFieldsWritten, boolean getTermStatistics, boolean getFieldStatistics, boolean scores)
        throws IOException {
        // now, write the information about offset of the terms in the
        // termVectors field
        BytesStreamOutput header = new BytesStreamOutput();
        header.writeString(HEADER);
        header.writeInt(CURRENT_VERSION);
        header.writeBoolean(getTermStatistics);
        header.writeBoolean(getFieldStatistics);
        header.writeBoolean(scores);
        header.writeVInt(numFieldsWritten);
        for (int i = 0; i < fields.size(); i++) {
            header.writeString(fields.get(i));
            header.writeVLong(fieldOffset.get(i));
        }
        header.close();
        return header.bytes();
    }

    private PostingsEnum writeTermWithDocsOnly(TermsEnum iterator, PostingsEnum docsEnum) throws IOException {
        docsEnum = iterator.postings(docsEnum);
        int nextDoc = docsEnum.nextDoc();
        assert nextDoc != DocIdSetIterator.NO_MORE_DOCS;
        writeFreq(docsEnum.freq());
        nextDoc = docsEnum.nextDoc();
        assert nextDoc == DocIdSetIterator.NO_MORE_DOCS;
        return docsEnum;
    }

    private PostingsEnum writeTermWithDocsAndPos(
        TermsEnum iterator,
        PostingsEnum docsAndPosEnum,
        boolean positions,
        boolean offsets,
        boolean payloads
    ) throws IOException {
        docsAndPosEnum = iterator.postings(docsAndPosEnum, PostingsEnum.ALL);
        // for each term (iterator next) in this field (field)
        // iterate over the docs (should only be one)
        int nextDoc = docsAndPosEnum.nextDoc();
        assert nextDoc != DocIdSetIterator.NO_MORE_DOCS;
        final int freq = docsAndPosEnum.freq();
        writeFreq(freq);
        for (int j = 0; j < freq; j++) {
            int curPos = docsAndPosEnum.nextPosition();
            if (positions) {
                writePosition(curPos);
            }
            if (offsets) {
                writeOffsets(docsAndPosEnum.startOffset(), docsAndPosEnum.endOffset());
            }
            if (payloads) {
                writePayload(docsAndPosEnum.getPayload());
            }
        }
        nextDoc = docsAndPosEnum.nextDoc();
        assert nextDoc == DocIdSetIterator.NO_MORE_DOCS;
        return docsAndPosEnum;
    }

    private void writePayload(BytesRef payload) throws IOException {
        if (payload != null) {
            output.writeVInt(payload.length);
            output.writeBytes(payload.bytes, payload.offset, payload.length);
        } else {
            output.writeVInt(0);
        }
    }

    private void writeFreq(int termFreq) throws IOException {
        writePotentiallyNegativeVInt(termFreq);
    }

    private void writeOffsets(int startOffset, int endOffset) throws IOException {
        assert (startOffset >= 0);
        assert (endOffset >= 0);
        if ((startOffset >= 0) && (endOffset >= 0)) {
            output.writeVInt(startOffset);
            output.writeVInt(endOffset);
        }
    }

    private void writePosition(int pos) throws IOException {
        assert (pos >= 0);
        if (pos >= 0) {
            output.writeVInt(pos);
        }
    }

    private void startField(String fieldName, long termsSize, boolean writePositions, boolean writeOffsets, boolean writePayloads)
        throws IOException {
        fields.add(fieldName);
        fieldOffset.add(output.position());
        output.writeVLong(termsSize);
        // add information on if positions etc. are written
        output.writeBoolean(writePositions);
        output.writeBoolean(writeOffsets);
        output.writeBoolean(writePayloads);
    }

    private void startTerm(BytesRef term) throws IOException {
        output.writeVInt(term.length);
        output.writeBytes(term.bytes, term.offset, term.length);
    }

    private void writeMissingTermStatistics() throws IOException {
        writePotentiallyNegativeVInt(0);
        writePotentiallyNegativeVInt(0);
    }

    private void writeTermStatistics(TermsEnum topLevelIterator) throws IOException {
        int docFreq = topLevelIterator.docFreq();
        assert (docFreq >= -1);
        writePotentiallyNegativeVInt(docFreq);
        long ttf = topLevelIterator.totalTermFreq();
        assert (ttf >= -1);
        writePotentiallyNegativeVLong(ttf);
    }

    private void writeFieldStatistics(Terms topLevelTerms) throws IOException {
        long sttf = topLevelTerms.getSumTotalTermFreq();
        assert (sttf >= -1);
        writePotentiallyNegativeVLong(sttf);
        long sdf = topLevelTerms.getSumDocFreq();
        assert (sdf >= -1);
        writePotentiallyNegativeVLong(sdf);
        int dc = topLevelTerms.getDocCount();
        assert (dc >= -1);
        writePotentiallyNegativeVInt(dc);
    }

    private void writeScoreTerm(TermVectorsFilter.ScoreTerm scoreTerm) throws IOException {
        output.writeFloat(Math.max(0, scoreTerm.score));
    }

    private void writePotentiallyNegativeVInt(int value) throws IOException {
        // term freq etc. can be negative if not present... we transport that
        // further...
        output.writeVInt(Math.max(0, value + 1));
    }

    private void writePotentiallyNegativeVLong(long value) throws IOException {
        // term freq etc. can be negative if not present... we transport that
        // further...
        output.writeVLong(Math.max(0, value + 1));
    }

    /** Implements an empty {@link Terms}. */
    private static final Terms EMPTY_TERMS = new Terms() {
        @Override
        public TermsEnum iterator() {
            return TermsEnum.EMPTY;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public long getSumTotalTermFreq() {
            return 0;
        }

        @Override
        public long getSumDocFreq() {
            return 0;
        }

        @Override
        public int getDocCount() {
            return 0;
        }

        @Override
        public boolean hasFreqs() {
            return false;
        }

        @Override
        public boolean hasOffsets() {
            return false;
        }

        @Override
        public boolean hasPositions() {
            return false;
        }

        @Override
        public boolean hasPayloads() {
            return false;
        }
    };

}
