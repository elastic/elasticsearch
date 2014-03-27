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
package org.elasticsearch.action.termvector;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.termvector.TermVectorRequest.Flag;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

// package only - this is an internal class!
final class TermVectorWriter {
    final List<String> fields = new ArrayList<>();
    final List<Long> fieldOffset = new ArrayList<>();
    final BytesStreamOutput output = new BytesStreamOutput(1); // can we somehow
    // predict the
    // size here?
    private static final String HEADER = "TV";
    private static final int CURRENT_VERSION = -1;
    TermVectorResponse response = null;

    TermVectorWriter(TermVectorResponse termVectorResponse) throws IOException {
        response = termVectorResponse;
    }

    void setFields(Fields termVectorsByField, Set<String> selectedFields, EnumSet<Flag> flags, Fields topLevelFields) throws IOException {

        int numFieldsWritten = 0;
        TermsEnum iterator = null;
        DocsAndPositionsEnum docsAndPosEnum = null;
        DocsEnum docsEnum = null;
        TermsEnum topLevelIterator = null;
        for (String field : termVectorsByField) {
            if ((selectedFields != null) && (!selectedFields.contains(field))) {
                continue;
            }

            Terms fieldTermVector = termVectorsByField.terms(field);
            Terms topLevelTerms = topLevelFields.terms(field);

            topLevelIterator = topLevelTerms.iterator(topLevelIterator);
            boolean positions = flags.contains(Flag.Positions) && fieldTermVector.hasPositions();
            boolean offsets = flags.contains(Flag.Offsets) && fieldTermVector.hasOffsets();
            boolean payloads = flags.contains(Flag.Payloads) && fieldTermVector.hasPayloads();
            startField(field, fieldTermVector.size(), positions, offsets, payloads);
            if (flags.contains(Flag.FieldStatistics)) {
                writeFieldStatistics(topLevelTerms);
            }
            iterator = fieldTermVector.iterator(iterator);
            final boolean useDocsAndPos = positions || offsets || payloads;
            while (iterator.next() != null) { // iterate all terms of the
                // current field
                // get the doc frequency
                BytesRef term = iterator.term();
                boolean foundTerm = topLevelIterator.seekExact(term);
                assert (foundTerm);
                startTerm(term);
                if (flags.contains(Flag.TermStatistics)) {
                    writeTermStatistics(topLevelIterator);
                }
                if (useDocsAndPos) {
                    // given we have pos or offsets
                    docsAndPosEnum = writeTermWithDocsAndPos(iterator, docsAndPosEnum, positions, offsets, payloads);
                } else {
                    // if we do not have the positions stored, we need to
                    // get the frequency from a DocsEnum.
                    docsEnum = writeTermWithDocsOnly(iterator, docsEnum);
                }
            }
            numFieldsWritten++;
        }
        response.setTermVectorField(output);
        response.setHeader(writeHeader(numFieldsWritten, flags.contains(Flag.TermStatistics), flags.contains(Flag.FieldStatistics)));
    }

    private BytesReference writeHeader(int numFieldsWritten, boolean getTermStatistics, boolean getFieldStatistics) throws IOException {
        // now, write the information about offset of the terms in the
        // termVectors field
        BytesStreamOutput header = new BytesStreamOutput();
        header.writeString(HEADER);
        header.writeInt(CURRENT_VERSION);
        header.writeBoolean(getTermStatistics);
        header.writeBoolean(getFieldStatistics);
        header.writeVInt(numFieldsWritten);
        for (int i = 0; i < fields.size(); i++) {
            header.writeString(fields.get(i));
            header.writeVLong(fieldOffset.get(i).longValue());
        }
        header.close();
        return header.bytes();
    }

    private DocsEnum writeTermWithDocsOnly(TermsEnum iterator, DocsEnum docsEnum) throws IOException {
        docsEnum = iterator.docs(null, docsEnum);
        int nextDoc = docsEnum.nextDoc();
        assert nextDoc != DocsEnum.NO_MORE_DOCS;
        writeFreq(docsEnum.freq());
        nextDoc = docsEnum.nextDoc();
        assert nextDoc == DocsEnum.NO_MORE_DOCS;
        return docsEnum;
    }

    private DocsAndPositionsEnum writeTermWithDocsAndPos(TermsEnum iterator, DocsAndPositionsEnum docsAndPosEnum, boolean positions,
                                                         boolean offsets, boolean payloads) throws IOException {
        docsAndPosEnum = iterator.docsAndPositions(null, docsAndPosEnum);
        // for each term (iterator next) in this field (field)
        // iterate over the docs (should only be one)
        int nextDoc = docsAndPosEnum.nextDoc();
        assert nextDoc != DocsEnum.NO_MORE_DOCS;
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
        assert nextDoc == DocsEnum.NO_MORE_DOCS;
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

    private void writeTermStatistics(TermsEnum topLevelIterator) throws IOException {
        int docFreq = topLevelIterator.docFreq();
        assert (docFreq >= 0);
        writePotentiallyNegativeVInt(docFreq);
        long ttf = topLevelIterator.totalTermFreq();
        assert (ttf >= 0);
        writePotentiallyNegativeVLong(ttf);

    }

    private void writeFieldStatistics(Terms topLevelTerms) throws IOException {
        long sttf = topLevelTerms.getSumTotalTermFreq();
        assert (sttf >= 0);
        writePotentiallyNegativeVLong(sttf);
        long sdf = topLevelTerms.getSumDocFreq();
        assert (sdf >= 0);
        writePotentiallyNegativeVLong(sdf);
        int dc = topLevelTerms.getDocCount();
        assert (dc >= 0);
        writePotentiallyNegativeVInt(dc);

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

}