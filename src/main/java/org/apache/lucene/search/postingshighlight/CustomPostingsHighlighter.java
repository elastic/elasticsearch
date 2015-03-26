/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.lucene.search.postingshighlight;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.highlight.HighlightUtils;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.List;
import java.util.Map;

/**
 * Subclass of the {@link XPostingsHighlighter} that works for a single field in a single document.
 * It receives the field values as input and it performs discrete highlighting on each single value
 * calling the highlightDoc method multiple times.
 * It allows to pass in the query terms to avoid calling extract terms multiple times.
 *
 * The use that we make of the postings highlighter is not optimal. It would be much better to
 * highlight multiple docs in a single call, as we actually lose its sequential IO.  But that would require:
 * 1) to make our fork more complex and harder to maintain to perform discrete highlighting (needed to return
 * a different snippet per value when number_of_fragments=0 and the field has multiple values)
 * 2) refactoring of the elasticsearch highlight api which currently works per hit
 *
 */
public final class CustomPostingsHighlighter extends XPostingsHighlighter {

    private static final Snippet[] EMPTY_SNIPPET = new Snippet[0];
    private static final Passage[] EMPTY_PASSAGE = new Passage[0];

    private final CustomPassageFormatter passageFormatter;
    private final int noMatchSize;
    private final int totalContentLength;
    private final String[] fieldValues;
    private final int[] fieldValuesOffsets;
    private int currentValueIndex = 0;

    private BreakIterator breakIterator;

    public CustomPostingsHighlighter(CustomPassageFormatter passageFormatter, List<Object> fieldValues, boolean mergeValues, int maxLength, int noMatchSize) {
        super(maxLength);
        this.passageFormatter = passageFormatter;
        this.noMatchSize = noMatchSize;

        if (mergeValues) {
            String rawValue = Strings.collectionToDelimitedString(fieldValues, String.valueOf(getMultiValuedSeparator("")));
            String fieldValue = rawValue.substring(0, Math.min(rawValue.length(), maxLength));
            this.fieldValues = new String[]{fieldValue};
            this.fieldValuesOffsets = new int[]{0};
            this.totalContentLength = fieldValue.length();
        } else {
            this.fieldValues = new String[fieldValues.size()];
            this.fieldValuesOffsets = new int[fieldValues.size()];
            int contentLength = 0;
            int offset = 0;
            int previousLength = -1;
            for (int i = 0; i < fieldValues.size(); i++) {
                String rawValue = fieldValues.get(i).toString();
                String fieldValue = rawValue.substring(0, Math.min(rawValue.length(), maxLength));
                this.fieldValues[i] = fieldValue;
                contentLength += fieldValue.length();
                offset += previousLength + 1;
                this.fieldValuesOffsets[i] = offset;
                previousLength = fieldValue.length();
            }
            this.totalContentLength = contentLength;
        }
    }

    /*
    Our own api to highlight a single document field, passing in the query terms, and get back our own Snippet object
     */
    public Snippet[] highlightDoc(String field, BytesRef[] terms, IndexSearcher searcher, int docId, int maxPassages) throws IOException {
        IndexReader reader = searcher.getIndexReader();
        IndexReaderContext readerContext = reader.getContext();
        List<LeafReaderContext> leaves = readerContext.leaves();

        String[] contents = new String[]{loadCurrentFieldValue()};
        Map<Integer, Object> snippetsMap = highlightField(field, contents, getBreakIterator(field), terms, new int[]{docId}, leaves, maxPassages);

        //increment the current value index so that next time we'll highlight the next value if available
        currentValueIndex++;

        Object snippetObject = snippetsMap.get(docId);
        if (snippetObject != null && snippetObject instanceof Snippet[]) {
            return (Snippet[]) snippetObject;
        }
        return EMPTY_SNIPPET;
    }

    /*
    Method provided through our own fork: allows to do proper scoring when doing per value discrete highlighting.
    Used to provide the total length of the field (all values) for proper scoring.
     */
    @Override
    protected int getContentLength(String field, int docId) {
        return totalContentLength;
    }

    /*
    Method provided through our own fork: allows to perform proper per value discrete highlighting.
    Used to provide the offset for the current value.
     */
    @Override
    protected int getOffsetForCurrentValue(String field, int docId) {
        if (currentValueIndex < fieldValuesOffsets.length) {
            return fieldValuesOffsets[currentValueIndex];
        }
        throw new IllegalArgumentException("No more values offsets to return");
    }

    public void setBreakIterator(BreakIterator breakIterator) {
        this.breakIterator = breakIterator;
    }

    @Override
    protected PassageFormatter getFormatter(String field) {
        return passageFormatter;
    }

    @Override
    protected BreakIterator getBreakIterator(String field) {
        if (breakIterator == null) {
            return super.getBreakIterator(field);
        }
        return breakIterator;
    }

    @Override
    protected char getMultiValuedSeparator(String field) {
        //U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting
        return HighlightUtils.PARAGRAPH_SEPARATOR;
    }

    /*
    By default the postings highlighter returns non highlighted snippet when there are no matches.
    We want to return no snippets by default, unless no_match_size is greater than 0
     */
    @Override
    protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
        if (noMatchSize > 0) {
            //we want to return the first sentence of the first snippet only
            return super.getEmptyHighlight(fieldName, bi, 1);
        }
        return EMPTY_PASSAGE;
    }

    /*
    Not needed since we call our own loadCurrentFieldValue explicitly, but we override it anyway for consistency.
     */
    @Override
    protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
        return new String[][]{new String[]{loadCurrentFieldValue()}};
    }

    /*
     Our own method that returns the field values, which relies on the content that was provided when creating the highlighter.
     Supports per value discrete highlighting calling the highlightDoc method multiple times, one per value.
    */
    protected String loadCurrentFieldValue() {
        if (currentValueIndex < fieldValues.length) {
            return fieldValues[currentValueIndex];
        }
        throw new IllegalArgumentException("No more values to return");
    }
}
