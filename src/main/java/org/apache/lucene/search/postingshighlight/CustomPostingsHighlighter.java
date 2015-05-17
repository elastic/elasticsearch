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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Map;

/**
 * Subclass of the {@link PostingsHighlighter} that works for a single field in a single document.
 * Uses a custom {@link PassageFormatter}. Accepts field content as a constructor argument, given that loading
 * is custom and can be done reading from _source field. Supports using different {@link BreakIterator} to break
 * the text into fragments. Considers every distinct field value as a discrete passage for highlighting (unless
 * the whole content needs to be highlighted). Supports both returning empty snippets and non highlighted snippets
 * when no highlighting can be performed.
 *
 * The use that we make of the postings highlighter is not optimal. It would be much better to highlight
 * multiple docs in a single call, as we actually lose its sequential IO.  That would require to
 * refactor the elasticsearch highlight api which currently works per hit.
 */
public final class CustomPostingsHighlighter extends PostingsHighlighter {

    private static final Snippet[] EMPTY_SNIPPET = new Snippet[0];
    private static final Passage[] EMPTY_PASSAGE = new Passage[0];

    private final Analyzer analyzer;
    private final CustomPassageFormatter passageFormatter;
    private final BreakIterator breakIterator;
    private final boolean returnNonHighlightedSnippets;
    private final String fieldValue;

    /**
     * Creates a new instance of {@link CustomPostingsHighlighter}
     *
     * @param analyzer the analyzer used for the field at index time, used for multi term queries internally
     * @param passageFormatter our own {@link PassageFormatter} which generates snippets in forms of {@link Snippet} objects
     * @param fieldValue the original field values as constructor argument, loaded from te _source field or the relevant stored field.
     * @param returnNonHighlightedSnippets whether non highlighted snippets should be returned rather than empty snippets when
     *                                     no highlighting can be performed
     */
    public CustomPostingsHighlighter(Analyzer analyzer, CustomPassageFormatter passageFormatter, String fieldValue, boolean returnNonHighlightedSnippets) {
        this(analyzer, passageFormatter, null, fieldValue, returnNonHighlightedSnippets);
    }

    /**
     * Creates a new instance of {@link CustomPostingsHighlighter}
     *
     * @param analyzer the analyzer used for the field at index time, used for multi term queries internally
     * @param passageFormatter our own {@link PassageFormatter} which generates snippets in forms of {@link Snippet} objects
     * @param breakIterator an instance {@link BreakIterator} selected depending on the highlighting options
     * @param fieldValue the original field values as constructor argument, loaded from te _source field or the relevant stored field.
     * @param returnNonHighlightedSnippets whether non highlighted snippets should be returned rather than empty snippets when
     *                                     no highlighting can be performed
     */
    public CustomPostingsHighlighter(Analyzer analyzer, CustomPassageFormatter passageFormatter, BreakIterator breakIterator, String fieldValue, boolean returnNonHighlightedSnippets) {
        this.analyzer = analyzer;
        this.passageFormatter = passageFormatter;
        this.breakIterator = breakIterator;
        this.returnNonHighlightedSnippets = returnNonHighlightedSnippets;
        this.fieldValue = fieldValue;
    }

    /**
     * Highlights terms extracted from the provided query within the content of the provided field name
     */
    public Snippet[] highlightField(String field, Query query, IndexSearcher searcher, int docId, int maxPassages) throws IOException {
        Map<String, Object[]> fieldsAsObjects = super.highlightFieldsAsObjects(new String[]{field}, query, searcher, new int[]{docId}, new int[]{maxPassages});
        Object[] snippetObjects = fieldsAsObjects.get(field);
        if (snippetObjects != null) {
            //one single document at a time
            assert snippetObjects.length == 1;
            Object snippetObject = snippetObjects[0];
            if (snippetObject != null && snippetObject instanceof Snippet[]) {
                return (Snippet[]) snippetObject;
            }
        }
        return EMPTY_SNIPPET;
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

    /*
    By default the postings highlighter returns non highlighted snippet when there are no matches.
    We want to return no snippets by default, unless no_match_size is greater than 0
     */
    @Override
    protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
        if (returnNonHighlightedSnippets) {
            //we want to return the first sentence of the first snippet only
            return super.getEmptyHighlight(fieldName, bi, 1);
        }
        return EMPTY_PASSAGE;
    }

    @Override
    protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
    }

    @Override
    protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
        //we only highlight one field, one document at a time
        return new String[][]{new String[]{fieldValue}};
    }
}
