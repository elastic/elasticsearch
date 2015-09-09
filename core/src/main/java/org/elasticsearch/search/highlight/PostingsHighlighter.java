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
package org.elasticsearch.search.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.postingshighlight.CustomPassageFormatter;
import org.apache.lucene.search.postingshighlight.CustomPostingsHighlighter;
import org.apache.lucene.search.postingshighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.postingshighlight.Snippet;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PostingsHighlighter implements Highlighter {

    private static final String CACHE_KEY = "highlight-postings";

    @Override
    public HighlightField highlight(HighlighterContext highlighterContext) {

        FieldMapper fieldMapper = highlighterContext.mapper;
        SearchContextHighlight.Field field = highlighterContext.field;
        if (canHighlight(fieldMapper) == false) {
            throw new IllegalArgumentException("the field [" + highlighterContext.fieldName + "] should be indexed with positions and offsets in the postings list to be used with postings highlighter");
        }

        SearchContext context = highlighterContext.context;
        FetchSubPhase.HitContext hitContext = highlighterContext.hitContext;

        if (!hitContext.cache().containsKey(CACHE_KEY)) {
            hitContext.cache().put(CACHE_KEY, new HighlighterEntry());
        }

        HighlighterEntry highlighterEntry = (HighlighterEntry) hitContext.cache().get(CACHE_KEY);
        MapperHighlighterEntry mapperHighlighterEntry = highlighterEntry.mappers.get(fieldMapper);

        if (mapperHighlighterEntry == null) {
            Encoder encoder = field.fieldOptions().encoder().equals("html") ? HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;
            CustomPassageFormatter passageFormatter = new CustomPassageFormatter(field.fieldOptions().preTags()[0], field.fieldOptions().postTags()[0], encoder);
            mapperHighlighterEntry = new MapperHighlighterEntry(passageFormatter);
        }

        List<Snippet> snippets = new ArrayList<>();
        int numberOfFragments;
        try {
            Analyzer analyzer = context.mapperService().documentMapper(hitContext.hit().type()).mappers().indexAnalyzer();
            List<Object> fieldValues = HighlightUtils.loadFieldValues(field, fieldMapper, context, hitContext);
            CustomPostingsHighlighter highlighter;
            if (field.fieldOptions().numberOfFragments() == 0) {
                //we use a control char to separate values, which is the only char that the custom break iterator breaks the text on,
                //so we don't lose the distinction between the different values of a field and we get back a snippet per value
                String fieldValue = mergeFieldValues(fieldValues, HighlightUtils.NULL_SEPARATOR);
                CustomSeparatorBreakIterator breakIterator = new CustomSeparatorBreakIterator(HighlightUtils.NULL_SEPARATOR);
                highlighter = new CustomPostingsHighlighter(analyzer, mapperHighlighterEntry.passageFormatter, breakIterator, fieldValue, field.fieldOptions().noMatchSize() > 0);
                numberOfFragments = fieldValues.size(); //we are highlighting the whole content, one snippet per value
            } else {
                //using paragraph separator we make sure that each field value holds a discrete passage for highlighting
                String fieldValue = mergeFieldValues(fieldValues, HighlightUtils.PARAGRAPH_SEPARATOR);
                highlighter = new CustomPostingsHighlighter(analyzer, mapperHighlighterEntry.passageFormatter, fieldValue, field.fieldOptions().noMatchSize() > 0);
                numberOfFragments = field.fieldOptions().numberOfFragments();
            }

            IndexSearcher searcher = new IndexSearcher(hitContext.reader());
            Snippet[] fieldSnippets = highlighter.highlightField(fieldMapper.fieldType().names().indexName(), highlighterContext.query, searcher, hitContext.docId(), numberOfFragments);
            for (Snippet fieldSnippet : fieldSnippets) {
                if (Strings.hasText(fieldSnippet.getText())) {
                    snippets.add(fieldSnippet);
                }
            }

        } catch(IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
        }

        snippets = filterSnippets(snippets, field.fieldOptions().numberOfFragments());

        if (field.fieldOptions().scoreOrdered()) {
            //let's sort the snippets by score if needed
            CollectionUtil.introSort(snippets, new Comparator<Snippet>() {
                @Override
                public int compare(Snippet o1, Snippet o2) {
                    return (int) Math.signum(o2.getScore() - o1.getScore());
                }
            });
        }

        String[] fragments = new String[snippets.size()];
        for (int i = 0; i < fragments.length; i++) {
            fragments[i] = snippets.get(i).getText();
        }

        if (fragments.length > 0) {
            return new HighlightField(highlighterContext.fieldName, StringText.convertFromStringArray(fragments));
        }

        return null;
    }

    @Override
    public boolean canHighlight(FieldMapper fieldMapper) {
        return fieldMapper.fieldType().indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
    }

    private static String mergeFieldValues(List<Object> fieldValues, char valuesSeparator) {
        //postings highlighter accepts all values in a single string, as offsets etc. need to match with content
        //loaded from stored fields, we merge all values using a proper separator
        String rawValue = Strings.collectionToDelimitedString(fieldValues, String.valueOf(valuesSeparator));
        return rawValue.substring(0, Math.min(rawValue.length(), Integer.MAX_VALUE - 1));
    }

    private static List<Snippet> filterSnippets(List<Snippet> snippets, int numberOfFragments) {

        //We need to filter the snippets as due to no_match_size we could have
        //either highlighted snippets or non highlighted ones and we don't want to mix those up
        List<Snippet> filteredSnippets = new ArrayList<>(snippets.size());
        for (Snippet snippet : snippets) {
            if (snippet.isHighlighted()) {
                filteredSnippets.add(snippet);
            }
        }

        //if there's at least one highlighted snippet, we return all the highlighted ones
        //otherwise we return the first non highlighted one if available
        if (filteredSnippets.size() == 0) {
            if (snippets.size() > 0) {
                Snippet snippet = snippets.get(0);
                //if we tried highlighting the whole content using whole break iterator (as number_of_fragments was 0)
                //we need to return the first sentence of the content rather than the whole content
                if (numberOfFragments == 0) {
                    BreakIterator bi = BreakIterator.getSentenceInstance(Locale.ROOT);
                    String text = snippet.getText();
                    bi.setText(text);
                    int next = bi.next();
                    if (next != BreakIterator.DONE) {
                        String newText = text.substring(0, next).trim();
                        snippet = new Snippet(newText, snippet.getScore(), snippet.isHighlighted());
                    }
                }
                filteredSnippets.add(snippet);
            }
        }

        return filteredSnippets;
    }

    private static class HighlighterEntry {
        Map<FieldMapper, MapperHighlighterEntry> mappers = new HashMap<>();
    }

    private static class MapperHighlighterEntry {
        final CustomPassageFormatter passageFormatter;

        private MapperHighlighterEntry(CustomPassageFormatter passageFormatter) {
            this.passageFormatter = passageFormatter;
        }
    }
}
