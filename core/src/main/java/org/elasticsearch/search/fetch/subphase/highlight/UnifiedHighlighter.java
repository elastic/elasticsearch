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
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.Snippet;
import org.apache.lucene.search.uhighlight.CustomPassageFormatter;
import org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.search.fetch.subphase.highlight.PostingsHighlighter.filterSnippets;
import static org.elasticsearch.search.fetch.subphase.highlight.PostingsHighlighter.mergeFieldValues;

public class UnifiedHighlighter implements Highlighter {
    private static final String CACHE_KEY = "highlight-unified";

    @Override
    public boolean canHighlight(FieldMapper fieldMapper) {
        return true;
    }

    @Override
    public HighlightField highlight(HighlighterContext highlighterContext) {
        FieldMapper fieldMapper = highlighterContext.mapper;
        SearchContextHighlight.Field field = highlighterContext.field;
        SearchContext context = highlighterContext.context;
        FetchSubPhase.HitContext hitContext = highlighterContext.hitContext;

        if (!hitContext.cache().containsKey(CACHE_KEY)) {
            hitContext.cache().put(CACHE_KEY, new HighlighterEntry());
        }

        HighlighterEntry highlighterEntry = (HighlighterEntry) hitContext.cache().get(CACHE_KEY);
        MapperHighlighterEntry mapperHighlighterEntry = highlighterEntry.mappers.get(fieldMapper);

        if (mapperHighlighterEntry == null) {
            Encoder encoder = field.fieldOptions().encoder().equals("html") ?
                HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;
            CustomPassageFormatter passageFormatter =
                new CustomPassageFormatter(field.fieldOptions().preTags()[0],
                    field.fieldOptions().postTags()[0], encoder);
            mapperHighlighterEntry = new MapperHighlighterEntry(passageFormatter);
        }

        List<Snippet> snippets = new ArrayList<>();
        int numberOfFragments;
        try {
            Analyzer analyzer =
                context.mapperService().documentMapper(hitContext.hit().getType()).mappers().indexAnalyzer();
            List<Object> fieldValues = HighlightUtils.loadFieldValues(field, fieldMapper, context, hitContext);
            fieldValues = fieldValues.stream().map(obj -> {
                if (obj instanceof BytesRef) {
                    return fieldMapper.fieldType().valueForDisplay(obj).toString();
                } else {
                    return obj;
                }
            }).collect(Collectors.toList());
            IndexSearcher searcher = new IndexSearcher(hitContext.reader());
            CustomUnifiedHighlighter highlighter;
            if (field.fieldOptions().numberOfFragments() == 0) {
                // we use a control char to separate values, which is the only char that the custom break iterator
                // breaks the text on, so we don't lose the distinction between the different values of a field and we
                // get back a snippet per value
                String fieldValue = mergeFieldValues(fieldValues, HighlightUtils.NULL_SEPARATOR);
                org.apache.lucene.search.postingshighlight.CustomSeparatorBreakIterator breakIterator =
                    new org.apache.lucene.search.postingshighlight
                        .CustomSeparatorBreakIterator(HighlightUtils.NULL_SEPARATOR);
                highlighter =
                    new CustomUnifiedHighlighter(searcher, analyzer, mapperHighlighterEntry.passageFormatter,
                        breakIterator, fieldValue, field.fieldOptions().noMatchSize() > 0);
                numberOfFragments = fieldValues.size(); // we are highlighting the whole content, one snippet per value
            } else {
                //using paragraph separator we make sure that each field value holds a discrete passage for highlighting
                String fieldValue = mergeFieldValues(fieldValues, HighlightUtils.PARAGRAPH_SEPARATOR);
                highlighter = new CustomUnifiedHighlighter(searcher, analyzer,
                    mapperHighlighterEntry.passageFormatter, null, fieldValue, field.fieldOptions().noMatchSize() > 0);
                numberOfFragments = field.fieldOptions().numberOfFragments();
            }
            if (field.fieldOptions().requireFieldMatch()) {
                final String fieldName = highlighterContext.fieldName;
                highlighter.setFieldMatcher((name) -> fieldName.equals(name));
            } else {
                highlighter.setFieldMatcher((name) -> true);
            }
            Snippet[] fieldSnippets = highlighter.highlightField(highlighterContext.fieldName,
                highlighterContext.query, hitContext.docId(), numberOfFragments);
            for (Snippet fieldSnippet : fieldSnippets) {
                if (Strings.hasText(fieldSnippet.getText())) {
                    snippets.add(fieldSnippet);
                }
            }
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context,
                "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
        }

        snippets = filterSnippets(snippets, field.fieldOptions().numberOfFragments());

        if (field.fieldOptions().scoreOrdered()) {
            //let's sort the snippets by score if needed
            CollectionUtil.introSort(snippets, (o1, o2) -> Double.compare(o2.getScore(), o1.getScore()));
        }

        String[] fragments = new String[snippets.size()];
        for (int i = 0; i < fragments.length; i++) {
            fragments[i] = snippets.get(i).getText();
        }

        if (fragments.length > 0) {
            return new HighlightField(highlighterContext.fieldName, Text.convertFromStringArray(fragments));
        }
        return null;
    }

    static class HighlighterEntry {
        Map<FieldMapper, MapperHighlighterEntry> mappers = new HashMap<>();
    }

    static class MapperHighlighterEntry {
        final CustomPassageFormatter passageFormatter;

        private MapperHighlighterEntry(CustomPassageFormatter passageFormatter) {
            this.passageFormatter = passageFormatter;
        }
    }
}
