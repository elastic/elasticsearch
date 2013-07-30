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
package org.elasticsearch.search.highlight;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class PlainHighlighter implements Highlighter {

    private static final String CACHE_KEY = "highlight-plain";

    @Override
    public String[] names() {
        return new String[] { "plain", "highlighter" };
    }

    public HighlightField highlight(HighlighterContext highlighterContext) {
        SearchContextHighlight.Field field = highlighterContext.field;
        SearchContext context = highlighterContext.context;
        FetchSubPhase.HitContext hitContext = highlighterContext.hitContext;
        FieldMapper<?> mapper = highlighterContext.mapper;

        Encoder encoder = field.encoder().equals("html") ? Encoders.HTML : Encoders.DEFAULT;

        if (!hitContext.cache().containsKey(CACHE_KEY)) {
            Map<FieldMapper<?>, org.apache.lucene.search.highlight.Highlighter> mappers = Maps.newHashMap();
            hitContext.cache().put(CACHE_KEY, mappers);
        }
        Map<FieldMapper<?>, org.apache.lucene.search.highlight.Highlighter> cache = (Map<FieldMapper<?>, org.apache.lucene.search.highlight.Highlighter>) hitContext.cache().get(CACHE_KEY);

        org.apache.lucene.search.highlight.Highlighter entry = cache.get(mapper);
        if (entry == null) {
            // Don't use the context.query() since it might be rewritten, and we need to pass the non rewritten queries to
            // let the highlighter handle MultiTerm ones
            Query query = context.parsedQuery().query();
            QueryScorer queryScorer = new CustomQueryScorer(query, field.requireFieldMatch() ? mapper.names().indexName() : null);
            queryScorer.setExpandMultiTermQuery(true);
            Fragmenter fragmenter;
            if (field.numberOfFragments() == 0) {
                fragmenter = new NullFragmenter();
            } else if (field.fragmenter() == null) {
                fragmenter = new SimpleSpanFragmenter(queryScorer, field.fragmentCharSize());
            } else if ("simple".equals(field.fragmenter())) {
                fragmenter = new SimpleFragmenter(field.fragmentCharSize());
            } else if ("span".equals(field.fragmenter())) {
                fragmenter = new SimpleSpanFragmenter(queryScorer, field.fragmentCharSize());
            } else {
                throw new ElasticSearchIllegalArgumentException("unknown fragmenter option [" + field.fragmenter() + "] for the field [" + highlighterContext.fieldName + "]");
            }
            Formatter formatter = new SimpleHTMLFormatter(field.preTags()[0], field.postTags()[0]);

            entry = new org.apache.lucene.search.highlight.Highlighter(formatter, encoder, queryScorer);
            entry.setTextFragmenter(fragmenter);
            // always highlight across all data
            entry.setMaxDocCharsToAnalyze(Integer.MAX_VALUE);

            cache.put(mapper, entry);
        }

        List<Object> textsToHighlight;
        if (mapper.fieldType().stored()) {
            try {
                CustomFieldsVisitor fieldVisitor = new CustomFieldsVisitor(ImmutableSet.of(mapper.names().indexName()), false);
                hitContext.reader().document(hitContext.docId(), fieldVisitor);
                textsToHighlight = fieldVisitor.fields().get(mapper.names().indexName());
                if (textsToHighlight == null) {
                    // Can happen if the document doesn't have the field to highlight
                    textsToHighlight = ImmutableList.of();
                }
            } catch (Exception e) {
                throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
            }
        } else {
            SearchLookup lookup = context.lookup();
            lookup.setNextReader(hitContext.readerContext());
            lookup.setNextDocId(hitContext.docId());
            textsToHighlight = lookup.source().extractRawValues(mapper.names().sourcePath());
        }
        assert textsToHighlight != null;

        // a HACK to make highlighter do highlighting, even though its using the single frag list builder
        int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
        ArrayList<TextFragment> fragsList = new ArrayList<TextFragment>();
        try {
            for (Object textToHighlight : textsToHighlight) {
                String text = textToHighlight.toString();
                Analyzer analyzer = context.mapperService().documentMapper(hitContext.hit().type()).mappers().indexAnalyzer();
                TokenStream tokenStream = analyzer.tokenStream(mapper.names().indexName(), text);
                if (!tokenStream.hasAttribute(CharTermAttribute.class) || !tokenStream.hasAttribute(OffsetAttribute.class)) {
                    // can't perform highlighting if the stream has no terms (binary token stream) or no offsets
                    continue;
                }
                TextFragment[] bestTextFragments = entry.getBestTextFragments(tokenStream, text, false, numberOfFragments);
                for (TextFragment bestTextFragment : bestTextFragments) {
                    if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
                        fragsList.add(bestTextFragment);
                    }
                }
            }
        } catch (Exception e) {
            throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
        }
        if (field.scoreOrdered()) {
            CollectionUtil.introSort(fragsList, new Comparator<TextFragment>() {
                public int compare(TextFragment o1, TextFragment o2) {
                    return Math.round(o2.getScore() - o1.getScore());
                }
            });
        }
        String[] fragments = null;
        // number_of_fragments is set to 0 but we have a multivalued field
        if (field.numberOfFragments() == 0 && textsToHighlight.size() > 1 && fragsList.size() > 0) {
            fragments = new String[fragsList.size()];
            for (int i = 0; i < fragsList.size(); i++) {
                fragments[i] = fragsList.get(i).toString();
            }
        } else {
            // refine numberOfFragments if needed
            numberOfFragments = fragsList.size() < numberOfFragments ? fragsList.size() : numberOfFragments;
            fragments = new String[numberOfFragments];
            for (int i = 0; i < fragments.length; i++) {
                fragments[i] = fragsList.get(i).toString();
            }
        }

        if (fragments != null && fragments.length > 0) {
            return new HighlightField(highlighterContext.fieldName, StringText.convertFromStringArray(fragments));
        }

        return null;
    }

    private static class Encoders {
        public static Encoder DEFAULT = new DefaultEncoder();
        public static Encoder HTML = new SimpleHTMLEncoder();
    }
}
