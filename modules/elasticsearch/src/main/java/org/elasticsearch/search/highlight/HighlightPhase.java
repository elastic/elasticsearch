/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.vectorhighlight.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.document.SingleFieldSelector;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.SearchHitPhase;
import org.elasticsearch.search.highlight.vectorhighlight.SourceScoreOrderFragmentsBuilder;
import org.elasticsearch.search.highlight.vectorhighlight.SourceSimpleFragmentsBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.collect.Maps.*;

/**
 * @author kimchy (shay.banon)
 */
public class HighlightPhase implements SearchHitPhase {

    private static final Encoder DEFAULT_ENCODER = new DefaultEncoder();

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("highlight", new HighlighterParseElement());
    }

    @Override public boolean executionNeeded(SearchContext context) {
        return context.highlight() != null;
    }

    @Override public void execute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        try {
            DocumentMapper documentMapper = context.mapperService().documentMapper(hitContext.hit().type());

            Map<String, HighlightField> highlightFields = newHashMap();
            for (SearchContextHighlight.Field field : context.highlight().fields()) {
                FieldMapper mapper = documentMapper.mappers().smartNameFieldMapper(field.field());
                if (mapper == null) {
                    //Save skipping missing fields
                    continue;
                }

                // if we can do highlighting using Term Vectors, use FastVectorHighlighter, otherwise, use the
                // slower plain highlighter
                if (mapper.termVector() != Field.TermVector.WITH_POSITIONS_OFFSETS) {
                    if (!context.queryRewritten()) {
                        try {
                            context.updateRewriteQuery(context.searcher().rewrite(context.query()));
                        } catch (IOException e) {
                            throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                        }
                    }
                    // Don't use the context.query() since it might be rewritten, and we need to pass the non rewritten queries to
                    // let the highlighter handle MultiTerm ones
                    QueryScorer queryScorer = new QueryScorer(context.parsedQuery().query(), null);
                    queryScorer.setExpandMultiTermQuery(true);
                    Fragmenter fragmenter;
                    if (field.numberOfFragments() == 0) {
                        fragmenter = new NullFragmenter();
                    } else {
                        fragmenter = new SimpleSpanFragmenter(queryScorer, field.fragmentCharSize());
                    }
                    Formatter formatter = new SimpleHTMLFormatter(field.preTags()[0], field.postTags()[0]);
                    Highlighter highlighter = new Highlighter(formatter, DEFAULT_ENCODER, queryScorer);
                    highlighter.setTextFragmenter(fragmenter);

                    List<Object> textsToHighlight;
                    if (mapper.stored()) {
                        try {
                            Document doc = hitContext.reader().document(hitContext.docId(), new SingleFieldSelector(mapper.names().indexName()));
                            textsToHighlight = new ArrayList<Object>(doc.getFields().size());
                            for (Fieldable docField : doc.getFields()) {
                                if (docField.stringValue() != null) {
                                    textsToHighlight.add(docField.stringValue());
                                }
                            }
                        } catch (Exception e) {
                            throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                        }
                    } else {
                        SearchLookup lookup = context.lookup();
                        lookup.setNextReader(hitContext.reader());
                        lookup.setNextDocId(hitContext.docId());
                        textsToHighlight = lookup.source().getValues(mapper.names().fullName());
                    }

                    // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                    int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                    ArrayList<TextFragment> fragsList = new ArrayList<TextFragment>();
                    try {
                        for (Object textToHighlight : textsToHighlight) {
                            String text = textToHighlight.toString();
                            Analyzer analyzer = context.mapperService().documentMapper(hitContext.hit().type()).mappers().indexAnalyzer();
                            TokenStream tokenStream = analyzer.reusableTokenStream(mapper.names().indexName(), new FastStringReader(text));
                            TextFragment[] bestTextFragments = highlighter.getBestTextFragments(tokenStream, text, false, numberOfFragments);
                            for (TextFragment bestTextFragment : bestTextFragments) {
                                if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
                                    fragsList.add(bestTextFragment);
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                    }
                    if (field.scoreOrdered()) {
                        Collections.sort(fragsList, new Comparator<TextFragment>() {
                            public int compare(TextFragment o1, TextFragment o2) {
                                return Math.round(o2.getScore() - o1.getScore());
                            }
                        });
                    }
                    String[] fragments;
                    // number_of_fragments is set to 0 but we have a multivalued field
                    if (field.numberOfFragments() == 0 && textsToHighlight.size() > 1) {
                        fragments = new String[1];
                        for (int i = 0; i < fragsList.size(); i++) {
                            fragments[0] = (fragments[0] != null ? (fragments[0]+" ") : "") + fragsList.get(i).toString();
                        }
                    } else {
                        // refine numberOfFragments if needed
                        numberOfFragments = fragsList.size() < numberOfFragments ? fragsList.size() : numberOfFragments;
                        fragments = new String[numberOfFragments];
                        for (int i = 0; i < fragments.length; i++) {
                            fragments[i] = fragsList.get(i).toString();
                        }
                    }

                    if (fragments.length > 0) {
                        HighlightField highlightField = new HighlightField(field.field(), fragments);
                        highlightFields.put(highlightField.name(), highlightField);
                    }
                } else {
                    FastVectorHighlighter highlighter = buildHighlighter(context, mapper, field);
                    FieldQuery fieldQuery = buildFieldQuery(highlighter, context.query(), hitContext.reader(), field);

                    String[] fragments;
                    try {
                        // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                        int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                        fragments = highlighter.getBestFragments(fieldQuery, hitContext.reader(), hitContext.docId(), mapper.names().indexName(), field.fragmentCharSize(), numberOfFragments);
                    } catch (IOException e) {
                        throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                    }
                    if (fragments != null && fragments.length > 0) {
                        HighlightField highlightField = new HighlightField(field.field(), fragments);
                        highlightFields.put(highlightField.name(), highlightField);
                    }
                }
            }

            hitContext.hit().highlightFields(highlightFields);
        } finally {
            CustomFieldQuery.reader.remove();
            CustomFieldQuery.highlightFilters.remove();
        }
    }

    private FieldQuery buildFieldQuery(FastVectorHighlighter highlighter, Query query, IndexReader indexReader, SearchContextHighlight.Field field) {
        CustomFieldQuery.reader.set(indexReader);
        CustomFieldQuery.highlightFilters.set(field.highlightFilter());
        return new CustomFieldQuery(query, highlighter);
    }

    private FastVectorHighlighter buildHighlighter(SearchContext searchContext, FieldMapper fieldMapper, SearchContextHighlight.Field field) {
        FragListBuilder fragListBuilder;
        FragmentsBuilder fragmentsBuilder;
        if (field.numberOfFragments() == 0) {
            fragListBuilder = new SingleFragListBuilder();
            if (fieldMapper.stored()) {
                fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
            } else {
                fragmentsBuilder = new SourceSimpleFragmentsBuilder(fieldMapper, searchContext, field.preTags(), field.postTags());
            }
        } else {
            fragListBuilder = new SimpleFragListBuilder();
            if (field.scoreOrdered()) {
                if (fieldMapper.stored()) {
                    fragmentsBuilder = new ScoreOrderFragmentsBuilder(field.preTags(), field.postTags());
                } else {
                    fragmentsBuilder = new SourceScoreOrderFragmentsBuilder(fieldMapper, searchContext, field.preTags(), field.postTags());
                }
            } else {
                if (fieldMapper.stored()) {
                    fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
                } else {
                    fragmentsBuilder = new SourceSimpleFragmentsBuilder(fieldMapper, searchContext, field.preTags(), field.postTags());
                }
            }
        }

        return new FastVectorHighlighter(true, false, fragListBuilder, fragmentsBuilder);
    }
}
