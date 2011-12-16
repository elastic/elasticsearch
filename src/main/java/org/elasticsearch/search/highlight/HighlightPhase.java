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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.vectorhighlight.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.document.SingleFieldSelector;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.highlight.vectorhighlight.SourceScoreOrderFragmentsBuilder;
import org.elasticsearch.search.highlight.vectorhighlight.SourceSimpleFragmentsBuilder;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.*;

import static com.google.common.collect.Maps.newHashMap;

/**
 *
 */
public class HighlightPhase implements FetchSubPhase {

    public static class Encoders {
        public static Encoder DEFAULT = new DefaultEncoder();
        public static Encoder HTML = new SimpleHTMLEncoder();
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("highlight", new HighlighterParseElement());
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticSearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.highlight() != null;
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        // we use a cache to cache heavy things, mainly the rewrite in FieldQuery for FVH
        Map<FieldMapper, HighlightEntry> cache = (Map<FieldMapper, HighlightEntry>) hitContext.cache().get("highlight");
        if (cache == null) {
            cache = Maps.newHashMap();
            hitContext.cache().put("highlight", cache);
        }

        DocumentMapper documentMapper = context.mapperService().documentMapper(hitContext.hit().type());

        Map<String, HighlightField> highlightFields = newHashMap();
        for (SearchContextHighlight.Field field : context.highlight().fields()) {
            Encoder encoder;
            if (field.encoder().equals("html")) {
                encoder = Encoders.HTML;
            } else {
                encoder = Encoders.DEFAULT;
            }
            FieldMapper mapper = documentMapper.mappers().smartNameFieldMapper(field.field());
            if (mapper == null) {
                MapperService.SmartNameFieldMappers fullMapper = context.mapperService().smartName(field.field());
                if (fullMapper == null || !fullMapper.hasDocMapper()) {
                    //Save skipping missing fields
                    continue;
                }
                if (!fullMapper.docMapper().type().equals(hitContext.hit().type())) {
                    continue;
                }
                mapper = fullMapper.mapper();
                if (mapper == null) {
                    continue;
                }
            }

            // if we can do highlighting using Term Vectors, use FastVectorHighlighter, otherwise, use the
            // slower plain highlighter
            if (mapper.termVector() != Field.TermVector.WITH_POSITIONS_OFFSETS) {
                HighlightEntry entry = cache.get(mapper);
                if (entry == null) {
                    // Don't use the context.query() since it might be rewritten, and we need to pass the non rewritten queries to
                    // let the highlighter handle MultiTerm ones

                    // QueryScorer uses WeightedSpanTermExtractor to extract terms, but we can't really plug into
                    // it, so, we hack here (and really only support top level queries)
                    Query query = context.parsedQuery().query();
                    while (true) {
                        boolean extracted = false;
                        if (query instanceof FunctionScoreQuery) {
                            query = ((FunctionScoreQuery) query).getSubQuery();
                            extracted = true;
                        } else if (query instanceof FiltersFunctionScoreQuery) {
                            query = ((FiltersFunctionScoreQuery) query).getSubQuery();
                            extracted = true;
                        } else if (query instanceof ConstantScoreQuery) {
                            ConstantScoreQuery q = (ConstantScoreQuery) query;
                            if (q.getQuery() != null) {
                                query = q.getQuery();
                                extracted = true;
                            }
                        }
                        if (!extracted) {
                            break;
                        }
                    }

                    QueryScorer queryScorer = new QueryScorer(query, null);
                    queryScorer.setExpandMultiTermQuery(true);
                    Fragmenter fragmenter;
                    if (field.numberOfFragments() == 0) {
                        fragmenter = new NullFragmenter();
                    } else {
                        fragmenter = new SimpleSpanFragmenter(queryScorer, field.fragmentCharSize());
                    }
                    Formatter formatter = new SimpleHTMLFormatter(field.preTags()[0], field.postTags()[0]);


                    entry = new HighlightEntry();
                    entry.highlighter = new Highlighter(formatter, encoder, queryScorer);
                    entry.highlighter.setTextFragmenter(fragmenter);

                    cache.put(mapper, entry);
                }

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
                    textsToHighlight = lookup.source().extractRawValues(mapper.names().fullName());
                }

                // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                ArrayList<TextFragment> fragsList = new ArrayList<TextFragment>();
                try {
                    for (Object textToHighlight : textsToHighlight) {
                        String text = textToHighlight.toString();
                        Analyzer analyzer = context.mapperService().documentMapper(hitContext.hit().type()).mappers().indexAnalyzer();
                        TokenStream tokenStream = analyzer.reusableTokenStream(mapper.names().indexName(), new FastStringReader(text));
                        TextFragment[] bestTextFragments = entry.highlighter.getBestTextFragments(tokenStream, text, false, numberOfFragments);
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
                String[] fragments = null;
                // number_of_fragments is set to 0 but we have a multivalued field
                if (field.numberOfFragments() == 0 && textsToHighlight.size() > 1 && fragsList.size() > 0) {
                    fragments = new String[1];
                    for (int i = 0; i < fragsList.size(); i++) {
                        fragments[0] = (fragments[0] != null ? (fragments[0] + " ") : "") + fragsList.get(i).toString();
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
                    HighlightField highlightField = new HighlightField(field.field(), fragments);
                    highlightFields.put(highlightField.name(), highlightField);
                }
            } else {
                try {
                    HighlightEntry entry = cache.get(mapper);
                    if (entry == null) {
                        FragListBuilder fragListBuilder;
                        FragmentsBuilder fragmentsBuilder;
                        if (field.numberOfFragments() == 0) {
                            fragListBuilder = new SingleFragListBuilder();

                            if (mapper.stored()) {
                                fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
                            } else {
                                fragmentsBuilder = new SourceSimpleFragmentsBuilder(mapper, context, field.preTags(), field.postTags());
                            }
                        } else {
                            if (field.fragmentOffset() == -1)
                                fragListBuilder = new SimpleFragListBuilder();
                            else
                                fragListBuilder = new SimpleFragListBuilder(field.fragmentOffset());

                            if (field.scoreOrdered()) {
                                if (mapper.stored()) {
                                    fragmentsBuilder = new ScoreOrderFragmentsBuilder(field.preTags(), field.postTags());
                                } else {
                                    fragmentsBuilder = new SourceScoreOrderFragmentsBuilder(mapper, context, field.preTags(), field.postTags());
                                }
                            } else {
                                if (mapper.stored()) {
                                    fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
                                } else {
                                    fragmentsBuilder = new SourceSimpleFragmentsBuilder(mapper, context, field.preTags(), field.postTags());
                                }
                            }
                        }
                        entry = new HighlightEntry();
                        entry.fragListBuilder = fragListBuilder;
                        entry.fragmentsBuilder = fragmentsBuilder;
                        entry.fvh = new FastVectorHighlighter(true, false, fragListBuilder, fragmentsBuilder);
                        CustomFieldQuery.highlightFilters.set(field.highlightFilter());
                        // we use top level reader to rewrite the query against all readers, with use caching it across hits (and across readers...)
                        entry.fieldQuery = new CustomFieldQuery(context.parsedQuery().query(), hitContext.topLevelReader(), entry.fvh);

                        cache.put(mapper, entry);
                    }

                    String[] fragments;

                    // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                    int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                    // we highlight against the low level reader and docId, because if we load source, we want to reuse it if possible
                    fragments = entry.fvh.getBestFragments(entry.fieldQuery, hitContext.reader(), hitContext.docId(), mapper.names().indexName(), field.fragmentCharSize(), numberOfFragments,
                            entry.fragListBuilder, entry.fragmentsBuilder, field.preTags(), field.postTags(), encoder);

                    if (fragments != null && fragments.length > 0) {
                        HighlightField highlightField = new HighlightField(field.field(), fragments);
                        highlightFields.put(highlightField.name(), highlightField);
                    }
                } catch (Exception e) {
                    throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                }
            }
        }

        hitContext.hit().highlightFields(highlightFields);
    }

    static class HighlightEntry {
        public FastVectorHighlighter fvh;
        public FieldQuery fieldQuery;
        public FragListBuilder fragListBuilder;
        public FragmentsBuilder fragmentsBuilder;

        public Highlighter highlighter;
    }
}
