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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoringRewrite;
import org.apache.lucene.search.TopTermsRewrite;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.postingshighlight.CustomPassageFormatter;
import org.apache.lucene.search.postingshighlight.CustomPostingsHighlighter;
import org.apache.lucene.search.postingshighlight.Snippet;
import org.apache.lucene.search.postingshighlight.WholeBreakIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class PostingsHighlighter implements Highlighter {

    private static final String CACHE_KEY = "highlight-postings";

    @Override
    public String[] names() {
        return new String[]{"postings", "postings-highlighter"};
    }

    @Override
    public HighlightField highlight(HighlighterContext highlighterContext) {

        FieldMapper<?> fieldMapper = highlighterContext.mapper;
        SearchContextHighlight.Field field = highlighterContext.field;
        if (fieldMapper.fieldType().indexOptions() != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
            throw new ElasticsearchIllegalArgumentException("the field [" + highlighterContext.fieldName + "] should be indexed with positions and offsets in the postings list to be used with postings highlighter");
        }

        SearchContext context = highlighterContext.context;
        FetchSubPhase.HitContext hitContext = highlighterContext.hitContext;

        if (!hitContext.cache().containsKey(CACHE_KEY)) {
            //get the non rewritten query and rewrite it
            Query query;
            try {
                query = rewrite(highlighterContext, hitContext.topLevelReader());
                SortedSet<Term> queryTerms = extractTerms(context.searcher().createNormalizedWeight(query, false));
                hitContext.cache().put(CACHE_KEY, new HighlighterEntry(queryTerms));
            } catch (IOException e) {
                throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
            }
        }

        HighlighterEntry highlighterEntry = (HighlighterEntry) hitContext.cache().get(CACHE_KEY);
        MapperHighlighterEntry mapperHighlighterEntry = highlighterEntry.mappers.get(fieldMapper);

        if (mapperHighlighterEntry == null) {
            Encoder encoder = field.fieldOptions().encoder().equals("html") ? HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;
            CustomPassageFormatter passageFormatter = new CustomPassageFormatter(field.fieldOptions().preTags()[0], field.fieldOptions().postTags()[0], encoder);
            BytesRef[] filteredQueryTerms = filterTerms(highlighterEntry.queryTerms, fieldMapper.names().indexName(), field.fieldOptions().requireFieldMatch());
            mapperHighlighterEntry = new MapperHighlighterEntry(passageFormatter, filteredQueryTerms);
        }

        //we merge back multiple values into a single value using the paragraph separator, unless we have to highlight every single value separately (number_of_fragments=0).
        boolean mergeValues = field.fieldOptions().numberOfFragments() != 0;
        List<Snippet> snippets = new ArrayList<>();
        int numberOfFragments;

        try {
            //we manually load the field values (from source if needed)
            List<Object> textsToHighlight = HighlightUtils.loadFieldValues(field, fieldMapper, context, hitContext);
            CustomPostingsHighlighter highlighter = new CustomPostingsHighlighter(mapperHighlighterEntry.passageFormatter, textsToHighlight, mergeValues, Integer.MAX_VALUE-1, field.fieldOptions().noMatchSize());

             if (field.fieldOptions().numberOfFragments() == 0) {
                highlighter.setBreakIterator(new WholeBreakIterator());
                numberOfFragments = 1; //1 per value since we highlight per value
            } else {
                numberOfFragments = field.fieldOptions().numberOfFragments();
            }

            //we highlight every value separately calling the highlight method multiple times, only if we need to have back a snippet per value (whole value)
            int values = mergeValues ? 1 : textsToHighlight.size();
            for (int i = 0; i < values; i++) {
                Snippet[] fieldSnippets = highlighter.highlightDoc(fieldMapper.names().indexName(), mapperHighlighterEntry.filteredQueryTerms, hitContext.searcher(), hitContext.docId(), numberOfFragments);
                if (fieldSnippets != null) {
                    for (Snippet fieldSnippet : fieldSnippets) {
                        if (Strings.hasText(fieldSnippet.getText())) {
                            snippets.add(fieldSnippet);
                        }
                    }
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

    private static Query rewrite(HighlighterContext highlighterContext, IndexReader reader) throws IOException {

        Query original = highlighterContext.query.originalQuery();

        //we walk the query tree and when we encounter multi term queries we need to make sure the rewrite method
        //supports multi term extraction. If not we temporarily override it (and restore it after the rewrite).
        List<Tuple<MultiTermQuery, MultiTermQuery.RewriteMethod>> modifiedMultiTermQueries = Lists.newArrayList();
        overrideMultiTermRewriteMethod(original, modifiedMultiTermQueries);

        //rewrite is expensive: if the query was already rewritten we try not to rewrite it again
        if (highlighterContext.query.queryRewritten() && modifiedMultiTermQueries.size() == 0) {
            //return the already rewritten query
            return highlighterContext.query.query();
        }

        Query query = original;
        for (Query rewrittenQuery = query.rewrite(reader); rewrittenQuery != query;
             rewrittenQuery = query.rewrite(reader)) {
            query = rewrittenQuery;
        }

        //set back the original rewrite method after the rewrite is done
        for (Tuple<MultiTermQuery, MultiTermQuery.RewriteMethod> modifiedMultiTermQuery : modifiedMultiTermQueries) {
            modifiedMultiTermQuery.v1().setRewriteMethod(modifiedMultiTermQuery.v2());
        }

        return query;
    }

    private static void overrideMultiTermRewriteMethod(Query query, List<Tuple<MultiTermQuery, MultiTermQuery.RewriteMethod>> modifiedMultiTermQueries) {

        if (query instanceof  MultiTermQuery) {
            MultiTermQuery originalMultiTermQuery = (MultiTermQuery) query;
            if (!allowsForTermExtraction(originalMultiTermQuery.getRewriteMethod())) {
                MultiTermQuery.RewriteMethod originalRewriteMethod = originalMultiTermQuery.getRewriteMethod();
                originalMultiTermQuery.setRewriteMethod(new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(50));
                //we need to rewrite anyway if it is a multi term query which was rewritten with the wrong rewrite method
                modifiedMultiTermQueries.add(Tuple.tuple(originalMultiTermQuery, originalRewriteMethod));
            }
        }

        if (query instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery) query;
            for (BooleanClause booleanClause : booleanQuery) {
                overrideMultiTermRewriteMethod(booleanClause.getQuery(), modifiedMultiTermQueries);
            }
        }

        if (query instanceof FilteredQuery) {
            overrideMultiTermRewriteMethod(((FilteredQuery) query).getQuery(), modifiedMultiTermQueries);
        }

        if (query instanceof ConstantScoreQuery) {
            overrideMultiTermRewriteMethod(((ConstantScoreQuery) query).getQuery(), modifiedMultiTermQueries);
        }
    }

    private static boolean allowsForTermExtraction(MultiTermQuery.RewriteMethod rewriteMethod) {
        return rewriteMethod instanceof TopTermsRewrite || rewriteMethod instanceof ScoringRewrite;
    }

    private static SortedSet<Term> extractTerms(Weight weight) {
        SortedSet<Term> queryTerms = new TreeSet<>();
        weight.extractTerms(queryTerms);
        return queryTerms;
    }

    private static BytesRef[] filterTerms(SortedSet<Term> queryTerms, String field, boolean requireFieldMatch) {
        SortedSet<Term> fieldTerms;
        if (requireFieldMatch) {
            Term floor = new Term(field, "");
            Term ceiling = new Term(field, UnicodeUtil.BIG_TERM);
            fieldTerms = queryTerms.subSet(floor, ceiling);
        } else {
            fieldTerms = queryTerms;
        }

        BytesRef terms[] = new BytesRef[fieldTerms.size()];
        int termUpto = 0;
        for(Term term : fieldTerms) {
            terms[termUpto++] = term.bytes();
        }

        return terms;
    }

    private static List<Snippet> filterSnippets(List<Snippet> snippets, int numberOfFragments) {

        //We need to filter the snippets as due to no_match_size we could have
        //either highlighted snippets together non highlighted ones
        //We don't want to mix those up
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
                //if we did discrete per value highlighting using whole break iterator (as number_of_fragments was 0)
                //we need to obtain the first sentence of the first value
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
        final SortedSet<Term> queryTerms;
        Map<FieldMapper<?>, MapperHighlighterEntry> mappers = Maps.newHashMap();

        private HighlighterEntry(SortedSet<Term> queryTerms) {
            this.queryTerms = queryTerms;
        }
    }

    private static class MapperHighlighterEntry {
        final CustomPassageFormatter passageFormatter;
        final BytesRef[] filteredQueryTerms;

        private MapperHighlighterEntry(CustomPassageFormatter passageFormatter, BytesRef[] filteredQueryTerms) {
            this.passageFormatter = passageFormatter;
            this.filteredQueryTerms = filteredQueryTerms;
        }
    }
}
