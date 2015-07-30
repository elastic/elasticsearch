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
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.xdocument.CompletionQuery;
import org.apache.lucene.search.suggest.xdocument.TopSuggestDocs;
import org.apache.lucene.search.suggest.xdocument.TopSuggestDocsCollector;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.io.IOException;
import java.util.*;

public class CompletionSuggester extends Suggester<CompletionSuggestionContext> {

    @Override
    public String[] names() {
        return new String[] { "completion" };
    }

    @Override
    public SuggestContextParser getContextParser() {
        return new CompletionSuggestParser(this);
    }

    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name,
            CompletionSuggestionContext suggestionContext, IndexSearcher searcher, CharsRefBuilder spare) throws IOException {
        if (suggestionContext.fieldType() == null) {
            throw new ElasticsearchException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        }
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(name, suggestionContext.getSize());
        spare.copyUTF8Bytes(suggestionContext.getText());
        CompletionSuggestion.Entry completionSuggestEntry = new CompletionSuggestion.Entry(new StringText(spare.toString()), 0, spare.length());
        completionSuggestion.addTerm(completionSuggestEntry);

        Map<String, Option> results = new LinkedHashMap<>(suggestionContext.getSize());
        TopSuggestDocsCollector collector = new TopSuggestDocsCollector(suggestionContext.getSize());
        suggest(searcher, toQuery(suggestionContext), collector);
        for (TopSuggestDocs.SuggestScoreDoc suggestDoc : collector.get().scoreLookupDocs()) {
            final String key = suggestDoc.key.toString();
            final float score = suggestDoc.score;
            final Map<String, CharSequence> contexts;
            if (suggestionContext.fieldType().hasContextMappings()) {
                contexts = suggestionContext.fieldType().getContextMappings().buildContextValueMap(suggestDoc.contexts);
            } else {
                assert suggestDoc.contexts == null;
                contexts = null;
            }
            final Option value = results.get(key);
            if (value == null) {
                final Option option = new Option(suggestDoc.doc, new StringText(key), score, contexts);
                results.put(key, option);
            } else {
                value.addContextMap(contexts);
                if (value.getScore() < score) {
                    value.setScore(score);
                }
            }
        }
        final List<Option> options = new ArrayList<>(results.values());
        int optionCount = Math.min(suggestionContext.getSize(), options.size());
        for (int i = 0 ; i < optionCount ; i++) {
            completionSuggestEntry.addOption(options.get(i));
        }
        return completionSuggestion;
    }

    /*
    TODO: should this be moved to CompletionSuggestionParser?
    so the CompletionSuggestionContext will have only the query instead
     */
    private CompletionQuery toQuery(CompletionSuggestionContext suggestionContext) throws IOException {
        CompletionFieldMapper.CompletionFieldType fieldType = suggestionContext.fieldType();
        final CompletionQuery query;
        if (suggestionContext.getPrefix() != null) {
            if (suggestionContext.getFuzzyOptionsBuilder() != null) {
                CompletionSuggestionBuilder.FuzzyOptionsBuilder fuzzyOptions = suggestionContext.getFuzzyOptionsBuilder();
                query = fieldType.fuzzyQuery(suggestionContext.getPrefix().utf8ToString(),
                        Fuzziness.fromEdits(fuzzyOptions.getEditDistance()),
                        fuzzyOptions.getFuzzyPrefixLength(), fuzzyOptions.getFuzzyMinLength(),
                        fuzzyOptions.getMaxDeterminizedStates(), fuzzyOptions.isTranspositions(),
                        fuzzyOptions.isUnicodeAware());
            } else {
                query = fieldType.prefixQuery(suggestionContext.getPrefix());
            }
        } else if (suggestionContext.getRegex() != null) {
            if (suggestionContext.getFuzzyOptionsBuilder() != null) {
                throw new IllegalArgumentException("can not use 'fuzzy' options with 'regex");
            }
            CompletionSuggestionBuilder.RegexOptionsBuilder regexOptions = suggestionContext.getRegexOptionsBuilder();
            if (regexOptions == null) {
                regexOptions = new CompletionSuggestionBuilder.RegexOptionsBuilder();
            }
            query = fieldType.regexpQuery(suggestionContext.getRegex(), regexOptions.getFlagsValue(),
                    regexOptions.getMaxDeterminizedStates());
        } else {
            throw new IllegalArgumentException("'prefix' or 'regex' must be defined");
        }
        if (fieldType.hasContextMappings()) {
            ContextMappings contextMappings = fieldType.getContextMappings();
            return contextMappings.toContextQuery(query, suggestionContext.getQueryContexts());
        }
        return query;
    }

    private static void suggest(IndexSearcher searcher, CompletionQuery query, TopSuggestDocsCollector collector) throws IOException {
        query = (CompletionQuery) query.rewrite(searcher.getIndexReader());
        Weight weight = query.createWeight(searcher, collector.needsScores());
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            BulkScorer scorer = weight.bulkScorer(context, context.reader().getLiveDocs());
            if (scorer != null) {
                try {
                    scorer.score(collector.getLeafCollector(context));
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        }
    }
}
