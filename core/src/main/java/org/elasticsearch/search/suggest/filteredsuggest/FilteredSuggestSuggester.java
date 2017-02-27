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
package org.elasticsearch.search.suggest.filteredsuggest;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.completion.CompletionSuggester.TopDocumentsCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilteredSuggestSuggester extends Suggester<FilteredSuggestSuggestionContext> {

    public static final FilteredSuggestSuggester INSTANCE = new FilteredSuggestSuggester();

    private FilteredSuggestSuggester() {
    }

    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name,
            final FilteredSuggestSuggestionContext suggestionContext, final IndexSearcher searcher, CharsRefBuilder spare)
            throws IOException {
        if (suggestionContext.getFieldType() != null) {
            FilteredSuggestSuggestion completionSuggestion = new FilteredSuggestSuggestion(name, suggestionContext.getSize());
            spare.copyUTF8Bytes(suggestionContext.getText());
            Map<String, FilteredSuggestSuggestion.Entry.Option> results = new HashMap<>(suggestionContext.getSize());

            FilteredSuggestSuggestion.Entry completionSuggestEntry = new FilteredSuggestSuggestion.Entry(new Text(spare.toString()), 0,
                    spare.length());
            completionSuggestion.addTerm(completionSuggestEntry);

            // TODO scoring catch here : scoring will be done per query , as we
            // have to intersect the results for the queries,
            // though we score and pick the first n results per query, we will
            // will not be able to score across filters.

            Set<String> finalKeySet = null;
            Set<String> keys = new HashSet<>();
            for (CompletionQuery compQuery : suggestionContext.toQueries()) {
                TopSuggestDocsCollector collector = new TopDocumentsCollector(suggestionContext.getSize());
                CompletionSuggester.suggest(searcher, compQuery, collector);

                keys.clear();
                for (TopSuggestDocs.SuggestScoreDoc suggestScoreDoc : collector.get().scoreLookupDocs()) {
                    TopDocumentsCollector.SuggestDoc suggestDoc = (TopDocumentsCollector.SuggestDoc) suggestScoreDoc;
                    // this code is to collect the contexts, we do not need them
                    // // collect contexts
                    // Map<String, Set<CharSequence>> contexts =
                    // Collections.emptyMap();
                    // if (fieldType.hasFilterMappings() &&
                    // suggestDoc.getContexts().isEmpty() == false) {
                    // contexts =
                    // fieldType.getFilterMappings().getNamedContexts(suggestDoc.getContexts());
                    // }

                    // Map<String, Set<CharSequence>> contexts =
                    // Collections.emptyMap();
                    // ENTRY.OPTION for CompletionSuggest has a field called
                    // context , which in our case is irrelevant.

                    for (CharSequence matchedKey : suggestDoc.getKeys()) {
                        final String key = matchedKey.toString();
                        final float score = suggestDoc.score;
                        final FilteredSuggestSuggestion.Entry.Option value = results.get(key);
                        if (value == null) {
                            FilteredSuggestSuggestion.Entry.Option option = new FilteredSuggestSuggestion.Entry.Option(suggestDoc.doc,
                                    new Text(key), suggestDoc.score);
                            results.put(key, option);
                            keys.add(key);
                        } else if (value.getScore() < score) {
                            results.put(key, new FilteredSuggestSuggestion.Entry.Option(suggestDoc.doc, new Text(key), suggestDoc.score));
                            keys.add(key);
                        } else {
                            keys.add(key);
                        }
                    }
                }

                // across filters its an AND like operation hence retain only
                // those which exist as results across all filters
                if (finalKeySet != null) {
                    finalKeySet.retainAll(keys);
                } else {
                    finalKeySet = new HashSet<>();
                    finalKeySet.addAll(keys);
                }
            }

            // retain filter level intersected results
            results.keySet().retainAll(finalKeySet);

            final List<FilteredSuggestSuggestion.Entry.Option> options = new ArrayList<>(results.values());
            CollectionUtil.introSort(options, Suggest.COMPARATOR);

            int optionCount = Math.min(suggestionContext.getSize(), options.size());
            for (int i = 0; i < optionCount; i++) {
                completionSuggestEntry.addOption(options.get(i));
            }
            return completionSuggestion;
        }
        return null;
    }
}
