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

import com.google.common.collect.Maps;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class CompletionSuggester extends Suggester<CompletionSuggestionContext> {

    private static final ScoreComparator scoreComparator = new ScoreComparator();


    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name,
            CompletionSuggestionContext suggestionContext, IndexReader indexReader, CharsRefBuilder spare) throws IOException {
        if (suggestionContext.mapper() == null || !(suggestionContext.mapper() instanceof CompletionFieldMapper)) {
            throw new ElasticsearchException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        }

        CompletionSuggestion completionSuggestion = new CompletionSuggestion(name, suggestionContext.getSize());
        spare.copyUTF8Bytes(suggestionContext.getText());

        CompletionSuggestion.Entry completionSuggestEntry = new CompletionSuggestion.Entry(new StringText(spare.toString()), 0, spare.length());
        completionSuggestion.addTerm(completionSuggestEntry);

        String fieldName = suggestionContext.getField();
        Map<String, CompletionSuggestion.Entry.Option> results = Maps.newHashMapWithExpectedSize(indexReader.leaves().size() * suggestionContext.getSize());
        for (LeafReaderContext atomicReaderContext : indexReader.leaves()) {
            LeafReader atomicReader = atomicReaderContext.reader();
            Terms terms = atomicReader.fields().terms(fieldName);
            if (terms instanceof Completion090PostingsFormat.CompletionTerms) {
                final Completion090PostingsFormat.CompletionTerms lookupTerms = (Completion090PostingsFormat.CompletionTerms) terms;
                final Lookup lookup = lookupTerms.getLookup(suggestionContext.mapper(), suggestionContext);
                if (lookup == null) {
                    // we don't have a lookup for this segment.. this might be possible if a merge dropped all
                    // docs from the segment that had a value in this segment.
                    continue;
                }
                List<Lookup.LookupResult> lookupResults = lookup.lookup(spare.get(), false, suggestionContext.getSize());
                for (Lookup.LookupResult res : lookupResults) {

                    final String key = res.key.toString();
                    final float score = res.value;
                    final Option value = results.get(key);
                    if (value == null) {
                        final Option option = new CompletionSuggestion.Entry.Option(new StringText(key), score, res.payload == null ? null
                                : new BytesArray(res.payload));
                        results.put(key, option);
                    } else if (value.getScore() < score) {
                        value.setScore(score);
                        value.setPayload(res.payload == null ? null : new BytesArray(res.payload));
                    }
                }
            }
        }
        final List<CompletionSuggestion.Entry.Option> options = new ArrayList<>(results.values());
        CollectionUtil.introSort(options, scoreComparator);

        int optionCount = Math.min(suggestionContext.getSize(), options.size());
        for (int i = 0 ; i < optionCount ; i++) {
            completionSuggestEntry.addOption(options.get(i));
        }

        return completionSuggestion;
    }

    @Override
    public String[] names() {
        return new String[] { "completion" };
    }

    @Override
    public SuggestContextParser getContextParser() {
        return new CompletionSuggestParser(this);
    }

    public static class ScoreComparator implements Comparator<CompletionSuggestion.Entry.Option> {
        @Override
        public int compare(Option o1, Option o2) {
            return Float.compare(o2.getScore(), o1.getScore());
        }
    }
}
