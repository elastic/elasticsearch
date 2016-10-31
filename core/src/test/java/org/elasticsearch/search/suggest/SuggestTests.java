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

package org.elasticsearch.search.suggest;

import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class SuggestTests extends ESTestCase {

    public void testFilter() throws Exception {
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions;
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(randomAsciiOfLength(10), 2);
        PhraseSuggestion phraseSuggestion = new PhraseSuggestion(randomAsciiOfLength(10), 2);
        TermSuggestion termSuggestion = new TermSuggestion(randomAsciiOfLength(10), 2, SortBy.SCORE);
        suggestions = Arrays.asList(completionSuggestion, phraseSuggestion, termSuggestion);
        Suggest suggest = new Suggest(suggestions);
        List<PhraseSuggestion> phraseSuggestions = suggest.filter(PhraseSuggestion.class);
        assertThat(phraseSuggestions.size(), equalTo(1));
        assertThat(phraseSuggestions.get(0), equalTo(phraseSuggestion));
        List<TermSuggestion> termSuggestions = suggest.filter(TermSuggestion.class);
        assertThat(termSuggestions.size(), equalTo(1));
        assertThat(termSuggestions.get(0), equalTo(termSuggestion));
        List<CompletionSuggestion> completionSuggestions = suggest.filter(CompletionSuggestion.class);
        assertThat(completionSuggestions.size(), equalTo(1));
        assertThat(completionSuggestions.get(0), equalTo(completionSuggestion));
    }

    public void testSuggestionOrdering() throws Exception {
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions;
        suggestions = new ArrayList<>();
        int n = randomIntBetween(2, 5);
        for (int i = 0; i < n; i++) {
            suggestions.add(new CompletionSuggestion(randomAsciiOfLength(10), randomIntBetween(3, 5)));
        }
        Collections.shuffle(suggestions, random());
        Suggest suggest = new Suggest(suggestions);
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> sortedSuggestions;
        sortedSuggestions = new ArrayList<>(suggestions);
        sortedSuggestions.sort((o1, o2) -> o1.getName().compareTo(o2.getName()));
        List<CompletionSuggestion> completionSuggestions = suggest.filter(CompletionSuggestion.class);
        assertThat(completionSuggestions.size(), equalTo(n));
        for (int i = 0; i < n; i++) {
            assertThat(completionSuggestions.get(i).getName(), equalTo(sortedSuggestions.get(i).getName()));
        }
    }

}
