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

import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

/**
 * A static factory for building suggester lookup queries
 */
public abstract class SuggestBuilders {

    /**
     * Creates a term suggestion lookup query with the provided <code>field</code>
     *
     * @return a {@link org.elasticsearch.search.suggest.term.TermSuggestionBuilder}
     * instance
     */
    public static TermSuggestionBuilder termSuggestion(String fieldname) {
        return new TermSuggestionBuilder(fieldname);
    }

    /**
     * Creates a phrase suggestion lookup query with the provided <code>field</code>
     *
     * @return a {@link org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder}
     * instance
     */
    public static PhraseSuggestionBuilder phraseSuggestion(String fieldname) {
        return new PhraseSuggestionBuilder(fieldname);
    }

    /**
     * Creates a completion suggestion lookup query with the provided <code>field</code>
     *
     * @return a {@link org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder}
     * instance
     */
    public static CompletionSuggestionBuilder completionSuggestion(String fieldname) {
        return new CompletionSuggestionBuilder(fieldname);
    }
}
