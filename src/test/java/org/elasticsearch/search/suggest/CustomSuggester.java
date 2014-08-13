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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class CustomSuggester extends Suggester<CustomSuggester.CustomSuggestionsContext> {


    // This is a pretty dumb implementation which returns the original text + fieldName + custom config option + 12 or 123
    @Override
    public Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name, CustomSuggestionsContext suggestion, IndexReader indexReader, CharsRefBuilder spare) throws IOException {
        // Get the suggestion context
        String text = suggestion.getText().utf8ToString();

        // create two suggestions with 12 and 123 appended
        Suggest.Suggestion<Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option>> response = new Suggest.Suggestion<>(name, suggestion.getSize());

        String firstSuggestion = String.format(Locale.ROOT, "%s-%s-%s-%s", text, suggestion.getField(), suggestion.options.get("suffix"), "12");
        Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option> resultEntry12 = new Suggest.Suggestion.Entry<>(new StringText(firstSuggestion), 0, text.length() + 2);
        response.addTerm(resultEntry12);

        String secondSuggestion = String.format(Locale.ROOT, "%s-%s-%s-%s", text, suggestion.getField(), suggestion.options.get("suffix"), "123");
        Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option> resultEntry123 = new Suggest.Suggestion.Entry<>(new StringText(secondSuggestion), 0, text.length() + 3);
        response.addTerm(resultEntry123);

        return response;
    }

    @Override
    public String[] names() {
        return new String[] {"custom"};
    }

    @Override
    public SuggestContextParser getContextParser() {
        return new SuggestContextParser() {
            @Override
            public SuggestionSearchContext.SuggestionContext parse(XContentParser parser, MapperService mapperService) throws IOException {
                Map<String, Object> options = parser.map();
                CustomSuggestionsContext suggestionContext = new CustomSuggestionsContext(CustomSuggester.this, options);
                suggestionContext.setField((String) options.get("field"));
                return suggestionContext;
            }
        };
    }

    public static class CustomSuggestionsContext extends SuggestionSearchContext.SuggestionContext {

        public Map<String, Object> options;

        public CustomSuggestionsContext(Suggester suggester, Map<String, Object> options) {
            super(suggester);
            this.options = options;
        }
    }
}
