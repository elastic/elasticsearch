/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.example.customsuggester;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggester;

import java.io.IOException;
import java.util.Locale;

public class CustomSuggester extends Suggester<CustomSuggestionContext> {

    // This is a pretty dumb implementation which returns the original text + fieldName + custom config option + 12 or 123
    @Override
    public Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(
        String name,
        CustomSuggestionContext suggestion,
        IndexSearcher searcher,
        CharsRefBuilder spare) throws IOException {

        // create two suggestions with 12 and 123 appended
        CustomSuggestion response = emptySuggestion(name, suggestion, spare);
        CustomSuggestion.Entry entry = response.getEntries().get(0);
        String text = entry.getText().string();

        String firstOption =
            String.format(Locale.ROOT, "%s-%s-%s-%s", text, suggestion.getField(), suggestion.options.get("suffix"), "12");
        CustomSuggestion.Entry.Option option12 = new CustomSuggestion.Entry.Option(new Text(firstOption), 0.9f, "option-dummy-value-1");
        entry.addOption(option12);

        String secondOption =
            String.format(Locale.ROOT, "%s-%s-%s-%s", text, suggestion.getField(), suggestion.options.get("suffix"), "123");
        CustomSuggestion.Entry.Option option123 = new CustomSuggestion.Entry.Option(new Text(secondOption), 0.8f, "option-dummy-value-2");
        entry.addOption(option123);

        return response;
    }

    @Override
    protected CustomSuggestion emptySuggestion(String name, CustomSuggestionContext suggestion,
            CharsRefBuilder spare) throws IOException {
        String text = suggestion.getText().utf8ToString();
        CustomSuggestion response = new CustomSuggestion(name, suggestion.getSize(), "suggestion-dummy-value");
        CustomSuggestion.Entry entry = new CustomSuggestion.Entry(new Text(text), 0, text.length(), "entry-dummy-value");
        response.addTerm(entry);
        return response;
    }
}
