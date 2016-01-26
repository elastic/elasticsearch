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

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines how to perform suggesting. This builders allows a number of global options to be specified and
 * an arbitrary number of {@link org.elasticsearch.search.suggest.term.TermSuggestionBuilder} instances.
 * <p>
 * Suggesting works by suggesting terms that appear in the suggest text that are similar compared to the terms in
 * provided text. These spelling suggestions are based on several options described in this class.
 */
public class SuggestBuilder extends ToXContentToBytes {

    private final String name;
    private String globalText;

    private final List<SuggestionBuilder<?>> suggestions = new ArrayList<>();

    public SuggestBuilder() {
        this.name = null;
    }

    public SuggestBuilder(String name) {
        this.name = name;
    }

    /**
     * Sets the text to provide suggestions for. The suggest text is a required option that needs
     * to be set either via this setter or via the {@link org.elasticsearch.search.suggest.SuggestionBuilder#text(String)} method.
     * <p>
     * The suggest text gets analyzed by the suggest analyzer or the suggest field search analyzer.
     * For each analyzed token, suggested terms are suggested if possible.
     */
    public SuggestBuilder setText(String globalText) {
        this.globalText = globalText;
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.search.suggest.term.TermSuggestionBuilder} instance under a user defined name.
     * The order in which the <code>Suggestions</code> are added, is the same as in the response.
     */
    public SuggestBuilder addSuggestion(SuggestionBuilder<?> suggestion) {
        suggestions.add(suggestion);
        return this;
    }

    /**
     * Returns all suggestions with the defined names.
     */
    public List<SuggestionBuilder<?>> getSuggestion() {
        return suggestions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if(name == null) {
            builder.startObject();
        } else {
            builder.startObject(name);
        }

        if (globalText != null) {
            builder.field("text", globalText);
        }
        for (SuggestionBuilder<?> suggestion : suggestions) {
            builder = suggestion.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
