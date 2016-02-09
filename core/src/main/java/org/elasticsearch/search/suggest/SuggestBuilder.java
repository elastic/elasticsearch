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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Defines how to perform suggesting. This builders allows a number of global options to be specified and
 * an arbitrary number of {@link SuggestionBuilder} instances.
 * <p>
 * Suggesting works by suggesting terms/phrases that appear in the suggest text that are similar compared
 * to the terms in provided text. These suggestions are based on several options described in this class.
 */
public class SuggestBuilder extends ToXContentToBytes implements Writeable<SuggestBuilder> {

    public static final SuggestBuilder PROTOTYPE = new SuggestBuilder();
    protected static final ParseField GLOBAL_TEXT_FIELD = new ParseField("text");

    private String globalText;
    private final List<SuggestionBuilder<?>> suggestions = new ArrayList<>();

    public SuggestBuilder() {
    }

    /**
     * Sets the text to provide suggestions for. The suggest text is a required option that needs
     * to be set either via this setter or via the {@link org.elasticsearch.search.suggest.SuggestionBuilder#text(String)} method.
     * <p>
     * The suggest text gets analyzed by the suggest analyzer or the suggest field search analyzer.
     * For each analyzed token, suggested terms are suggested if possible.
     */
    public SuggestBuilder setText(@Nullable String globalText) {
        this.globalText = globalText;
        return this;
    }

    /**
     * Gets the global suggest text
     */
    public String getText() {
        return null;
    }

    /**
     * Adds an {@link org.elasticsearch.search.suggest.SuggestionBuilder} instance under a user defined name.
     * The order in which the <code>Suggestions</code> are added, is the same as in the response.
     */
    public SuggestBuilder addSuggestion(SuggestionBuilder<?> suggestion) {
        suggestions.add(suggestion);
        return this;
    }

    /**
     * Get the <code>Suggestions</code> that were added to the globat {@link SuggestBuilder}
     */
    public List<SuggestionBuilder<?>> getSuggestions() {
        return suggestions;
    }

    /**
     * Returns all suggestions with the defined names.
     */
    public List<SuggestionBuilder<?>> getSuggestion() {
        return suggestions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (globalText != null) {
            builder.field("text", globalText);
        }
        for (SuggestionBuilder<?> suggestion : suggestions) {
            builder = suggestion.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public static SuggestBuilder fromXContent(QueryParseContext parseContext, Suggesters suggesters) throws IOException {
        XContentParser parser = parseContext.parser();
        ParseFieldMatcher parseFieldMatcher = parseContext.parseFieldMatcher();
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        String fieldName = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parseFieldMatcher.match(fieldName, GLOBAL_TEXT_FIELD)) {
                    suggestBuilder.setText(parser.text());
                } else {
                    throw new IllegalArgumentException("[suggest] does not support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                String suggestionName = fieldName;
                if (suggestionName == null) {
                    throw new IllegalArgumentException("Suggestion must have name");
                }
                suggestBuilder.addSuggestion(SuggestionBuilder.fromXContent(parseContext, suggestionName, suggesters));
            } else {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "] after [" + fieldName + "]");
            }
        }
        return suggestBuilder;
    }

    @Override
    public SuggestBuilder readFrom(StreamInput in) throws IOException {
        final SuggestBuilder builder = new SuggestBuilder();
        builder.globalText = in.readOptionalString();
        final int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.suggestions.add(in.readSuggestion());
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(globalText);
        final int size = suggestions.size();
        out.writeVInt(size);
        for (int i = 0; i < size; i++) {
            out.writeSuggestion(suggestions.get(i));
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        SuggestBuilder o = (SuggestBuilder)other;
        return Objects.equals(globalText, o.globalText) &&
               Objects.equals(suggestions, o.suggestions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(globalText, suggestions);
    }
}
