/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

/**
 * Defines how to perform suggesting. This builders allows a number of global options to be specified and
 * an arbitrary number of {@link org.elasticsearch.search.suggest.SuggestBuilder.TermSuggestionBuilder} instances.
 * <p/>
 * Suggesting works by suggesting terms that appear in the suggest text that are similar compared to the terms in
 * provided text. These spelling suggestions are based on several options described in this class.
 */
public class SuggestBuilder implements ToXContent {

    private final String name;
    private String globalText;

    private final List<SuggestionBuilder<?>> suggestions = new ArrayList<SuggestionBuilder<?>>();

    public SuggestBuilder() {
        this.name = null;
    }
    
    public SuggestBuilder(String name) {
        this.name = name;
    }
    
    /**
     * Sets the text to provide suggestions for. The suggest text is a required option that needs
     * to be set either via this setter or via the {@link org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder#setText(String)} method.
     * <p/>
     * The suggest text gets analyzed by the suggest analyzer or the suggest field search analyzer.
     * For each analyzed token, suggested terms are suggested if possible.
     */
    public SuggestBuilder setText(String globalText) {
        this.globalText = globalText;
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.search.suggest.SuggestBuilder.TermSuggestionBuilder} instance under a user defined name.
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

    /**
     * Convenience factory method.
     *
     * @param name The name of this suggestion. This is a required parameter.
     */
    public static TermSuggestionBuilder termSuggestion(String name) {
        return new TermSuggestionBuilder(name);
    }
    
    /**
     * Convenience factory method.
     *
     * @param name The name of this suggestion. This is a required parameter.
     */
    public static PhraseSuggestionBuilder phraseSuggestion(String name) {
        return new PhraseSuggestionBuilder(name);
    }

    public static abstract class SuggestionBuilder<T> implements ToXContent {

        private String name;
        private String suggester;
        private String text;
        private String field;
        private String analyzer;
        private Integer size;
        private Integer shardSize;

        public SuggestionBuilder(String name, String suggester) {
            this.name = name;
            this.suggester = suggester;
        }

        /**
         * Same as in {@link SuggestBuilder#setText(String)}, but in the suggestion scope.
         */
        @SuppressWarnings("unchecked")
        public T text(String text) {
            this.text = text;
            return (T) this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            if (text != null) {
                builder.field("text", text);
            }
            builder.startObject(suggester);
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (field != null) {
                builder.field("field", field);
            }
            if (size != null) {
                builder.field("size", size);
            }
            if (shardSize != null) {
                builder.field("shard_size", shardSize);
            }
            builder = innerToXContent(builder, params);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;

        /**
         * Sets from what field to fetch the candidate suggestions from. This is an
         * required option and needs to be set via this setter or
         * {@link org.elasticsearch.search.suggest.SuggestBuilder.TermSuggestionBuilder#setField(String)}
         * method
         */
        @SuppressWarnings("unchecked")
        public T field(String field) {
            this.field = field;
            return (T)this;
        }

        /**
         * Sets the analyzer to analyse to suggest text with. Defaults to the search
         * analyzer of the suggest field.
         */
        @SuppressWarnings("unchecked")
        public T analyzer(String analyzer) {
            this.analyzer = analyzer;
            return (T)this;
        }

        /**
         * Sets the maximum suggestions to be returned per suggest text term.
         */
        @SuppressWarnings("unchecked")
        public T size(int size) {
            if (size <= 0) {
                throw new ElasticSearchIllegalArgumentException("Size must be positive");
            }
            this.size = size;
            return (T)this;
        }

        /**
         * Sets the maximum number of suggested term to be retrieved from each
         * individual shard. During the reduce phase the only the top N suggestions
         * are returned based on the <code>size</code> option. Defaults to the
         * <code>size</code> option.
         * <p/>
         * Setting this to a value higher than the `size` can be useful in order to
         * get a more accurate document frequency for suggested terms. Due to the
         * fact that terms are partitioned amongst shards, the shard level document
         * frequencies of suggestions may not be precise. Increasing this will make
         * these document frequencies more precise.
         */
        @SuppressWarnings("unchecked")
        public T shardSize(Integer shardSize) {
            this.shardSize = shardSize;
            return (T)this;
        }
    }
}
