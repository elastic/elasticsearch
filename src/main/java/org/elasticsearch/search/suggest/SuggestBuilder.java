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
import org.elasticsearch.search.suggest.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.context.ContextMapping.ContextQuery;
import org.elasticsearch.search.suggest.context.GeolocationContextMapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines how to perform suggesting. This builders allows a number of global options to be specified and
 * an arbitrary number of {@link org.elasticsearch.search.suggest.term.TermSuggestionBuilder} instances.
 * <p/>
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

    public static abstract class SuggestionBuilder<T> extends ToXContentToBytes {

        private String name;
        private String suggester;
        private String text;
        private String field;
        private String analyzer;
        private Integer size;
        private Integer shardSize;
        
        private List<ContextQuery> contextQueries = new ArrayList<>();

        public SuggestionBuilder(String name, String suggester) {
            this.name = name;
            this.suggester = suggester;
        }

        @SuppressWarnings("unchecked")
        private T addContextQuery(ContextQuery ctx) {
            this.contextQueries.add(ctx);
            return (T) this;
        }

        /**
         * Setup a Geolocation for suggestions. See {@link GeolocationContextMapping}.
         * @param lat Latitude of the location
         * @param lon Longitude of the Location
         * @return this
         */
        public T addGeoLocation(String name, double lat, double lon, int ... precisions) {
            return addContextQuery(GeolocationContextMapping.query(name, lat, lon, precisions));
        }

        /**
         * Setup a Geolocation for suggestions. See {@link GeolocationContextMapping}.
         * @param lat Latitude of the location
         * @param lon Longitude of the Location
         * @param precisions precisions as string var-args
         * @return this
         */
        public T addGeoLocationWithPrecision(String name, double lat, double lon, String ... precisions) {
            return addContextQuery(GeolocationContextMapping.query(name, lat, lon, precisions));
        }

        /**
         * Setup a Geolocation for suggestions. See {@link GeolocationContextMapping}.
         * @param geohash Geohash of the location
         * @return this
         */
        public T addGeoLocation(String name, String geohash) {
            return addContextQuery(GeolocationContextMapping.query(name, geohash));
        }
        
        /**
         * Setup a Category for suggestions. See {@link CategoryContextMapping}.
         * @param categories name of the category
         * @return this
         */
        public T addCategory(String name, CharSequence...categories) {
            return addContextQuery(CategoryContextMapping.query(name, categories));
        }
        
        /**
         * Setup a Category for suggestions. See {@link CategoryContextMapping}.
         * @param categories name of the category
         * @return this
         */
        public T addCategory(String name, Iterable<? extends CharSequence> categories) {
            return addContextQuery(CategoryContextMapping.query(name, categories));
        }
        
        /**
         * Setup a Context Field for suggestions. See {@link CategoryContextMapping}.
         * @param fieldvalues name of the category
         * @return this
         */
        public T addContextField(String name, CharSequence...fieldvalues) {
            return addContextQuery(CategoryContextMapping.query(name, fieldvalues));
        }
        
        /**
         * Setup a Context Field for suggestions. See {@link CategoryContextMapping}.
         * @param fieldvalues name of the category
         * @return this
         */
        public T addContextField(String name, Iterable<? extends CharSequence> fieldvalues) {
            return addContextQuery(CategoryContextMapping.query(name, fieldvalues));
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

            if (!contextQueries.isEmpty()) {
                builder.startObject("context");
                for (ContextQuery query : contextQueries) {
                    query.toXContent(builder, params);
                }
                builder.endObject();
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
         * {@link org.elasticsearch.search.suggest.term.TermSuggestionBuilder#field(String)}
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
                throw new IllegalArgumentException("Size must be positive");
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
