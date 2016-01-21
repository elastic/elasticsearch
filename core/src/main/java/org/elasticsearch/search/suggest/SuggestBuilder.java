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
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

    private final List<SuggestionBuilder<? extends SuggestionBuilder>> suggestions = new ArrayList<>();

    public SuggestBuilder() {
        this.name = null;
    }

    public SuggestBuilder(String name) {
        this.name = name;
    }

    /**
     * Sets the text to provide suggestions for. The suggest text is a required option that needs
     * to be set either via this setter or via the {@link org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder#setText(String)} method.
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
    public SuggestBuilder addSuggestion(SuggestionBuilder<? extends SuggestionBuilder> suggestion) {
        suggestions.add(suggestion);
        return this;
    }

    /**
     * Returns all suggestions with the defined names.
     */
    public List<SuggestionBuilder<? extends SuggestionBuilder>> getSuggestion() {
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

    public static abstract class SuggestionBuilder<T extends SuggestionBuilder> extends ToXContentToBytes implements NamedWriteable<T> {

        private String name;
        private String suggester;
        private String text;
        private String prefix;
        private String regex;
        private String field;
        private String analyzer;
        private Integer size;
        private Integer shardSize;

        public SuggestionBuilder(String name, String suggester) {
            this.name = name;
            this.suggester = suggester;
        }

        // for use when constructing builders from an input stream
        protected SuggestionBuilder() { }

        /**
         * Same as in {@link SuggestBuilder#setText(String)}, but in the suggestion scope.
         */
        @SuppressWarnings("unchecked")
        public T text(String text) {
            this.text = text;
            return (T) this;
        }

        protected void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        protected void setRegex(String regex) {
            this.regex = regex;
        }

        protected void setName(String name) {
            this.name = name;
        }

        protected void setSuggester(String suggester) {
            this.suggester = suggester;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            if (text != null) {
                builder.field("text", text);
            }
            if (prefix != null) {
                builder.field("prefix", prefix);
            }
            if (regex != null) {
                builder.field("regex", regex);
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
         * <p>
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

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public final T readFrom(StreamInput in) throws IOException {
            T builder = doReadFrom(in);
            builder.setName(in.readString());
            builder.setSuggester(in.readString());
            builder.text(in.readOptionalString());
            builder.setPrefix(in.readOptionalString());
            builder.setRegex(in.readOptionalString());
            builder.field(in.readOptionalString())
                   .analyzer(in.readOptionalString())
                   .size(in.readOptionalVInt())
                   .shardSize(in.readOptionalVInt());
            return builder;
        }

        @Override
        public final void writeTo(StreamOutput out) throws IOException {
            doWriteTo(out);
            out.writeString(name);
            out.writeString(suggester);
            out.writeOptionalString(text);
            out.writeOptionalString(prefix);
            out.writeOptionalString(regex);
            out.writeOptionalString(field);
            out.writeOptionalString(analyzer);
            out.writeOptionalVInt(size);
            out.writeOptionalVInt(shardSize);
        }

        protected abstract T doReadFrom(StreamInput in) throws IOException;

        protected abstract void doWriteTo(StreamOutput out) throws IOException;

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (other == null || !(other instanceof SuggestionBuilder)) {
                return false;
            }
            SuggestionBuilder o = (SuggestionBuilder) other;
            return Objects.equals(name, o.name) &&
                   Objects.equals(suggester, o.suggester) &&
                   Objects.equals(text, o.text) &&
                   Objects.equals(prefix, o.prefix) &&
                   Objects.equals(regex, o.regex) &&
                   Objects.equals(field, o.field) &&
                   Objects.equals(analyzer, o.analyzer) &&
                   Objects.equals(size, o.size) &&
                   Objects.equals(shardSize, o.shardSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, suggester, text, prefix, regex, field, analyzer, size, shardSize);
        }
    }
}
