/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.LinkedHashMap;
import java.util.Map;

public class SuggestionSearchContext {

    private final Map<String, SuggestionContext> suggestions = new LinkedHashMap<>(4);

    public void addSuggestion(String name, SuggestionContext suggestion) {
        suggestions.put(name, suggestion);
    }

    public Map<String, SuggestionContext> suggestions() {
        return suggestions;
    }

    public abstract static class SuggestionContext {

        private BytesRef text;
        private BytesRef prefix;
        private BytesRef regex;
        private String field;
        private Analyzer analyzer;
        private int size = 5;
        private int shardSize = -1;
        private SearchExecutionContext searchExecutionContext;
        private Suggester<?> suggester;

        protected SuggestionContext(Suggester<?> suggester, SearchExecutionContext searchExecutionContext) {
            this.suggester = suggester;
            this.searchExecutionContext = searchExecutionContext;
        }

        public BytesRef getText() {
            return text;
        }

        public void setText(BytesRef text) {
            this.text = text;
        }

        public BytesRef getPrefix() {
            return prefix;
        }

        public void setPrefix(BytesRef prefix) {
            this.prefix = prefix;
        }

        public BytesRef getRegex() {
            return regex;
        }

        public void setRegex(BytesRef regex) {
            this.regex = regex;
        }

        @SuppressWarnings("unchecked")
        public Suggester<SuggestionContext> getSuggester() {
            return ((Suggester<SuggestionContext>) suggester);
        }

        public Analyzer getAnalyzer() {
            return analyzer;
        }

        public void setAnalyzer(Analyzer analyzer) {
            this.analyzer = analyzer;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            if (size <= 0) {
                throw new IllegalArgumentException("Size must be positive but was: " + size);
            }
            this.size = size;
        }

        public Integer getShardSize() {
            return shardSize;
        }

        public void setShardSize(int shardSize) {
            if (shardSize <= 0) {
                throw new IllegalArgumentException("ShardSize must be positive but was: " + shardSize);
            }
            this.shardSize = shardSize;
        }

        public SearchExecutionContext getSearchExecutionContext() {
            return this.searchExecutionContext;
        }

        @Override
        public String toString() {
            return "[" +
                       "text=" + text +
                       ",field=" + field +
                       ",prefix=" + prefix +
                       ",regex=" + regex +
                       ",size=" + size +
                       ",shardSize=" + shardSize +
                       ",suggester=" + suggester +
                       ",analyzer=" + analyzer +
                       ",searchExecutionContext=" + searchExecutionContext +
                   "]";
        }
    }

}
