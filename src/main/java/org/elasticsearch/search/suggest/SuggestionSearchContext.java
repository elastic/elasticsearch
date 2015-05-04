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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.BytesRef;

import java.util.LinkedHashMap;
import java.util.Map;
/**
 */
public class SuggestionSearchContext {

    private final Map<String, SuggestionContext> suggestions = new LinkedHashMap<>(4);

    public void addSuggestion(String name, SuggestionContext suggestion) {
        suggestions.put(name, suggestion);
    }

    public Map<String, SuggestionContext> suggestions() {
        return suggestions;
    }
    
    public static class SuggestionContext {
        
        private BytesRef text;
        private final Suggester suggester;
        private String field;
        private Analyzer analyzer;
        private int size = 5;
        private int shardSize = -1;
        private int shardId;
        private String index;
        
        public BytesRef getText() {
            return text;
        }

        public void setText(BytesRef text) {
            this.text = text;
        }
        
        public SuggestionContext(Suggester suggester) {
            this.suggester = suggester;
        }

        public Suggester<SuggestionContext> getSuggester() {
            return this.suggester;
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
        
        public void setShard(int shardId) {
            this.shardId = shardId;
        }

        public void setIndex(String index) {
            this.index = index;
        }
        
        public String getIndex() {
            return index;
        }
        
        public int getShard() {
            return shardId;
        }
    }

}
