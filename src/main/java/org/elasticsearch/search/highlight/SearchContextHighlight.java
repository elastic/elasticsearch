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

package org.elasticsearch.search.highlight;

import org.apache.lucene.search.Query;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SearchContextHighlight {

    private final List<Field> fields;

    public SearchContextHighlight(List<Field> fields) {
        this.fields = fields;
    }

    public List<Field> fields() {
        return fields;
    }

    public static class Field {
        // Fields that default to null or -1 are often set to their real default in HighlighterParseElement#parse
        private final String field;

        private int fragmentCharSize = -1;

        private int numberOfFragments = -1;

        private int fragmentOffset = -1;

        private String encoder;

        private String[] preTags;

        private String[] postTags;

        private Boolean scoreOrdered;

        private Boolean highlightFilter;

        private Boolean requireFieldMatch;

        private String highlighterType;

        private String fragmenter;

        private int boundaryMaxScan = -1;

        private Character[] boundaryChars = null;

        private Query highlightQuery;

        private int noMatchSize = -1;

        private Set<String> matchedFields;

        private Map<String, Object> options;

        public Field(String field) {
            this.field = field;
        }

        public String field() {
            return field;
        }

        public int fragmentCharSize() {
            return fragmentCharSize;
        }

        public void fragmentCharSize(int fragmentCharSize) {
            this.fragmentCharSize = fragmentCharSize;
        }

        public int numberOfFragments() {
            return numberOfFragments;
        }

        public void numberOfFragments(int numberOfFragments) {
            this.numberOfFragments = numberOfFragments;
        }

        public int fragmentOffset() {
            return fragmentOffset;
        }

        public void fragmentOffset(int fragmentOffset) {
            this.fragmentOffset = fragmentOffset;
        }

        public String encoder() {
            return encoder;
        }

        public void encoder(String encoder) {
            this.encoder = encoder;
        }

        public String[] preTags() {
            return preTags;
        }

        public void preTags(String[] preTags) {
            this.preTags = preTags;
        }

        public String[] postTags() {
            return postTags;
        }

        public void postTags(String[] postTags) {
            this.postTags = postTags;
        }

        public Boolean scoreOrdered() {
            return scoreOrdered;
        }

        public void scoreOrdered(boolean scoreOrdered) {
            this.scoreOrdered = scoreOrdered;
        }

        public Boolean highlightFilter() {
            return highlightFilter;
        }

        public void highlightFilter(boolean highlightFilter) {
            this.highlightFilter = highlightFilter;
        }

        public Boolean requireFieldMatch() {
            return requireFieldMatch;
        }

        public void requireFieldMatch(boolean requireFieldMatch) {
            this.requireFieldMatch = requireFieldMatch;
        }

        public String highlighterType() {
            return highlighterType;
        }

        public void highlighterType(String type) {
            this.highlighterType = type;
        }

        public String fragmenter() {
            return fragmenter;
        }

        public void fragmenter(String fragmenter) {
            this.fragmenter = fragmenter;
        }

        public int boundaryMaxScan() {
            return boundaryMaxScan;
        }

        public void boundaryMaxScan(int boundaryMaxScan) {
            this.boundaryMaxScan = boundaryMaxScan;
        }

        public Character[] boundaryChars() {
            return boundaryChars;
        }

        public void boundaryChars(Character[] boundaryChars) {
            this.boundaryChars = boundaryChars;
        }

        public Query highlightQuery() {
            return highlightQuery;
        }

        public void highlightQuery(Query highlightQuery) {
            this.highlightQuery = highlightQuery;
        }

        public int noMatchSize() {
            return noMatchSize;
        }

        public void noMatchSize(int noMatchSize) {
            this.noMatchSize = noMatchSize;
        }

        public Set<String> matchedFields() {
            return matchedFields;
        }

        public void matchedFields(Set<String> matchedFields) {
            this.matchedFields = matchedFields;
        }

        public Map<String, Object> options() {
            return options;
        }

        public void options(Map<String, Object> options) {
            this.options = options;
        }
    }
}
