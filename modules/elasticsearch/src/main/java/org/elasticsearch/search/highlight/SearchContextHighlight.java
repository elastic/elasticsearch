/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class SearchContextHighlight {

    private final ParsedHighlightSettings global;

    private final List<ParsedHighlightField> fields;

    public SearchContextHighlight(List<ParsedHighlightField> fields, ParsedHighlightSettings settings) {
        this.fields = fields;
        this.global = settings;
    }

    public List<ParsedHighlightField> fields() {
        return fields;
    }

    public ParsedHighlightSettings global() {
        return global;
    }

    public static class ParsedHighlightField {

        private final String field;

        private final ParsedHighlightSettings settings;

        public ParsedHighlightField(String field, ParsedHighlightSettings settings) {
            this.field = field;
            this.settings = settings;
        }

        public String field() {
            return field;
        }

        public ParsedHighlightSettings settings() {
            return settings;
        }
    }

    public static class ParsedHighlightSettings {

        private final int fragmentCharSize;

        private final int numberOfFragments;

        private final String[] preTags;

        private final String[] postTags;

        private boolean scoreOrdered = false;

        private boolean highlightFilter = true;

        private boolean fragmentsAllowed = true;

        public ParsedHighlightSettings(int fragmentCharSize, int numberOfFragments, String[] preTags, String[] postTags,
                                  boolean scoreOrdered, boolean highlightFilter, boolean fragmentsAllowed) {
            this.fragmentCharSize = fragmentCharSize;
            this.numberOfFragments = numberOfFragments;
            this.preTags = preTags;
            this.postTags = postTags;
            this.scoreOrdered = scoreOrdered;
            this.highlightFilter = highlightFilter;
            this.fragmentsAllowed = fragmentsAllowed;
        }

        public int fragmentCharSize() {
            return fragmentCharSize;
        }

        public int numberOfFragments() {
            return numberOfFragments;
        }

        public String[] preTags() {
            return preTags;
        }

        public String[] postTags() {
            return postTags;
        }

        public boolean scoreOrdered() {
            return scoreOrdered;
        }

        public boolean highlightFilter() {
            return highlightFilter;
        }

        public boolean fragmentsAllowed() {
            return fragmentsAllowed;
        }
    }
}
