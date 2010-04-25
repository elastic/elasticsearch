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

    private List<ParsedHighlightField> fields;

    private String[] preTags;

    private String[] postTags;

    private boolean scoreOrdered = false;

    private boolean highlightFilter;

    public SearchContextHighlight(List<ParsedHighlightField> fields, String[] preTags, String[] postTags,
                                  boolean scoreOrdered, boolean highlightFilter) {
        this.fields = fields;
        this.preTags = preTags;
        this.postTags = postTags;
        this.scoreOrdered = scoreOrdered;
        this.highlightFilter = highlightFilter;
    }

    public boolean highlightFilter() {
        return highlightFilter;
    }

    public List<ParsedHighlightField> fields() {
        return fields;
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

    public static class ParsedHighlightField {

        private final String field;

        private final int fragmentCharSize;

        private final int numberOfFragments;

        public ParsedHighlightField(String field, int fragmentCharSize, int numberOfFragments) {
            this.field = field;
            this.fragmentCharSize = fragmentCharSize;
            this.numberOfFragments = numberOfFragments;
        }

        public String field() {
            return field;
        }

        public int fragmentCharSize() {
            return fragmentCharSize;
        }

        public int numberOfFragments() {
            return numberOfFragments;
        }
    }
}
