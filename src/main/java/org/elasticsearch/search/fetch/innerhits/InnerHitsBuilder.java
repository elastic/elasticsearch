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

package org.elasticsearch.search.fetch.innerhits;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class InnerHitsBuilder implements ToXContent {

    private Map<String, InnerHit> innerHits = new HashMap<>();

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("inner_hits");
        for (Map.Entry<String, InnerHit> entry : innerHits.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        return builder.endObject();
    }

    public void addInnerHit(String name, InnerHit innerHit) {
        innerHits.put(name, innerHit);
    }

    public static class InnerHit implements ToXContent {

        private String path;
        private String type;

        private SearchSourceBuilder sourceBuilder;

        public InnerHit setQuery(QueryBuilder query) {
            sourceBuilder().query(query);
            return this;
        }

        public InnerHit setPath(String path) {
            this.path = path;
            return this;
        }

        public InnerHit setType(String type) {
            this.type = type;
            return this;
        }

        /**
         * The index to start to return hits from. Defaults to <tt>0</tt>.
         */
        public InnerHit setFrom(int from) {
            sourceBuilder().from(from);
            return this;
        }


        /**
         * The number of search hits to return. Defaults to <tt>10</tt>.
         */
        public InnerHit setSize(int size) {
            sourceBuilder().size(size);
            return this;
        }

        /**
         * Applies when sorting, and controls if scores will be tracked as well. Defaults to
         * <tt>false</tt>.
         */
        public InnerHit setTrackScores(boolean trackScores) {
            sourceBuilder().trackScores(trackScores);
            return this;
        }

        /**
         * Should each {@link org.elasticsearch.search.SearchHit} be returned with an
         * explanation of the hit (ranking).
         */
        public InnerHit setExplain(boolean explain) {
            sourceBuilder().explain(explain);
            return this;
        }

        /**
         * Should each {@link org.elasticsearch.search.SearchHit} be returned with its
         * version.
         */
        public InnerHit setVersion(boolean version) {
            sourceBuilder().version(version);
            return this;
        }

        /**
         * Sets no fields to be loaded, resulting in only id and type to be returned per field.
         */
        public InnerHit setNoFields() {
            sourceBuilder().noFields();
            return this;
        }

        /**
         * Indicates whether the response should contain the stored _source for every hit
         */
        public InnerHit setFetchSource(boolean fetch) {
            sourceBuilder().fetchSource(fetch);
            return this;
        }

        /**
         * Indicate that _source should be returned with every hit, with an "include" and/or "exclude" set which can include simple wildcard
         * elements.
         *
         * @param include An optional include (optionally wildcarded) pattern to filter the returned _source
         * @param exclude An optional exclude (optionally wildcarded) pattern to filter the returned _source
         */
        public InnerHit setFetchSource(@Nullable String include, @Nullable String exclude) {
            sourceBuilder().fetchSource(include, exclude);
            return this;
        }

        /**
         * Indicate that _source should be returned with every hit, with an "include" and/or "exclude" set which can include simple wildcard
         * elements.
         *
         * @param includes An optional list of include (optionally wildcarded) pattern to filter the returned _source
         * @param excludes An optional list of exclude (optionally wildcarded) pattern to filter the returned _source
         */
        public InnerHit setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
            sourceBuilder().fetchSource(includes, excludes);
            return this;
        }

        /**
         * Adds a field data based field to load and return. The field does not have to be stored,
         * but its recommended to use non analyzed or numeric fields.
         *
         * @param name The field to get from the field data cache
         */
        public InnerHit addFieldDataField(String name) {
            sourceBuilder().fieldDataField(name);
            return this;
        }

        /**
         * Adds a script based field to load and return. The field does not have to be stored,
         * but its recommended to use non analyzed or numeric fields.
         *
         * @param name   The name that will represent this value in the return hit
         * @param script The script to use
         */
        public InnerHit addScriptField(String name, String script) {
            sourceBuilder().scriptField(name, script);
            return this;
        }

        /**
         * Adds a script based field to load and return. The field does not have to be stored,
         * but its recommended to use non analyzed or numeric fields.
         *
         * @param name   The name that will represent this value in the return hit
         * @param script The script to use
         * @param params Parameters that the script can use.
         */
        public InnerHit addScriptField(String name, String script, Map<String, Object> params) {
            sourceBuilder().scriptField(name, script, params);
            return this;
        }

        /**
         * Adds a script based field to load and return. The field does not have to be stored,
         * but its recommended to use non analyzed or numeric fields.
         *
         * @param name   The name that will represent this value in the return hit
         * @param lang   The language of the script
         * @param script The script to use
         * @param params Parameters that the script can use (can be <tt>null</tt>).
         */
        public InnerHit addScriptField(String name, String lang, String script, Map<String, Object> params) {
            sourceBuilder().scriptField(name, lang, script, params);
            return this;
        }

        /**
         * Adds a sort against the given field name and the sort ordering.
         *
         * @param field The name of the field
         * @param order The sort ordering
         */
        public InnerHit addSort(String field, SortOrder order) {
            sourceBuilder().sort(field, order);
            return this;
        }

        /**
         * Adds a generic sort builder.
         *
         * @see org.elasticsearch.search.sort.SortBuilders
         */
        public InnerHit addSort(SortBuilder sort) {
            sourceBuilder().sort(sort);
            return this;
        }

        /**
         * Adds a field to be highlighted with default fragment size of 100 characters, and
         * default number of fragments of 5.
         *
         * @param name The field to highlight
         */
        public InnerHit addHighlightedField(String name) {
            highlightBuilder().field(name);
            return this;
        }


        /**
         * Adds a field to be highlighted with a provided fragment size (in characters), and
         * default number of fragments of 5.
         *
         * @param name         The field to highlight
         * @param fragmentSize The size of a fragment in characters
         */
        public InnerHit addHighlightedField(String name, int fragmentSize) {
            highlightBuilder().field(name, fragmentSize);
            return this;
        }

        /**
         * Adds a field to be highlighted with a provided fragment size (in characters), and
         * a provided (maximum) number of fragments.
         *
         * @param name              The field to highlight
         * @param fragmentSize      The size of a fragment in characters
         * @param numberOfFragments The (maximum) number of fragments
         */
        public InnerHit addHighlightedField(String name, int fragmentSize, int numberOfFragments) {
            highlightBuilder().field(name, fragmentSize, numberOfFragments);
            return this;
        }

        /**
         * Adds a field to be highlighted with a provided fragment size (in characters),
         * a provided (maximum) number of fragments and an offset for the highlight.
         *
         * @param name              The field to highlight
         * @param fragmentSize      The size of a fragment in characters
         * @param numberOfFragments The (maximum) number of fragments
         */
        public InnerHit addHighlightedField(String name, int fragmentSize, int numberOfFragments,
                                                    int fragmentOffset) {
            highlightBuilder().field(name, fragmentSize, numberOfFragments, fragmentOffset);
            return this;
        }

        /**
         * Adds a highlighted field.
         */
        public InnerHit addHighlightedField(HighlightBuilder.Field field) {
            highlightBuilder().field(field);
            return this;
        }

        /**
         * Set a tag scheme that encapsulates a built in pre and post tags. The allows schemes
         * are <tt>styled</tt> and <tt>default</tt>.
         *
         * @param schemaName The tag scheme name
         */
        public InnerHit setHighlighterTagsSchema(String schemaName) {
            highlightBuilder().tagsSchema(schemaName);
            return this;
        }

        public InnerHit setHighlighterFragmentSize(Integer fragmentSize) {
            highlightBuilder().fragmentSize(fragmentSize);
            return this;
        }

        public InnerHit setHighlighterNumOfFragments(Integer numOfFragments) {
            highlightBuilder().numOfFragments(numOfFragments);
            return this;
        }

        public InnerHit setHighlighterFilter(Boolean highlightFilter) {
            highlightBuilder().highlightFilter(highlightFilter);
            return this;
        }

        /**
         * The encoder to set for highlighting
         */
        public InnerHit setHighlighterEncoder(String encoder) {
            highlightBuilder().encoder(encoder);
            return this;
        }

        /**
         * Explicitly set the pre tags that will be used for highlighting.
         */
        public InnerHit setHighlighterPreTags(String... preTags) {
            highlightBuilder().preTags(preTags);
            return this;
        }

        /**
         * Explicitly set the post tags that will be used for highlighting.
         */
        public InnerHit setHighlighterPostTags(String... postTags) {
            highlightBuilder().postTags(postTags);
            return this;
        }

        /**
         * The order of fragments per field. By default, ordered by the order in the
         * highlighted text. Can be <tt>score</tt>, which then it will be ordered
         * by score of the fragments.
         */
        public InnerHit setHighlighterOrder(String order) {
            highlightBuilder().order(order);
            return this;
        }

        public InnerHit setHighlighterRequireFieldMatch(boolean requireFieldMatch) {
            highlightBuilder().requireFieldMatch(requireFieldMatch);
            return this;
        }

        public InnerHit setHighlighterBoundaryMaxScan(Integer boundaryMaxScan) {
            highlightBuilder().boundaryMaxScan(boundaryMaxScan);
            return this;
        }

        public InnerHit setHighlighterBoundaryChars(char[] boundaryChars) {
            highlightBuilder().boundaryChars(boundaryChars);
            return this;
        }

        /**
         * The highlighter type to use.
         */
        public InnerHit setHighlighterType(String type) {
            highlightBuilder().highlighterType(type);
            return this;
        }

        public InnerHit setHighlighterFragmenter(String fragmenter) {
            highlightBuilder().fragmenter(fragmenter);
            return this;
        }

        /**
         * Sets a query to be used for highlighting all fields instead of the search query.
         */
        public InnerHit setHighlighterQuery(QueryBuilder highlightQuery) {
            highlightBuilder().highlightQuery(highlightQuery);
            return this;
        }

        /**
         * Sets the size of the fragment to return from the beginning of the field if there are no matches to
         * highlight and the field doesn't also define noMatchSize.
         * @param noMatchSize integer to set or null to leave out of request.  default is null.
         * @return this builder for chaining
         */
        public InnerHit setHighlighterNoMatchSize(Integer noMatchSize) {
            highlightBuilder().noMatchSize(noMatchSize);
            return this;
        }

        /**
         * Sets the maximum number of phrases the fvh will consider if the field doesn't also define phraseLimit.
         */
        public InnerHit setHighlighterPhraseLimit(Integer phraseLimit) {
            highlightBuilder().phraseLimit(phraseLimit);
            return this;
        }

        public InnerHit setHighlighterOptions(Map<String, Object> options) {
            highlightBuilder().options(options);
            return this;
        }

        public InnerHit addInnerHit(String name, InnerHit innerHit) {
            sourceBuilder().innerHitsBuilder().addInnerHit(name, innerHit);
            return this;
        }

        private SearchSourceBuilder sourceBuilder() {
            if (sourceBuilder == null) {
                sourceBuilder = new SearchSourceBuilder();
            }
            return sourceBuilder;
        }

        public HighlightBuilder highlightBuilder() {
            return sourceBuilder().highlighter();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (path != null) {
                builder.startObject("path").startObject(path);
            } else {
                builder.startObject("type").startObject(type);
            }
            if (sourceBuilder != null) {
                sourceBuilder.innerToXContent(builder, params);
            }
            return builder.endObject().endObject();
        }
    }

}
