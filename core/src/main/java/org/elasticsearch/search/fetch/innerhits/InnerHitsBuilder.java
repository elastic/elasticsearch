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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
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

    private final Map<String, InnerHitsHolder> innerHits = new HashMap<>();

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("inner_hits");
        innerXContent(builder, params);
        return builder.endObject();
    }

    public void innerXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, InnerHitsHolder> entry : innerHits.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
    }

    /**
     * For nested inner hits the path to collect child nested docs for.
     * @param name the name / key of the inner hits in the response
     * @param path the path into the nested to collect inner hits for
     * @param innerHit the inner hits definition
     */
    public void addNestedInnerHits(String name, String path, InnerHit innerHit) {
        if (innerHits.containsKey(name)) {
            throw new IllegalArgumentException("inner hits for name: [" + name +"] is already registered");
        }
        innerHits.put(name, new NestedInnerHitsHolder(path, innerHit));
    }

    /**
     * For parent/child inner hits the type to collect inner hits for.
     * @param name the name / key of the inner hits in the response
     * @param type the document type to collect inner hits for
     * @param innerHit the inner hits definition
     */
    public void addParentChildInnerHits(String name, String type, InnerHit innerHit) {
        innerHits.put(name, new ParentChildInnerHitsHolder(type, innerHit));
    }

    private static class InnerHitsHolder implements ToXContent{
        private final InnerHit hits;

        private InnerHitsHolder(InnerHit hits) {
            this.hits = hits;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return hits.toXContent(builder, params);
        }
    }

    private static class ParentChildInnerHitsHolder extends InnerHitsHolder {

        private final String type;

        private ParentChildInnerHitsHolder(String type, InnerHit hits) {
            super(hits);
            this.type = type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("type").startObject(type);
            super.toXContent(builder, params);
            return builder.endObject().endObject();
        }
    }

    private static class NestedInnerHitsHolder extends InnerHitsHolder {

        private final String path;

        private NestedInnerHitsHolder(String path, InnerHit hits) {
            super(hits);
            this.path = path;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("path").startObject(path);
            super.toXContent(builder, params);
            return builder.endObject().endObject();
        }
    }

    public static class InnerHit implements ToXContent {

        private SearchSourceBuilder sourceBuilder;
        private String path;
        private String type;

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
         * Add a stored field to be loaded and returned with the inner hit.
         */
        public InnerHit field(String name) {
            sourceBuilder().field(name);
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
        public InnerHit addScriptField(String name, Script script) {
            sourceBuilder().scriptField(name, script);
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

        public BytesReference highlighter() {
            return sourceBuilder().highlighter();
        }

        public InnerHit highlighter(HighlightBuilder highlightBuilder) {
            sourceBuilder().highlighter(highlightBuilder);
            return this;
        }

        protected SearchSourceBuilder sourceBuilder() {
            if (sourceBuilder == null) {
                sourceBuilder = new SearchSourceBuilder();
            }
            return sourceBuilder;
        }

        /**
         * Sets the query to run for collecting the inner hits.
         */
        public InnerHit setQuery(QueryBuilder query) {
            sourceBuilder().query(query);
            return this;
        }

        public InnerHit innerHits(InnerHitsBuilder innerHitsBuilder) {
            sourceBuilder().innerHits(innerHitsBuilder);
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (sourceBuilder != null) {
                sourceBuilder.innerToXContent(builder, params);
            }
            return builder;
        }
    }
}
