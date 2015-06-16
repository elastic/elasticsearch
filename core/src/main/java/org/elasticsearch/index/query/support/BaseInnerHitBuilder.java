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

package org.elasticsearch.index.query.support;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;

/**
 */
@SuppressWarnings("unchecked")
public abstract class BaseInnerHitBuilder<T extends BaseInnerHitBuilder> implements ToXContent {

    protected SearchSourceBuilder sourceBuilder;

    /**
     * The index to start to return hits from. Defaults to <tt>0</tt>.
     */
    public T setFrom(int from) {
        sourceBuilder().from(from);
        return (T) this;
    }


    /**
     * The number of search hits to return. Defaults to <tt>10</tt>.
     */
    public T setSize(int size) {
        sourceBuilder().size(size);
        return (T) this;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well. Defaults to
     * <tt>false</tt>.
     */
    public T setTrackScores(boolean trackScores) {
        sourceBuilder().trackScores(trackScores);
        return (T) this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with an
     * explanation of the hit (ranking).
     */
    public T setExplain(boolean explain) {
        sourceBuilder().explain(explain);
        return (T) this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with its
     * version.
     */
    public T setVersion(boolean version) {
        sourceBuilder().version(version);
        return (T) this;
    }

    /**
     * Add a stored field to be loaded and returned with the inner hit.
     */
    public T field(String name) {
        sourceBuilder().field(name);
        return (T) this;
    }

    /**
     * Sets no fields to be loaded, resulting in only id and type to be returned per field.
     */
    public T setNoFields() {
        sourceBuilder().noFields();
        return (T) this;
    }

    /**
     * Indicates whether the response should contain the stored _source for every hit
     */
    public T setFetchSource(boolean fetch) {
        sourceBuilder().fetchSource(fetch);
        return (T) this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include An optional include (optionally wildcarded) pattern to filter the returned _source
     * @param exclude An optional exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public T setFetchSource(@Nullable String include, @Nullable String exclude) {
        sourceBuilder().fetchSource(include, exclude);
        return (T) this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes An optional list of include (optionally wildcarded) pattern to filter the returned _source
     * @param excludes An optional list of exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public T setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        sourceBuilder().fetchSource(includes, excludes);
        return (T) this;
    }

    /**
     * Adds a field data based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name The field to get from the field data cache
     */
    public T addFieldDataField(String name) {
        sourceBuilder().fieldDataField(name);
        return (T) this;
    }

    /**
     * Adds a script based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name   The name that will represent this value in the return hit
     * @param script The script to use
     */
    public T addScriptField(String name, Script script) {
        sourceBuilder().scriptField(name, script);
        return (T) this;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param field The name of the field
     * @param order The sort ordering
     */
    public T addSort(String field, SortOrder order) {
        sourceBuilder().sort(field, order);
        return (T) this;
    }

    /**
     * Adds a generic sort builder.
     *
     * @see org.elasticsearch.search.sort.SortBuilders
     */
    public T addSort(SortBuilder sort) {
        sourceBuilder().sort(sort);
        return (T) this;
    }

    public HighlightBuilder highlightBuilder() {
        return sourceBuilder().highlighter();
    }

    /**
     * Adds a field to be highlighted with default fragment size of 100 characters, and
     * default number of fragments of 5.
     *
     * @param name The field to highlight
     */
    public T addHighlightedField(String name) {
        highlightBuilder().field(name);
        return (T) this;
    }


    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * default number of fragments of 5.
     *
     * @param name         The field to highlight
     * @param fragmentSize The size of a fragment in characters
     */
    public T addHighlightedField(String name, int fragmentSize) {
        highlightBuilder().field(name, fragmentSize);
        return (T) this;
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * a provided (maximum) number of fragments.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     */
    public T addHighlightedField(String name, int fragmentSize, int numberOfFragments) {
        highlightBuilder().field(name, fragmentSize, numberOfFragments);
        return (T) this;
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters),
     * a provided (maximum) number of fragments and an offset for the highlight.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     */
    public T addHighlightedField(String name, int fragmentSize, int numberOfFragments,
                                        int fragmentOffset) {
        highlightBuilder().field(name, fragmentSize, numberOfFragments, fragmentOffset);
        return (T) this;
    }

    /**
     * Adds a highlighted field.
     */
    public T addHighlightedField(HighlightBuilder.Field field) {
        highlightBuilder().field(field);
        return (T) this;
    }

    /**
     * Set a tag scheme that encapsulates a built in pre and post tags. The allows schemes
     * are <tt>styled</tt> and <tt>default</tt>.
     *
     * @param schemaName The tag scheme name
     */
    public T setHighlighterTagsSchema(String schemaName) {
        highlightBuilder().tagsSchema(schemaName);
        return (T) this;
    }

    public T setHighlighterFragmentSize(Integer fragmentSize) {
        highlightBuilder().fragmentSize(fragmentSize);
        return (T) this;
    }

    public T setHighlighterNumOfFragments(Integer numOfFragments) {
        highlightBuilder().numOfFragments(numOfFragments);
        return (T) this;
    }

    public T setHighlighterFilter(Boolean highlightFilter) {
        highlightBuilder().highlightFilter(highlightFilter);
        return (T) this;
    }

    /**
     * The encoder to set for highlighting
     */
    public T setHighlighterEncoder(String encoder) {
        highlightBuilder().encoder(encoder);
        return (T) this;
    }

    /**
     * Explicitly set the pre tags that will be used for highlighting.
     */
    public T setHighlighterPreTags(String... preTags) {
        highlightBuilder().preTags(preTags);
        return (T) this;
    }

    /**
     * Explicitly set the post tags that will be used for highlighting.
     */
    public T setHighlighterPostTags(String... postTags) {
        highlightBuilder().postTags(postTags);
        return (T) this;
    }

    /**
     * The order of fragments per field. By default, ordered by the order in the
     * highlighted text. Can be <tt>score</tt>, which then it will be ordered
     * by score of the fragments.
     */
    public T setHighlighterOrder(String order) {
        highlightBuilder().order(order);
        return (T) this;
    }

    public T setHighlighterRequireFieldMatch(boolean requireFieldMatch) {
        highlightBuilder().requireFieldMatch(requireFieldMatch);
        return (T) this;
    }

    public T setHighlighterBoundaryMaxScan(Integer boundaryMaxScan) {
        highlightBuilder().boundaryMaxScan(boundaryMaxScan);
        return (T) this;
    }

    public T setHighlighterBoundaryChars(char[] boundaryChars) {
        highlightBuilder().boundaryChars(boundaryChars);
        return (T) this;
    }

    /**
     * The highlighter type to use.
     */
    public T setHighlighterType(String type) {
        highlightBuilder().highlighterType(type);
        return (T) this;
    }

    public T setHighlighterFragmenter(String fragmenter) {
        highlightBuilder().fragmenter(fragmenter);
        return (T) this;
    }

    /**
     * Sets a query to be used for highlighting all fields instead of the search query.
     */
    public T setHighlighterQuery(QueryBuilder highlightQuery) {
        highlightBuilder().highlightQuery(highlightQuery);
        return (T) this;
    }

    /**
     * Sets the size of the fragment to return from the beginning of the field if there are no matches to
     * highlight and the field doesn't also define noMatchSize.
     * @param noMatchSize integer to set or null to leave out of request.  default is null.
     * @return this builder for chaining
     */
    public T setHighlighterNoMatchSize(Integer noMatchSize) {
        highlightBuilder().noMatchSize(noMatchSize);
        return (T) this;
    }

    /**
     * Sets the maximum number of phrases the fvh will consider if the field doesn't also define phraseLimit.
     */
    public T setHighlighterPhraseLimit(Integer phraseLimit) {
        highlightBuilder().phraseLimit(phraseLimit);
        return (T) this;
    }

    public T setHighlighterOptions(Map<String, Object> options) {
        highlightBuilder().options(options);
        return (T) this;
    }

    protected SearchSourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new SearchSourceBuilder();
        }
        return sourceBuilder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (sourceBuilder != null) {
            sourceBuilder.innerToXContent(builder, params);
        }
        return builder;
    }

}
