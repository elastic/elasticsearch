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
package org.elasticsearch.search.aggregations.metrics.tophits;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;

/**
 * Builder for the {@link TopHits} aggregation.
 */
public class TopHitsBuilder extends AbstractAggregationBuilder {

    private SearchSourceBuilder sourceBuilder;

    /**
     * Sole constructor.
     */
    public TopHitsBuilder(String name) {
        super(name, InternalTopHits.TYPE.name());
    }

    /**
     * The index to start to return hits from. Defaults to <tt>0</tt>.
     */
    public TopHitsBuilder setFrom(int from) {
        sourceBuilder().from(from);
        return this;
    }


    /**
     * The number of search hits to return. Defaults to <tt>10</tt>.
     */
    public TopHitsBuilder setSize(int size) {
        sourceBuilder().size(size);
        return this;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well. Defaults to
     * <tt>false</tt>.
     */
    public TopHitsBuilder setTrackScores(boolean trackScores) {
        sourceBuilder().trackScores(trackScores);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with an
     * explanation of the hit (ranking).
     */
    public TopHitsBuilder setExplain(boolean explain) {
        sourceBuilder().explain(explain);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with its
     * version.
     */
    public TopHitsBuilder setVersion(boolean version) {
        sourceBuilder().version(version);
        return this;
    }

    /**
     * Sets no fields to be loaded, resulting in only id and type to be returned per field.
     */
    public TopHitsBuilder setNoFields() {
        sourceBuilder().noFields();
        return this;
    }

    /**
     * Indicates whether the response should contain the stored _source for every hit
     */
    public TopHitsBuilder setFetchSource(boolean fetch) {
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
    public TopHitsBuilder setFetchSource(@Nullable String include, @Nullable String exclude) {
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
    public TopHitsBuilder setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        sourceBuilder().fetchSource(includes, excludes);
        return this;
    }

    /**
     * Adds a field data based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name The field to get from the field data cache
     */
    public TopHitsBuilder addFieldDataField(String name) {
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
    public TopHitsBuilder addScriptField(String name, String script) {
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
    public TopHitsBuilder addScriptField(String name, String script, Map<String, Object> params) {
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
    public TopHitsBuilder addScriptField(String name, String lang, String script, Map<String, Object> params) {
        sourceBuilder().scriptField(name, lang, script, params);
        return this;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param field The name of the field
     * @param order The sort ordering
     */
    public TopHitsBuilder addSort(String field, SortOrder order) {
        sourceBuilder().sort(field, order);
        return this;
    }

    /**
     * Adds a generic sort builder.
     *
     * @see org.elasticsearch.search.sort.SortBuilders
     */
    public TopHitsBuilder addSort(SortBuilder sort) {
        sourceBuilder().sort(sort);
        return this;
    }

    /**
     * Adds a field to be highlighted with default fragment size of 100 characters, and
     * default number of fragments of 5.
     *
     * @param name The field to highlight
     */
    public TopHitsBuilder addHighlightedField(String name) {
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
    public TopHitsBuilder addHighlightedField(String name, int fragmentSize) {
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
    public TopHitsBuilder addHighlightedField(String name, int fragmentSize, int numberOfFragments) {
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
    public TopHitsBuilder addHighlightedField(String name, int fragmentSize, int numberOfFragments,
                                                    int fragmentOffset) {
        highlightBuilder().field(name, fragmentSize, numberOfFragments, fragmentOffset);
        return this;
    }

    /**
     * Adds a highlighted field.
     */
    public TopHitsBuilder addHighlightedField(HighlightBuilder.Field field) {
        highlightBuilder().field(field);
        return this;
    }

    /**
     * Set a tag scheme that encapsulates a built in pre and post tags. The allows schemes
     * are <tt>styled</tt> and <tt>default</tt>.
     *
     * @param schemaName The tag scheme name
     */
    public TopHitsBuilder setHighlighterTagsSchema(String schemaName) {
        highlightBuilder().tagsSchema(schemaName);
        return this;
    }

    public TopHitsBuilder setHighlighterFragmentSize(Integer fragmentSize) {
        highlightBuilder().fragmentSize(fragmentSize);
        return this;
    }

    public TopHitsBuilder setHighlighterNumOfFragments(Integer numOfFragments) {
        highlightBuilder().numOfFragments(numOfFragments);
        return this;
    }

    public TopHitsBuilder setHighlighterFilter(Boolean highlightFilter) {
        highlightBuilder().highlightFilter(highlightFilter);
        return this;
    }

    /**
     * The encoder to set for highlighting
     */
    public TopHitsBuilder setHighlighterEncoder(String encoder) {
        highlightBuilder().encoder(encoder);
        return this;
    }

    /**
     * Explicitly set the pre tags that will be used for highlighting.
     */
    public TopHitsBuilder setHighlighterPreTags(String... preTags) {
        highlightBuilder().preTags(preTags);
        return this;
    }

    /**
     * Explicitly set the post tags that will be used for highlighting.
     */
    public TopHitsBuilder setHighlighterPostTags(String... postTags) {
        highlightBuilder().postTags(postTags);
        return this;
    }

    /**
     * The order of fragments per field. By default, ordered by the order in the
     * highlighted text. Can be <tt>score</tt>, which then it will be ordered
     * by score of the fragments.
     */
    public TopHitsBuilder setHighlighterOrder(String order) {
        highlightBuilder().order(order);
        return this;
    }

    public TopHitsBuilder setHighlighterRequireFieldMatch(boolean requireFieldMatch) {
        highlightBuilder().requireFieldMatch(requireFieldMatch);
        return this;
    }

    public TopHitsBuilder setHighlighterBoundaryMaxScan(Integer boundaryMaxScan) {
        highlightBuilder().boundaryMaxScan(boundaryMaxScan);
        return this;
    }

    public TopHitsBuilder setHighlighterBoundaryChars(char[] boundaryChars) {
        highlightBuilder().boundaryChars(boundaryChars);
        return this;
    }

    /**
     * The highlighter type to use.
     */
    public TopHitsBuilder setHighlighterType(String type) {
        highlightBuilder().highlighterType(type);
        return this;
    }

    public TopHitsBuilder setHighlighterFragmenter(String fragmenter) {
        highlightBuilder().fragmenter(fragmenter);
        return this;
    }

    /**
     * Sets a query to be used for highlighting all fields instead of the search query.
     */
    public TopHitsBuilder setHighlighterQuery(QueryBuilder highlightQuery) {
        highlightBuilder().highlightQuery(highlightQuery);
        return this;
    }

    /**
     * Sets the size of the fragment to return from the beginning of the field if there are no matches to
     * highlight and the field doesn't also define noMatchSize.
     * @param noMatchSize integer to set or null to leave out of request.  default is null.
     * @return this builder for chaining
     */
    public TopHitsBuilder setHighlighterNoMatchSize(Integer noMatchSize) {
        highlightBuilder().noMatchSize(noMatchSize);
        return this;
    }

    /**
     * Sets the maximum number of phrases the fvh will consider if the field doesn't also define phraseLimit.
     */
    public TopHitsBuilder setHighlighterPhraseLimit(Integer phraseLimit) {
        highlightBuilder().phraseLimit(phraseLimit);
        return this;
    }

    public TopHitsBuilder setHighlighterOptions(Map<String, Object> options) {
        highlightBuilder().options(options);
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName()).field(type);
        sourceBuilder().toXContent(builder, params);
        return builder.endObject();
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
}
