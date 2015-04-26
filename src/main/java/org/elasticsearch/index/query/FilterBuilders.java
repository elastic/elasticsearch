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

package org.elasticsearch.index.query;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;

/**
 * A static factory for simple "import static" usage.
 */
public abstract class FilterBuilders {

    /**
     * A filter that matches all documents.
     */
    public static MatchAllFilterBuilder matchAllFilter() {
        return new MatchAllFilterBuilder();
    }

    /**
     * A filter that limits the results to the provided limit value (per shard!).
     * @deprecated Use {@link SearchRequestBuilder#setTerminateAfter(int)} instead.
     */
    @Deprecated
    public static LimitFilterBuilder limitFilter(int limit) {
        return new LimitFilterBuilder(limit);
    }

    public static NestedFilterBuilder nestedFilter(String path, QueryBuilder query) {
        return new NestedFilterBuilder(path, query);
    }

    public static NestedFilterBuilder nestedFilter(String path, FilterBuilder filter) {
        return new NestedFilterBuilder(path, filter);
    }

    /**
     * Creates a new ids filter with the provided doc/mapping types.
     *
     * @param types The types to match the ids against.
     */
    public static IdsFilterBuilder idsFilter(@Nullable String... types) {
        return new IdsFilterBuilder(types);
    }

    /**
     * A filter based on doc/mapping type.
     */
    public static TypeFilterBuilder typeFilter(String type) {
        return new TypeFilterBuilder(type);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermFilterBuilder termFilter(String name, String value) {
        return new TermFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermFilterBuilder termFilter(String name, int value) {
        return new TermFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermFilterBuilder termFilter(String name, long value) {
        return new TermFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermFilterBuilder termFilter(String name, float value) {
        return new TermFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermFilterBuilder termFilter(String name, double value) {
        return new TermFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermFilterBuilder termFilter(String name, Object value) {
        return new TermFilterBuilder(name, value);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder termsFilter(String name, String... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder termsFilter(String name, int... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder termsFilter(String name, long... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder termsFilter(String name, float... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder termsFilter(String name, double... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder termsFilter(String name, Object... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder termsFilter(String name, Iterable<?> values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A terms lookup filter for the provided field name. A lookup terms filter can
     * extract the terms to filter by from another doc in an index.
     */
    public static TermsLookupFilterBuilder termsLookupFilter(String name) {
        return new TermsLookupFilterBuilder(name);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder inFilter(String name, String... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder inFilter(String name, int... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder inFilter(String name, long... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder inFilter(String name, float... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder inFilter(String name, double... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsFilterBuilder inFilter(String name, Object... values) {
        return new TermsFilterBuilder(name, values);
    }

    /**
     * A filter that restricts search results to values that have a matching prefix in a given
     * field.
     *
     * @param name   The field name
     * @param prefix The prefix
     */
    public static PrefixFilterBuilder prefixFilter(String name, String prefix) {
        return new PrefixFilterBuilder(name, prefix);
    }

    /**
     * A filter that restricts search results to field values that match a given regular expression.
     *
     * @param name   The field name
     * @param regexp The regular expression
     */
    public static RegexpFilterBuilder regexpFilter(String name, String regexp) {
        return new RegexpFilterBuilder(name, regexp);
    }

    /**
     * A filter that restricts search results to values that are within the given range.
     *
     * @param name The field name
     */
    public static RangeFilterBuilder rangeFilter(String name) {
        return new RangeFilterBuilder(name);
    }

    /**
     * A filter that simply wraps a query.
     *
     * @param queryBuilder The query to wrap as a filter
     */
    public static QueryFilterBuilder queryFilter(QueryBuilder queryBuilder) {
        return new QueryFilterBuilder(queryBuilder);
    }

    /**
     * A builder for filter based on a script.
     *
     * @param script The script to filter by.
     */
    public static ScriptFilterBuilder scriptFilter(String script) {
        return new ScriptFilterBuilder(script);
    }

    /**
     * A filter to filter based on a specific distance from a specific geo location / point.
     *
     * @param name The location field name.
     */
    public static GeoDistanceFilterBuilder geoDistanceFilter(String name) {
        return new GeoDistanceFilterBuilder(name);
    }

    /**
     * A filter to filter based on a specific range from a specific geo location / point.
     *
     * @param name The location field name.
     */
    public static GeoDistanceRangeFilterBuilder geoDistanceRangeFilter(String name) {
        return new GeoDistanceRangeFilterBuilder(name);
    }

    /**
     * A filter to filter based on a bounding box defined by top left and bottom right locations / points
     *
     * @param name The location field name.
     */
    public static GeoBoundingBoxFilterBuilder geoBoundingBoxFilter(String name) {
        return new GeoBoundingBoxFilterBuilder(name);
    }

    /**
     * A filter based on a bounding box defined by geohash. The field this filter is applied to
     * must have <code>{&quot;type&quot;:&quot;geo_point&quot;, &quot;geohash&quot;:true}</code>
     * to work.
     *
     * @param name The geo point field name.
     */
    public static GeohashCellFilter.Builder geoHashCellFilter(String name) {
        return new GeohashCellFilter.Builder(name);
    }

    /**
     * A filter based on a bounding box defined by geohash. The field this filter is applied to
     * must have <code>{&quot;type&quot;:&quot;geo_point&quot;, &quot;geohash&quot;:true}</code>
     * to work.
     *
     * @param name The geo point field name.
     * @param geohash The Geohash to filter
     */
    public static GeohashCellFilter.Builder geoHashCellFilter(String name, String geohash) {
        return new GeohashCellFilter.Builder(name, geohash);
    }

    /**
     * A filter based on a bounding box defined by geohash. The field this filter is applied to
     * must have <code>{&quot;type&quot;:&quot;geo_point&quot;, &quot;geohash&quot;:true}</code>
     * to work.
     *
     * @param name The geo point field name.
     * @param point a geo point within the geohash bucket
     */
    public static GeohashCellFilter.Builder geoHashCellFilter(String name, GeoPoint point) {
        return new GeohashCellFilter.Builder(name, point);
    }

    /**
     * A filter based on a bounding box defined by geohash. The field this filter is applied to
     * must have <code>{&quot;type&quot;:&quot;geo_point&quot;, &quot;geohash&quot;:true}</code>
     * to work.
     *
     * @param name The geo point field name
     * @param geohash The Geohash to filter
     * @param neighbors should the neighbor cell also be filtered
     */
    public static GeohashCellFilter.Builder geoHashCellFilter(String name, String geohash, boolean neighbors) {
        return new GeohashCellFilter.Builder(name, geohash, neighbors);
    }
    
    /**
     * A filter to filter based on a polygon defined by a set of locations  / points.
     *
     * @param name The location field name.
     */
    public static GeoPolygonFilterBuilder geoPolygonFilter(String name) {
        return new GeoPolygonFilterBuilder(name);
    }

    /**
     * A filter based on the relationship of a shape and indexed shapes
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     * @param relation relation of the shapes
     */
    public static GeoShapeFilterBuilder geoShapeFilter(String name, ShapeBuilder shape, ShapeRelation relation) {
        return new GeoShapeFilterBuilder(name, shape, relation);
    }

    public static GeoShapeFilterBuilder geoShapeFilter(String name, String indexedShapeId, String indexedShapeType, ShapeRelation relation) {
        return new GeoShapeFilterBuilder(name, indexedShapeId, indexedShapeType, relation);
    }

    /**
     * A filter to filter indexed shapes intersecting with shapes
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeFilterBuilder geoIntersectionFilter(String name, ShapeBuilder shape) {
        return geoShapeFilter(name, shape, ShapeRelation.INTERSECTS);
    }

    public static GeoShapeFilterBuilder geoIntersectionFilter(String name, String indexedShapeId, String indexedShapeType) {
        return geoShapeFilter(name, indexedShapeId, indexedShapeType, ShapeRelation.INTERSECTS);
    }

    /**
     * A filter to filter indexed shapes that are contained by a shape
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeFilterBuilder geoWithinFilter(String name, ShapeBuilder shape) {
        return geoShapeFilter(name, shape, ShapeRelation.WITHIN);
    }

    public static GeoShapeFilterBuilder geoWithinFilter(String name, String indexedShapeId, String indexedShapeType) {
        return geoShapeFilter(name, indexedShapeId, indexedShapeType, ShapeRelation.WITHIN);
    }

    /**
     * A filter to filter indexed shapes that are not intersection with the query shape
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeFilterBuilder geoDisjointFilter(String name, ShapeBuilder shape) {
        return geoShapeFilter(name, shape, ShapeRelation.DISJOINT);
    }

    public static GeoShapeFilterBuilder geoDisjointFilter(String name, String indexedShapeId, String indexedShapeType) {
        return geoShapeFilter(name, indexedShapeId, indexedShapeType, ShapeRelation.DISJOINT);
    }

    /**
     * A filter to filter only documents where a field exists in them.
     *
     * @param name The name of the field
     */
    public static ExistsFilterBuilder existsFilter(String name) {
        return new ExistsFilterBuilder(name);
    }

    /**
     * A filter to filter only documents where a field does not exists in them.
     *
     * @param name The name of the field
     */
    public static MissingFilterBuilder missingFilter(String name) {
        return new MissingFilterBuilder(name);
    }

    /**
     * Constructs a child filter, with the child type and the query to run against child documents, with
     * the result of the filter being the *parent* documents.
     *
     * @param type  The child type
     * @param query The query to run against the child type
     */
    public static HasChildFilterBuilder hasChildFilter(String type, QueryBuilder query) {
        return new HasChildFilterBuilder(type, query);
    }

    /**
     * Constructs a child filter, with the child type and the filter to run against child documents, with
     * the result of the filter being the *parent* documents.
     *
     * @param type   The child type
     * @param filter The query to run against the child type
     */
    public static HasChildFilterBuilder hasChildFilter(String type, FilterBuilder filter) {
        return new HasChildFilterBuilder(type, filter);
    }

    /**
     * Constructs a parent filter, with the parent type and the query to run against parent documents, with
     * the result of the filter being the *child* documents.
     *
     * @param parentType The parent type
     * @param query      The query to run against the parent type
     */
    public static HasParentFilterBuilder hasParentFilter(String parentType, QueryBuilder query) {
        return new HasParentFilterBuilder(parentType, query);
    }

    /**
     * Constructs a parent filter, with the parent type and the filter to run against parent documents, with
     * the result of the filter being the *child* documents.
     *
     * @param parentType The parent type
     * @param filter     The filter to run against the parent type
     */
    public static HasParentFilterBuilder hasParentFilter(String parentType, FilterBuilder filter) {
        return new HasParentFilterBuilder(parentType, filter);
    }

    public static BoolFilterBuilder boolFilter() {
        return new BoolFilterBuilder();
    }

    /**
     * @deprecated Use {@link #boolFilter()} instead
     */
    @Deprecated
    public static AndFilterBuilder andFilter(FilterBuilder... filters) {
        return new AndFilterBuilder(filters);
    }

    /**
     * @deprecated Use {@link #boolFilter()} instead
     */
    @Deprecated
    public static OrFilterBuilder orFilter(FilterBuilder... filters) {
        return new OrFilterBuilder(filters);
    }

    public static NotFilterBuilder notFilter(FilterBuilder filter) {
        return new NotFilterBuilder(filter);
    }

    public static IndicesFilterBuilder indicesFilter(FilterBuilder filter, String... indices) {
        return new IndicesFilterBuilder(filter, indices);
    }

    public static WrapperFilterBuilder wrapperFilter(String filter) {
        return new WrapperFilterBuilder(filter);
    }

    public static WrapperFilterBuilder wrapperFilter(byte[] data, int offset, int length) {
        return new WrapperFilterBuilder(data, offset, length);
    }

    /**
     * Constructs a bytes filter to generate a filter from a {@link BytesReference} source
     *
     * @param source The filter source
     */
    public static BytesFilterBuilder bytesFilter(BytesReference source) {
        return new BytesFilterBuilder(source);
    }

    private FilterBuilders() {

    }
}
