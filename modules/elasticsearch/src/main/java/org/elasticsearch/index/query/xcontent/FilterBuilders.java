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

package org.elasticsearch.index.query.xcontent;

/**
 * A static factory for simple "import static" usage.
 *
 * @author kimchy (shay.banon)
 */
public abstract class FilterBuilders {

    /**
     * A filter that matches all documents.
     */
    public static MatchAllFilterBuilder matchAllFilter() {
        return new MatchAllFilterBuilder();
    }

    /**
     * Creates a new ids filter with the provided doc/mapping type.
     *
     * @param type The type
     */
    public static IdsFilterBuilder idsFilter(String type) {
        return new IdsFilterBuilder(type);
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
     * A filter that restricts search results to values that are within the given range.
     *
     * @param name The field name
     */
    public static RangeFilterBuilder rangeFilter(String name) {
        return new RangeFilterBuilder(name);
    }

    /**
     * A filter that restricts search results to values that are within the given numeric range. Uses the
     * field data cache (loading all the values for the specified field into memory)
     *
     * @param name The field name
     */
    public static NumericRangeFilterBuilder numericRangeFilter(String name) {
        return new NumericRangeFilterBuilder(name);
    }

    /**
     * A filter that simply wraps a query.
     *
     * @param queryBuilder The query to wrap as a filter
     */
    public static QueryFilterBuilder queryFilter(XContentQueryBuilder queryBuilder) {
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
     * A filter to filter based on a polygon defined by a set of locations  / points.
     *
     * @param name The location field name.
     */
    public static GeoPolygonFilterBuilder geoPolygonFilter(String name) {
        return new GeoPolygonFilterBuilder(name);
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
    public static HasChildFilterBuilder hasChildFilter(String type, XContentQueryBuilder query) {
        return new HasChildFilterBuilder(type, query);
    }

    public static BoolFilterBuilder boolFilter() {
        return new BoolFilterBuilder();
    }

    public static AndFilterBuilder andFilter(XContentFilterBuilder... filters) {
        return new AndFilterBuilder(filters);
    }

    public static OrFilterBuilder orFilter(XContentFilterBuilder... filters) {
        return new OrFilterBuilder(filters);
    }

    public static NotFilterBuilder notFilter(XContentFilterBuilder filter) {
        return new NotFilterBuilder(filter);
    }

    private FilterBuilders() {

    }
}
