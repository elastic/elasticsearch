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

package org.elasticsearch.index.query.json;

/**
 * A static factory for simple "import static" usage.
 *
 * @author kimchy (shay.banon)
 */
public abstract class JsonQueryBuilders {

    /**
     * A query that match on all documents.
     */
    public static MatchAllJsonQueryBuilder matchAllQuery() {
        return new MatchAllJsonQueryBuilder();
    }

    /**
     * A query that generates the union of documents produced by its sub-queries, and that scores each document
     * with the maximum score for that document as produced by any sub-query, plus a tie breaking increment for any
     * additional matching sub-queries.
     */
    public static DisMaxJsonQueryBuilder disMaxQuery() {
        return new DisMaxJsonQueryBuilder();
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermJsonQueryBuilder termQuery(String name, String value) {
        return new TermJsonQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermJsonQueryBuilder termQuery(String name, int value) {
        return new TermJsonQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermJsonQueryBuilder termQuery(String name, long value) {
        return new TermJsonQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermJsonQueryBuilder termQuery(String name, float value) {
        return new TermJsonQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermJsonQueryBuilder termQuery(String name, double value) {
        return new TermJsonQueryBuilder(name, value);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringJsonQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name The name of the field
     */
    public static FieldJsonQueryBuilder fieldQuery(String name, String query) {
        return new FieldJsonQueryBuilder(name, query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringJsonQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public static FieldJsonQueryBuilder fieldQuery(String name, int query) {
        return new FieldJsonQueryBuilder(name, query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringJsonQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public static FieldJsonQueryBuilder fieldQuery(String name, long query) {
        return new FieldJsonQueryBuilder(name, query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringJsonQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public static FieldJsonQueryBuilder fieldQuery(String name, float query) {
        return new FieldJsonQueryBuilder(name, query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringJsonQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public static FieldJsonQueryBuilder fieldQuery(String name, double query) {
        return new FieldJsonQueryBuilder(name, query);
    }

    /**
     * A Query that matches documents containing terms with a specified prefix.
     *
     * @param name   The name of the field
     * @param prefix The prefix query
     */
    public static PrefixJsonQueryBuilder prefixQuery(String name, String prefix) {
        return new PrefixJsonQueryBuilder(name, prefix);
    }

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param name The field name
     */
    public static RangeJsonQueryBuilder rangeQuery(String name) {
        return new RangeJsonQueryBuilder(name);
    }

    /**
     * Implements the wildcard search query. Supported wildcards are <tt>*</tt>, which
     * matches any character sequence (including the empty one), and <tt>?</tt>,
     * which matches any single character. Note this query can be slow, as it
     * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
     * a Wildcard term should not start with one of the wildcards <tt>*</tt> or
     * <tt>?</tt>.
     *
     * @param name  The field name
     * @param query The wildcard query string
     */
    public static WildcardJsonQueryBuilder wildcardQuery(String name, String query) {
        return new WildcardJsonQueryBuilder(name, query);
    }

    /**
     * A query that parses a query string and runs it. There are two modes that this operates. The first,
     * when no field is added (using {@link QueryStringJsonQueryBuilder#field(String)}, will run the query once and non prefixed fields
     * will use the {@link QueryStringJsonQueryBuilder#defaultField(String)} set. The second, when one or more fields are added
     * (using {@link QueryStringJsonQueryBuilder#field(String)}), will run the parsed query against the provided fields, and combine
     * them either using DisMax or a plain boolean query (see {@link QueryStringJsonQueryBuilder#useDisMax(boolean)}).
     *
     * @param queryString The query string to run
     */
    public static QueryStringJsonQueryBuilder queryString(String queryString) {
        return new QueryStringJsonQueryBuilder(queryString);
    }

    /**
     * A Query that matches documents matching boolean combinations of other queries.
     */
    public static BoolJsonQueryBuilder boolQuery() {
        return new BoolJsonQueryBuilder();
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, String value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, int value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, long value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, float value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, double value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanFirstJsonQueryBuilder spanFirstQuery(JsonSpanQueryBuilder match, int end) {
        return new SpanFirstJsonQueryBuilder(match, end);
    }

    public static SpanNearJsonQueryBuilder spanNearQuery() {
        return new SpanNearJsonQueryBuilder();
    }

    public static SpanNotJsonQueryBuilder spanNotQuery() {
        return new SpanNotJsonQueryBuilder();
    }

    public static SpanOrJsonQueryBuilder spanOrQuery() {
        return new SpanOrJsonQueryBuilder();
    }

    /**
     * A query that applies a filter to the results of another query.
     *
     * @param queryBuilder  The query to apply the filter to
     * @param filterBuilder The filter to apply on the query
     */
    public static FilteredJsonQueryBuilder filtered(JsonQueryBuilder queryBuilder, JsonFilterBuilder filterBuilder) {
        return new FilteredJsonQueryBuilder(queryBuilder, filterBuilder);
    }

    /**
     * A query that wraps a filter and simply returns a constant score equal to the
     * query boost for every document in the filter.
     *
     * @param filterBuilder The filter to wrap in a constant score query
     */
    public static ConstantScoreQueryJsonQueryBuilder constantScoreQuery(JsonFilterBuilder filterBuilder) {
        return new ConstantScoreQueryJsonQueryBuilder(filterBuilder);
    }

    /**
     * A more like this query that finds documents that are "like" the provided {@link MoreLikeThisJsonQueryBuilder#likeText(String)}
     * which is checked against the fields the query is constructed with.
     *
     * @param fields The fields to run the query against
     */
    public static MoreLikeThisJsonQueryBuilder moreLikeThisQuery(String... fields) {
        return new MoreLikeThisJsonQueryBuilder(fields);
    }

    /**
     * A more like this query that finds documents that are "like" the provided {@link MoreLikeThisJsonQueryBuilder#likeText(String)}
     * which is checked against the "_all" field.
     */
    public static MoreLikeThisJsonQueryBuilder moreLikeThisQuery() {
        return new MoreLikeThisJsonQueryBuilder();
    }

    /**
     * A more like this query that runs against a specific field.
     *
     * @param name The field name
     */
    public static MoreLikeThisFieldJsonQueryBuilder moreLikeThisFieldQuery(String name) {
        return new MoreLikeThisFieldJsonQueryBuilder(name);
    }

    private JsonQueryBuilders() {

    }
}
