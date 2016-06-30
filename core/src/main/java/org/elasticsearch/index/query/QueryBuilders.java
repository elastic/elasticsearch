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

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.Template;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A static factory for simple "import static" usage.
 */
public abstract class QueryBuilders {

    /**
     * A query that matches on all documents.
     */
    public static MatchAllQueryBuilder matchAllQuery() {
        return new MatchAllQueryBuilder();
    }

    /**
     * Creates a match query with type "BOOLEAN" for the provided field name and text.
     *
     * @param name The field name.
     * @param text The query text (to be analyzed).
     */
    public static MatchQueryBuilder matchQuery(String name, Object text) {
        return new MatchQueryBuilder(name, text);
    }

    /**
     * Creates a common query for the provided field name and text.
     *
     * @param fieldName The field name.
     * @param text The query text (to be analyzed).
     */
    public static CommonTermsQueryBuilder commonTermsQuery(String fieldName, Object text) {
        return new CommonTermsQueryBuilder(fieldName, text);
    }

    /**
     * Creates a match query with type "BOOLEAN" for the provided field name and text.
     *
     * @param fieldNames The field names.
     * @param text       The query text (to be analyzed).
     */
    public static MultiMatchQueryBuilder multiMatchQuery(Object text, String... fieldNames) {
        return new MultiMatchQueryBuilder(text, fieldNames); // BOOLEAN is the default
    }

    /**
     * Creates a text query with type "PHRASE" for the provided field name and text.
     *
     * @param name The field name.
     * @param text The query text (to be analyzed).
     */
    public static MatchPhraseQueryBuilder matchPhraseQuery(String name, Object text) {
        return new MatchPhraseQueryBuilder(name, text);
    }

    /**
     * Creates a match query with type "PHRASE_PREFIX" for the provided field name and text.
     *
     * @param name The field name.
     * @param text The query text (to be analyzed).
     */
    public static MatchPhrasePrefixQueryBuilder matchPhrasePrefixQuery(String name, Object text) {
        return new MatchPhrasePrefixQueryBuilder(name, text);
    }

    /**
     * A query that generates the union of documents produced by its sub-queries, and that scores each document
     * with the maximum score for that document as produced by any sub-query, plus a tie breaking increment for any
     * additional matching sub-queries.
     */
    public static DisMaxQueryBuilder disMaxQuery() {
        return new DisMaxQueryBuilder();
    }

    /**
     * Constructs a query that will match only specific ids within all types.
     */
    public static IdsQueryBuilder idsQuery() {
        return new IdsQueryBuilder();
    }

    /**
     * Constructs a query that will match only specific ids within types.
     *
     * @param types The mapping/doc type
     */
    public static IdsQueryBuilder idsQuery(String... types) {
        return new IdsQueryBuilder(types);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, String value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, int value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, long value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, float value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, double value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, boolean value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, Object value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents using fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     *
     * @deprecated Fuzzy queries are not useful enough and will be removed with Elasticsearch 4.0. In most cases you may want to use
     * a match query with the fuzziness parameter for strings or range queries for numeric and date fields.
     *
     * @see #matchQuery(String, Object)
     * @see #rangeQuery(String)
     */
    @Deprecated
    public static FuzzyQueryBuilder fuzzyQuery(String name, String value) {
        return new FuzzyQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents using fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     *
     * @deprecated Fuzzy queries are not useful enough and will be removed with Elasticsearch 4.0. In most cases you may want to use
     * a match query with the fuzziness parameter for strings or range queries for numeric and date fields.
     *
     * @see #matchQuery(String, Object)
     * @see #rangeQuery(String)
     */
    @Deprecated
    public static FuzzyQueryBuilder fuzzyQuery(String name, Object value) {
        return new FuzzyQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing terms with a specified prefix.
     *
     * @param name   The name of the field
     * @param prefix The prefix query
     */
    public static PrefixQueryBuilder prefixQuery(String name, String prefix) {
        return new PrefixQueryBuilder(name, prefix);
    }

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param name The field name
     */
    public static RangeQueryBuilder rangeQuery(String name) {
        return new RangeQueryBuilder(name);
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
    public static WildcardQueryBuilder wildcardQuery(String name, String query) {
        return new WildcardQueryBuilder(name, query);
    }


    /**
     * A Query that matches documents containing terms with a specified regular expression.
     *
     * @param name   The name of the field
     * @param regexp The regular expression
     */
    public static RegexpQueryBuilder regexpQuery(String name, String regexp) {
        return new RegexpQueryBuilder(name, regexp);
    }

    /**
     * A query that parses a query string and runs it. There are two modes that this operates. The first,
     * when no field is added (using {@link QueryStringQueryBuilder#field(String)}, will run the query once and non prefixed fields
     * will use the {@link QueryStringQueryBuilder#defaultField(String)} set. The second, when one or more fields are added
     * (using {@link QueryStringQueryBuilder#field(String)}), will run the parsed query against the provided fields, and combine
     * them either using DisMax or a plain boolean query (see {@link QueryStringQueryBuilder#useDisMax(boolean)}).
     *
     * @param queryString The query string to run
     */
    public static QueryStringQueryBuilder queryStringQuery(String queryString) {
        return new QueryStringQueryBuilder(queryString);
    }

    /**
     * A query that acts similar to a query_string query, but won't throw
     * exceptions for any weird string syntax. See
     * {@link org.apache.lucene.queryparser.simple.SimpleQueryParser} for the full
     * supported syntax.
     */
    public static SimpleQueryStringBuilder simpleQueryStringQuery(String queryString) {
        return new SimpleQueryStringBuilder(queryString);
    }

    /**
     * The BoostingQuery class can be used to effectively demote results that match a given query.
     * Unlike the "NOT" clause, this still selects documents that contain undesirable terms,
     * but reduces their overall score:
     */
    public static BoostingQueryBuilder boostingQuery(QueryBuilder positiveQuery, QueryBuilder negativeQuery) {
        return new BoostingQueryBuilder(positiveQuery, negativeQuery);
    }

    /**
     * A Query that matches documents matching boolean combinations of other queries.
     */
    public static BoolQueryBuilder boolQuery() {
        return new BoolQueryBuilder();
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, String value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, int value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, long value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, float value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, double value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanFirstQueryBuilder spanFirstQuery(SpanQueryBuilder match, int end) {
        return new SpanFirstQueryBuilder(match, end);
    }

    public static SpanNearQueryBuilder spanNearQuery(SpanQueryBuilder initialClause, int slop) {
        return new SpanNearQueryBuilder(initialClause, slop);
    }

    public static SpanNotQueryBuilder spanNotQuery(SpanQueryBuilder include, SpanQueryBuilder exclude) {
        return new SpanNotQueryBuilder(include, exclude);
    }

    public static SpanOrQueryBuilder spanOrQuery(SpanQueryBuilder initialClause) {
        return new SpanOrQueryBuilder(initialClause);
    }

    /** Creates a new {@code span_within} builder.
    * @param big the big clause, it must enclose {@code little} for a match.
    * @param little the little clause, it must be contained within {@code big} for a match.
    */
    public static SpanWithinQueryBuilder spanWithinQuery(SpanQueryBuilder big, SpanQueryBuilder little) {
        return new SpanWithinQueryBuilder(big, little);
    }

    /**
     * Creates a new {@code span_containing} builder.
     * @param big the big clause, it must enclose {@code little} for a match.
     * @param little the little clause, it must be contained within {@code big} for a match.
     */
    public static SpanContainingQueryBuilder spanContainingQuery(SpanQueryBuilder big, SpanQueryBuilder little) {
        return new SpanContainingQueryBuilder(big, little);
    }

    /**
     * Creates a {@link SpanQueryBuilder} which allows having a sub query
     * which implements {@link MultiTermQueryBuilder}. This is useful for
     * having e.g. wildcard or fuzzy queries inside spans.
     *
     * @param multiTermQueryBuilder The {@link MultiTermQueryBuilder} that
     *                              backs the created builder.
     */

    public static SpanMultiTermQueryBuilder spanMultiTermQueryBuilder(MultiTermQueryBuilder multiTermQueryBuilder) {
        return new SpanMultiTermQueryBuilder(multiTermQueryBuilder);
    }

    public static FieldMaskingSpanQueryBuilder fieldMaskingSpanQuery(SpanQueryBuilder query, String field) {
        return new FieldMaskingSpanQueryBuilder(query, field);
    }

    /**
     * A query that wraps another query and simply returns a constant score equal to the
     * query boost for every document in the query.
     *
     * @param queryBuilder The query to wrap in a constant score query
     */
    public static ConstantScoreQueryBuilder constantScoreQuery(QueryBuilder queryBuilder) {
        return new ConstantScoreQueryBuilder(queryBuilder);
    }

    /**
     * A function_score query with no functions.
     *
     * @param queryBuilder The query to custom score
     * @return the function score query
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(QueryBuilder queryBuilder) {
        return new FunctionScoreQueryBuilder(queryBuilder);
    }

    /**
     * A query that allows to define a custom scoring function
     *
     * @param queryBuilder The query to custom score
     * @param filterFunctionBuilders the filters and functions to execute
     * @return the function score query
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(QueryBuilder queryBuilder, FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders) {
        return new FunctionScoreQueryBuilder(queryBuilder, filterFunctionBuilders);
    }

    /**
     * A query that allows to define a custom scoring function
     *
     * @param filterFunctionBuilders the filters and functions to execute
     * @return the function score query
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders) {
        return new FunctionScoreQueryBuilder(filterFunctionBuilders);
    }

    /**
     * A query that allows to define a custom scoring function.
     *
     * @param function The function builder used to custom score
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(ScoreFunctionBuilder function) {
        return new FunctionScoreQueryBuilder(function);
    }

    /**
     * A query that allows to define a custom scoring function.
     *
     * @param queryBuilder The query to custom score
     * @param function     The function builder used to custom score
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(QueryBuilder queryBuilder, ScoreFunctionBuilder function) {
        return (new FunctionScoreQueryBuilder(queryBuilder, function));
    }

    /**
     * A more like this query that finds documents that are "like" the provided texts or documents
     * which is checked against the fields the query is constructed with.
     *
     * @param fields the field names that will be used when generating the 'More Like This' query.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public static MoreLikeThisQueryBuilder moreLikeThisQuery(String[] fields, String[] likeTexts, Item[] likeItems) {
        return new MoreLikeThisQueryBuilder(fields, likeTexts, likeItems);
    }

    /**
     * A more like this query that finds documents that are "like" the provided texts or documents
     * which is checked against the "_all" field.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public static MoreLikeThisQueryBuilder moreLikeThisQuery(String[] likeTexts, Item[] likeItems) {
        return moreLikeThisQuery(null, likeTexts, likeItems);
    }

    /**
     * A more like this query that finds documents that are "like" the provided texts
     * which is checked against the "_all" field.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     */
    public static MoreLikeThisQueryBuilder moreLikeThisQuery(String[] likeTexts) {
        return moreLikeThisQuery(null, likeTexts, null);
    }

    /**
     * A more like this query that finds documents that are "like" the provided documents
     * which is checked against the "_all" field.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public static MoreLikeThisQueryBuilder moreLikeThisQuery(Item[] likeItems) {
        return moreLikeThisQuery(null, null, likeItems);
    }

    /**
     * Constructs a new has_child query, with the child type and the query to run on the child documents. The
     * results of this query are the parent docs that those child docs matched.
     *
     * @param type      The child type.
     * @param query     The query.
     * @param scoreMode How the scores from the children hits should be aggregated into the parent hit.
     */
    public static HasChildQueryBuilder hasChildQuery(String type, QueryBuilder query, ScoreMode scoreMode) {
        return new HasChildQueryBuilder(type, query, scoreMode);
    }

    /**
     * Constructs a new parent query, with the parent type and the query to run on the parent documents. The
     * results of this query are the children docs that those parent docs matched.
     *
     * @param type      The parent type.
     * @param query     The query.
     * @param score     Whether the score from the parent hit should propogate to the child hit
     */
    public static HasParentQueryBuilder hasParentQuery(String type, QueryBuilder query, boolean score) {
        return new HasParentQueryBuilder(type, query, score);
    }

    /**
     * Constructs a new parent id query that returns all child documents of the specified type that
     * point to the specified id.
     */
    public static ParentIdQueryBuilder parentId(String type, String id) {
        return new ParentIdQueryBuilder(type, id);
    }

    public static NestedQueryBuilder nestedQuery(String path, QueryBuilder query, ScoreMode scoreMode) {
        return new NestedQueryBuilder(path, query, scoreMode);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, String... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, int... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, long... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, float... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, double... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, Object... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, Collection<?> values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A query that will execute the wrapped query only for the specified
     * indices, and "match_all" when it does not match those indices.
     *
     * @deprecated instead search on the `_index` field
     */
    @Deprecated
    public static IndicesQueryBuilder indicesQuery(QueryBuilder queryBuilder, String... indices) {
        // TODO remove this method in 6.0
        return new IndicesQueryBuilder(queryBuilder, indices);
    }

    /**
     * A Query builder which allows building a query thanks to a JSON string or binary data.
     */
    public static WrapperQueryBuilder wrapperQuery(String source) {
        return new WrapperQueryBuilder(source);
    }

    /**
     * A Query builder which allows building a query thanks to a JSON string or binary data.
     */
    public static WrapperQueryBuilder wrapperQuery(BytesReference source) {
        return new WrapperQueryBuilder(source);
    }

    /**
     * A Query builder which allows building a query thanks to a JSON string or binary data.
     */
    public static WrapperQueryBuilder wrapperQuery(byte[] source) {
        return new WrapperQueryBuilder(source);
    }

    /**
     * Facilitates creating template query requests using an inline script
     */
    public static TemplateQueryBuilder templateQuery(Template template) {
        return new TemplateQueryBuilder(template);
    }

    /**
     * Facilitates creating template query requests using an inline script
     */
    public static TemplateQueryBuilder templateQuery(String template, Map<String, Object> vars) {
        return new TemplateQueryBuilder(new Template(template, ScriptService.ScriptType.INLINE, null, null, vars));
    }

    /**
     * Facilitates creating template query requests
     */
    public static TemplateQueryBuilder templateQuery(String template, ScriptService.ScriptType templateType, Map<String, Object> vars) {
        return new TemplateQueryBuilder(new Template(template, templateType, null, null, vars));
    }

    /**
     * A filter based on doc/mapping type.
     */
    public static TypeQueryBuilder typeQuery(String type) {
        return new TypeQueryBuilder(type);
    }

    /**
     * A terms query that can extract the terms from another doc in an index.
     */
    public static TermsQueryBuilder termsLookupQuery(String name, TermsLookup termsLookup) {
        return new TermsQueryBuilder(name, termsLookup);
    }

    /**
     * A builder for filter based on a script.
     *
     * @param script The script to filter by.
     */
    public static ScriptQueryBuilder scriptQuery(Script script) {
        return new ScriptQueryBuilder(script);
    }


    /**
     * A filter to filter based on a specific distance from a specific geo location / point.
     *
     * @param name The location field name.
     */
    public static GeoDistanceQueryBuilder geoDistanceQuery(String name) {
        return new GeoDistanceQueryBuilder(name);
    }

    /**
     * A filter to filter based on a specific range from a specific geo location / point.
     *
     * @param name The location field name.
     * @param point The point
     */
    public static GeoDistanceRangeQueryBuilder geoDistanceRangeQuery(String name, GeoPoint point) {
        return new GeoDistanceRangeQueryBuilder(name, point);
    }

    /**
     * A filter to filter based on a specific range from a specific geo location / point.
     *
     * @param name The location field name.
     * @param geohash The point as geohash
     */
    public static GeoDistanceRangeQueryBuilder geoDistanceRangeQuery(String name, String geohash) {
        return new GeoDistanceRangeQueryBuilder(name, geohash);
    }

    /**
     * A filter to filter based on a specific range from a specific geo location / point.
     *
     * @param name The location field name.
     * @param lat The points latitude
     * @param lon The points longitude
     */
    public static GeoDistanceRangeQueryBuilder geoDistanceRangeQuery(String name, double lat, double lon) {
        return new GeoDistanceRangeQueryBuilder(name, lat, lon);
    }

    /**
     * A filter to filter based on a bounding box defined by top left and bottom right locations / points
     *
     * @param name The location field name.
     */
    public static GeoBoundingBoxQueryBuilder geoBoundingBoxQuery(String name) {
        return new GeoBoundingBoxQueryBuilder(name);
    }

    /**
     * A filter based on a bounding box defined by geohash. The field this filter is applied to
     * must have <code>{&quot;type&quot;:&quot;geo_point&quot;, &quot;geohash&quot;:true}</code>
     * to work.
     *
     * @param name The geo point field name.
     * @param geohash The Geohash to filter
     */
    public static GeohashCellQuery.Builder geoHashCellQuery(String name, String geohash) {
        return new GeohashCellQuery.Builder(name, geohash);
    }

    /**
     * A filter based on a bounding box defined by geohash. The field this filter is applied to
     * must have <code>{&quot;type&quot;:&quot;geo_point&quot;, &quot;geohash&quot;:true}</code>
     * to work.
     *
     * @param name The geo point field name.
     * @param point a geo point within the geohash bucket
     */
    public static GeohashCellQuery.Builder geoHashCellQuery(String name, GeoPoint point) {
        return new GeohashCellQuery.Builder(name, point);
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
    public static GeohashCellQuery.Builder geoHashCellQuery(String name, String geohash, boolean neighbors) {
        return new GeohashCellQuery.Builder(name, geohash, neighbors);
    }

    /**
     * A filter to filter based on a polygon defined by a set of locations  / points.
     *
     * @param name The location field name.
     */
    public static GeoPolygonQueryBuilder geoPolygonQuery(String name, List<GeoPoint> points) {
        return new GeoPolygonQueryBuilder(name, points);
    }

    /**
     * A filter based on the relationship of a shape and indexed shapes
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoShapeQuery(String name, ShapeBuilder shape) throws IOException {
        return new GeoShapeQueryBuilder(name, shape);
    }

    public static GeoShapeQueryBuilder geoShapeQuery(String name, String indexedShapeId, String indexedShapeType) {
        return new GeoShapeQueryBuilder(name, indexedShapeId, indexedShapeType);
    }

    /**
     * A filter to filter indexed shapes intersecting with shapes
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoIntersectionQuery(String name, ShapeBuilder shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.INTERSECTS);
        return builder;
    }

    public static GeoShapeQueryBuilder geoIntersectionQuery(String name, String indexedShapeId, String indexedShapeType) {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, indexedShapeId, indexedShapeType);
        builder.relation(ShapeRelation.INTERSECTS);
        return builder;
    }

    /**
     * A filter to filter indexed shapes that are contained by a shape
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoWithinQuery(String name, ShapeBuilder shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.WITHIN);
        return builder;
    }

    public static GeoShapeQueryBuilder geoWithinQuery(String name, String indexedShapeId, String indexedShapeType) {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, indexedShapeId, indexedShapeType);
        builder.relation(ShapeRelation.WITHIN);
        return builder;
    }

    /**
     * A filter to filter indexed shapes that are not intersection with the query shape
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoDisjointQuery(String name, ShapeBuilder shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.DISJOINT);
        return builder;
    }

    public static GeoShapeQueryBuilder geoDisjointQuery(String name, String indexedShapeId, String indexedShapeType) {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, indexedShapeId, indexedShapeType);
        builder.relation(ShapeRelation.DISJOINT);
        return builder;
    }

    /**
     * A filter to filter only documents where a field exists in them.
     *
     * @param name The name of the field
     */
    public static ExistsQueryBuilder existsQuery(String name) {
        return new ExistsQueryBuilder(name);
    }

    private QueryBuilders() {

    }
}
