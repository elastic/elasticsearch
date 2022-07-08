/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.fetch.subphase.FetchFieldsPhase;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class MappedField<T extends MappedFieldType> {

    private final String name;
    private final T type;

    public MappedField(String name, T type) {
        this.name = Mapper.internFieldName(name);
        this.type = type;
    }

    public String name() {
        return name;
    }

    public T type() {
        return type;
    }

    /**
     * Return a fielddata builder for this field
     *
     * @param fullyQualifiedIndexName the name of the index this field-data is build for
     * @param searchLookup a {@link SearchLookup} supplier to allow for accessing other fields values in the context of runtime fields
     * @throws IllegalArgumentException if the fielddata is not supported on this type.
     * An IllegalArgumentException is needed in order to return an http error 400
     * when this error occurs in a request. see: {@link org.elasticsearch.ExceptionsHelper#status}
     */
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return type.fielddataBuilder(name, fullyQualifiedIndexName, searchLookup);
    }

    /**
     * Create a helper class to fetch field values during the {@link FetchFieldsPhase}.
     *
     * New field types must implement this method in order to support the search 'fields' option. Except
     * for metadata fields, field types should not throw {@link UnsupportedOperationException} since this
     * could cause a search retrieving multiple fields (like "fields": ["*"]) to fail.
     */
    public ValueFetcher valueFetcher(SearchExecutionContext context, @Nullable String format) {
        return type.valueFetcher(name, context, format);
    }

    /** Returns the name of this type, as would be specified in mapping properties */
    public String typeName() {
        return type.typeName();
    }

    /** Returns the field family type, as used in field capabilities */
    public String familyTypeName() {
        return type.familyTypeName();
    }

    public boolean hasDocValues() {
        return type.hasDocValues();
    }

    /**
     * Returns the collapse type of the field
     * CollapseType.NONE means the field can'be used for collapsing.
     * @return collapse type of the field
     */
    public MappedFieldType.CollapseType collapseType() {
        return type.collapseType();
    }

    /** Given a value that comes from the stored fields API, convert it to the
     *  expected type. For instance a date field would store dates as longs and
     *  format it back to a string in this method. */
    public Object valueForDisplay(Object value) {
        return type.valueForDisplay(value);
    }

    /**
     * Returns true if the field is searchable.
     */
    public boolean isSearchable() {
        return type.isSearchable();
    }

    /**
     * Returns true if the field is indexed.
     */
    public final boolean isIndexed() {
        return type.isIndexed();
    }

    /**
     * Returns true if the field is stored separately.
     */
    public final boolean isStored() {
        return type.isStored();
    }

    /**
     * If the field supports using the indexed data to speed up operations related to ordering of data, such as sorting or aggs, return
     * a function for doing that.  If it is unsupported for this field type, there is no need to override this method.
     *
     * @return null if the optimization cannot be applied, otherwise a function to use for the optimization
     */
    @Nullable
    public Function<byte[], Number> pointReaderIfPossible() {
        return type.pointReaderIfPossible();
    }

    /** Returns true if the field is aggregatable.
     *
     */
    public boolean isAggregatable() {
        return type.isAggregatable(name);
    }

    /**
     * @return true if field has been marked as a dimension field
     */
    public boolean isDimension() {
        return type.isDimension();
    }

    /**
     * @return metric type or null if the field is not a metric field
     */
    public TimeSeriesParams.MetricType getMetricType() {
        return type.getMetricType();
    }

    /** Generates a query that will only match documents that contain the given value.
     *  The default implementation returns a {@link TermQuery} over the value bytes
     *  @throws IllegalArgumentException if {@code value} cannot be converted to the expected data type or if the field is not searchable
     *      due to the way it is configured (eg. not indexed)
     *  @throws ElasticsearchParseException if {@code value} cannot be converted to the expected data type
     *  @throws UnsupportedOperationException if the field is not searchable regardless of options
     *  @throws QueryShardException if the field is not searchable regardless of options
     */
    // TODO: Standardize exception types
    public Query termQuery(Object value, @Nullable SearchExecutionContext context) {
        return type.termQuery(name, value, context);
    }

    // Case insensitive form of term query (not supported by all fields so must be overridden to enable)
    public Query termQueryCaseInsensitive(Object value, @Nullable SearchExecutionContext context) {
        return type.termQueryCaseInsensitive(name, value, context);
    }

    /** Build a constant-scoring query that matches all values. The default implementation uses a
     * {@link ConstantScoreQuery} around a {@link BooleanQuery} whose {@link BooleanClause.Occur#SHOULD} clauses
     * are generated with {@link #termQuery}. */
    public Query termsQuery(Collection<?> values, @Nullable SearchExecutionContext context) {
        return type.termsQuery(name, values, context);
    }

    /**
     * Factory method for range queries.
     * @param relation the relation, nulls should be interpreted like INTERSECTS
     */
    public Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ShapeRelation relation,
        ZoneId timeZone,
        DateMathParser parser,
        SearchExecutionContext context
    ) {
        return type.rangeQuery(name, lowerTerm, upperTerm, includeLower, includeUpper, relation, timeZone, parser, context);
    }

    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        SearchExecutionContext context
    ) {
        return type.fuzzyQuery(name, value, fuzziness, prefixLength, maxExpansions, transpositions, context);
    }

    // Case sensitive form of prefix query
    public final Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        return type.prefixQuery(name, value, method, context);
    }

    public Query prefixQuery(
        String value,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitve,
        SearchExecutionContext context
    ) {
        return type.prefixQuery(name, value, method, caseInsensitve, context);
    }

    // Case sensitive form of wildcard query
    public final Query wildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        return type.wildcardQuery(name, value, method, context);
    }

    public Query wildcardQuery(
        String value,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitve,
        SearchExecutionContext context
    ) {
        return type.wildcardQuery(name, value, method, caseInsensitve, context);
    }

    public Query normalizedWildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        return type.normalizedWildcardQuery(name, value, method, context);
    }

    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        @Nullable MultiTermQuery.RewriteMethod method,
        SearchExecutionContext context
    ) {
        return type.regexpQuery(name, value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
    }

    public Query existsQuery(SearchExecutionContext context) {
        return type.existsQuery(name, context);
    }

    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context)
        throws IOException {
        return type.phraseQuery(name, stream, slop, enablePositionIncrements, context);
    }

    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context)
        throws IOException {
        return type.multiPhraseQuery(name, stream, slop, enablePositionIncrements, context);
    }

    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext context) throws IOException {
        return type.phrasePrefixQuery(name, stream, slop, maxExpansions, context);
    }

    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, SearchExecutionContext context) {
        return type.spanPrefixQuery(name, value, method, context);
    }

    public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
        return type.distanceFeatureQuery(name, origin, pivot, context);
    }

    /**
     * Create an {@link IntervalsSource} for the given term.
     */
    public IntervalsSource termIntervals(BytesRef term, SearchExecutionContext context) {
        return type.termIntervals(name, term, context);
    }

    /**
     * Create an {@link IntervalsSource} for the given prefix.
     */
    public IntervalsSource prefixIntervals(BytesRef prefix, SearchExecutionContext context) {
        return type.prefixIntervals(name, prefix, context);
    }

    /**
     * Create a fuzzy {@link IntervalsSource} for the given term.
     */
    public IntervalsSource fuzzyIntervals(
        String term,
        int maxDistance,
        int prefixLength,
        boolean transpositions,
        SearchExecutionContext context
    ) {
        return type.fuzzyIntervals(name, term, maxDistance, prefixLength, transpositions, context);
    }

    /**
     * Create a wildcard {@link IntervalsSource} for the given pattern.
     */
    public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
        return type.wildcardIntervals(name, pattern, context);
    }

    /** Return whether all values of the given {@link IndexReader} are within the range,
     *  outside the range or cross the range. The default implementation returns
     *  {@link MappedFieldType.Relation#INTERSECTS}, which is always fine to return when there is
     *  no way to check whether values are actually within bounds. */
    public MappedFieldType.Relation isFieldWithinQuery(
        IndexReader reader,
        Object from,
        Object to,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser dateMathParser,
        QueryRewriteContext context
    ) throws IOException {
        return type.isFieldWithinQuery(name, reader, from, to, includeLower, includeLower, timeZone, dateMathParser, context);
    }

    /**
     * @return if this field type should load global ordinals eagerly
     */
    public boolean eagerGlobalOrdinals() {
        return type.eagerGlobalOrdinals();
    }

    /**
     * @return if the field may have values in the underlying index
     *
     * Note that this should only return {@code false} if it is not possible for it to
     * match on a term query.
     *
     * @see org.elasticsearch.index.search.QueryParserHelper
     */
    public boolean mayExistInIndex(SearchExecutionContext context) {
        return type.mayExistInIndex(name, context);
    }

    /**
     * Pick a {@link DocValueFormat} that can be used to display and parse
     * values of fields of this type.
     */
    public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
        return type.docValueFormat(name, format, timeZone);
    }

    /**
     * Get the metadata associated with this field.
     */
    public Map<String, String> meta() {
        return type.meta();
    }

    /**
     * Returns information on how any text in this field is indexed
     *
     * Fields that do not support any text-based queries should return
     * {@link TextSearchInfo#NONE}.  Some fields (eg keyword) may support
     * only simple match queries, and can return
     * {@link TextSearchInfo#SIMPLE_MATCH_ONLY}; other fields may support
     * simple match queries without using the terms index, and can return
     * {@link TextSearchInfo#SIMPLE_MATCH_WITHOUT_TERMS}
     */
    public TextSearchInfo getTextSearchInfo() {
        return type.getTextSearchInfo();
    }

    /**
     * This method is used to support auto-complete services and implementations
     * are expected to find terms beginning with the provided string very quickly.
     * If fields cannot look up matching terms quickly they should return null.
     * The returned TermEnum should implement next(), term() and doc_freq() methods
     * but postings etc are not required.
     * @param caseInsensitive if matches should be case insensitive
     * @param string the partially complete word the user has typed (can be empty)
     * @param queryShardContext the shard context
     * @param searchAfter - usually null. If supplied the TermsEnum result must be positioned after the provided term (used for pagination)
     * @return null or an enumeration of matching terms and their doc frequencies
     * @throws IOException Errors accessing data
     */
    public TermsEnum getTerms(boolean caseInsensitive, String string, SearchExecutionContext queryShardContext, String searchAfter)
        throws IOException {
        return type.getTerms(name, caseInsensitive, string, queryShardContext, searchAfter);
    }
}
