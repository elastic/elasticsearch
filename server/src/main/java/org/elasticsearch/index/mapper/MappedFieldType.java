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
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.DistanceFeatureQueryBuilder;
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
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This defines the core properties and functions to operate on a field.
 */
public abstract class MappedFieldType {

    private final String name;
    private final boolean docValues;
    private final boolean isIndexed;
    private final boolean isStored;
    private final TextSearchInfo textSearchInfo;
    private final Map<String, String> meta;

    public MappedFieldType(String name, boolean isIndexed, boolean isStored,
                           boolean hasDocValues, TextSearchInfo textSearchInfo, Map<String, String> meta) {
        this.name = Objects.requireNonNull(name);
        this.isIndexed = isIndexed;
        this.isStored = isStored;
        this.docValues = hasDocValues;
        this.textSearchInfo = Objects.requireNonNull(textSearchInfo);
        this.meta = Objects.requireNonNull(meta);
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
        throw new IllegalArgumentException("Fielddata is not supported on field [" + name() + "] of type [" + typeName() + "]");
    }

    /**
     * Create a helper class to fetch field values during the {@link FetchFieldsPhase}.
     *
     * New field types must implement this method in order to support the search 'fields' option. Except
     * for metadata fields, field types should not throw {@link UnsupportedOperationException} since this
     * could cause a search retrieving multiple fields (like "fields": ["*"]) to fail.
     */
    public abstract ValueFetcher valueFetcher(SearchExecutionContext context, @Nullable String format);

    /** Returns the name of this type, as would be specified in mapping properties */
    public abstract String typeName();

    /** Returns the field family type, as used in field capabilities */
    public String familyTypeName() {
        return typeName();
    }

    public String name() {
        return name;
    }

    public boolean hasDocValues() {
        return docValues;
    }

    /**
     * Returns the collapse type of the field
     * CollapseType.NONE means the field can'be used for collapsing.
     * @return collapse type of the field
     */
    public CollapseType collapseType() {
        return CollapseType.NONE;
    }

    /** Given a value that comes from the stored fields API, convert it to the
     *  expected type. For instance a date field would store dates as longs and
     *  format it back to a string in this method. */
    public Object valueForDisplay(Object value) {
        return value;
    }

    /**
     * Returns true if the field is searchable.
     */
    public boolean isSearchable() {
        return isIndexed;
    }

    /**
     * Returns true if the field is stored separately.
     */
    public boolean isStored() {
        return isStored;
    }

    /**
     * If the field supports using the indexed data to speed up operations related to ordering of data, such as sorting or aggs, return
     * a function for doing that.  If it is unsupported for this field type, there is no need to override this method.
     *
     * @return null if the optimization cannot be applied, otherwise a function to use for the optimization
     */
    @Nullable
    public Function<byte[], Number> pointReaderIfPossible() {
        return null;
    }

    /** Returns true if the field is aggregatable.
     *
     */
    public boolean isAggregatable() {
        try {
            fielddataBuilder("", () -> {
                throw new UnsupportedOperationException("SearchLookup not available");
            });
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * @return true if field has been marked as a dimension field
     */
    public boolean isDimension() {
        return false;
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
    public abstract Query termQuery(Object value, @Nullable SearchExecutionContext context);


    // Case insensitive form of term query (not supported by all fields so must be overridden to enable)
    public Query termQueryCaseInsensitive(Object value, @Nullable SearchExecutionContext context) {
        throw new QueryShardException(context, "[" + name + "] field which is of type [" + typeName() +
            "], does not support case insensitive term queries");
    }

    /** Build a constant-scoring query that matches all values. The default implementation uses a
     * {@link ConstantScoreQuery} around a {@link BooleanQuery} whose {@link Occur#SHOULD} clauses
     * are generated with {@link #termQuery}. */
    public Query termsQuery(Collection<?> values, @Nullable SearchExecutionContext context) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Object value : values) {
            builder.add(termQuery(value, context), Occur.SHOULD);
        }
        return new ConstantScoreQuery(builder.build());
    }

    /**
     * Factory method for range queries.
     * @param relation the relation, nulls should be interpreted like INTERSECTS
     */
    public Query rangeQuery(
        Object lowerTerm, Object upperTerm,
        boolean includeLower, boolean includeUpper,
        ShapeRelation relation, ZoneId timeZone, DateMathParser parser,
        SearchExecutionContext context) {
        throw new IllegalArgumentException("Field [" + name + "] of type [" + typeName() + "] does not support range queries");
    }

    public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions,
                            SearchExecutionContext context) {
        throw new IllegalArgumentException("Can only use fuzzy queries on keyword and text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    // Case sensitive form of prefix query
    public final Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        return prefixQuery(value, method, false, context);
    }

    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, boolean caseInsensitve,
        SearchExecutionContext context) {
        throw new QueryShardException(context, "Can only use prefix queries on keyword, text and wildcard fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    // Case sensitive form of wildcard query
    public final Query wildcardQuery(String value,
        @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context
    ) {
        return wildcardQuery(value, method, false, context);
    }

    public Query wildcardQuery(String value,
                               @Nullable MultiTermQuery.RewriteMethod method,
                               boolean caseInsensitve, SearchExecutionContext context) {
        throw new QueryShardException(context, "Can only use wildcard queries on keyword, text and wildcard fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query normalizedWildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        throw new QueryShardException(context, "Can only use wildcard queries on keyword, text and wildcard fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query regexpQuery(String value, int syntaxFlags, int matchFlags, int maxDeterminizedStates,
        @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        throw new QueryShardException(context, "Can only use regexp queries on keyword and text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query existsQuery(SearchExecutionContext context) {
        if (hasDocValues()) {
            return new DocValuesFieldExistsQuery(name());
        } else if (getTextSearchInfo().hasNorms()) {
            return new NormsFieldExistsQuery(name());
        } else {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }
    }

    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements,
            SearchExecutionContext context) throws IOException {
        throw new IllegalArgumentException("Can only use phrase queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements,
        SearchExecutionContext context) throws IOException {
        throw new IllegalArgumentException("Can only use phrase queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext context) throws IOException {
        throw new IllegalArgumentException("Can only use phrase prefix queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, SearchExecutionContext context) {
        throw new IllegalArgumentException("Can only use span prefix queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
        throw new IllegalArgumentException("Illegal data type of [" + typeName() + "]!"+
            "[" + DistanceFeatureQueryBuilder.NAME + "] query can only be run on a date, date_nanos or geo_point field type!");
    }

    /**
     * Create an {@link IntervalsSource} for the given term.
     */
    public IntervalsSource termIntervals(BytesRef term, SearchExecutionContext context) {
        throw new IllegalArgumentException("Can only use interval queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    /**
     * Create an {@link IntervalsSource} for the given prefix.
     */
    public IntervalsSource prefixIntervals(BytesRef prefix, SearchExecutionContext context) {
        throw new IllegalArgumentException("Can only use interval queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    /**
     * Create a fuzzy {@link IntervalsSource} for the given term.
     */
    public IntervalsSource fuzzyIntervals(String term, int maxDistance, int prefixLength,
            boolean transpositions, SearchExecutionContext context) {
        throw new IllegalArgumentException("Can only use interval queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    /**
     * Create a wildcard {@link IntervalsSource} for the given pattern.
     */
    public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
        throw new IllegalArgumentException("Can only use interval queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    /**
     * An enum used to describe the relation between the range of terms in a
     * shard when compared with a query range
     */
    public enum Relation {
        WITHIN,
        INTERSECTS,
        DISJOINT
    }

    /** Return whether all values of the given {@link IndexReader} are within the range,
     *  outside the range or cross the range. The default implementation returns
     *  {@link Relation#INTERSECTS}, which is always fine to return when there is
     *  no way to check whether values are actually within bounds. */
    public Relation isFieldWithinQuery(
            IndexReader reader,
            Object from, Object to,
            boolean includeLower, boolean includeUpper,
            ZoneId timeZone, DateMathParser dateMathParser, QueryRewriteContext context) throws IOException {
        return Relation.INTERSECTS;
    }

    /** @throws IllegalArgumentException if the fielddata is not supported on this type.
     *  An IllegalArgumentException is needed in order to return an http error 400
     *  when this error occurs in a request. see: {@link org.elasticsearch.ExceptionsHelper#status}
     **/
    protected final void failIfNoDocValues() {
        if (hasDocValues() == false) {
            throw new IllegalArgumentException("Can't load fielddata on [" + name()
                + "] because fielddata is unsupported on fields of type ["
                + typeName() + "]. Use doc values instead.");
        }
    }

    protected final void failIfNotIndexed() {
        if (isIndexed == false) {
            // we throw an IAE rather than an ISE so that it translates to a 4xx code rather than 5xx code on the http layer
            throw new IllegalArgumentException("Cannot search on field [" + name() + "] since it is not indexed.");
        }
    }

    /**
     * @return if this field type should load global ordinals eagerly
     */
    public boolean eagerGlobalOrdinals() {
        return false;
    }

    /** Return a {@link DocValueFormat} that can be used to display and parse
     *  values as returned by the fielddata API.
     *  The default implementation returns a {@link DocValueFormat#RAW}. */
    public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
        }
        if (timeZone != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones");
        }
        return DocValueFormat.RAW;
    }

    /**
     * Extract a {@link Term} from a query created with {@link #termQuery} by
     * recursively removing {@link BoostQuery} wrappers.
     * @throws IllegalArgumentException if the wrapped query is not a {@link TermQuery}
     */
    public static Term extractTerm(Query termQuery) {
        while (termQuery instanceof BoostQuery) {
            termQuery = ((BoostQuery) termQuery).getQuery();
        }
        if (termQuery instanceof TermInSetQuery) {
            TermInSetQuery tisQuery = (TermInSetQuery) termQuery;
            PrefixCodedTerms terms = tisQuery.getTermData();
            if (terms.size() == 1) {
                TermIterator it = terms.iterator();
                BytesRef term = it.next();
                return new Term(it.field(), term);
            }
        }
        if (termQuery instanceof TermQuery == false) {
            throw new IllegalArgumentException("Cannot extract a term from a query of type "
                    + termQuery.getClass() + ": " + termQuery);
        }
        return ((TermQuery) termQuery).getTerm();
    }

    /**
     * Get the metadata associated with this field.
     */
    public Map<String, String> meta() {
        return meta;
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
        return textSearchInfo;
    }

    public enum CollapseType {
        NONE, // this field is not collapsable
        KEYWORD,
        NUMERIC
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
        return null;
    }
}
