/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.DistanceFeatureQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.fetch.subphase.FetchFieldsPhase;
import org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

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

    public MappedFieldType(
        String name,
        boolean isIndexed,
        boolean isStored,
        boolean hasDocValues,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta
    ) {
        this.name = Mapper.internFieldName(name);
        this.isIndexed = isIndexed;
        this.isStored = isStored;
        this.docValues = hasDocValues;
        this.textSearchInfo = Objects.requireNonNull(textSearchInfo);
        // meta should be sorted but for the one item or empty case we can fall back to immutable maps to save some memory since order is
        // irrelevant
        this.meta = meta.size() <= 1 ? Map.copyOf(meta) : meta;
    }

    /**
     * Operation to specify what data structures are used to retrieve
     * field data from and generate a representation of doc values.
     */
    public enum FielddataOperation {
        SEARCH,
        SCRIPT
    }

    /**
     * Return a fielddata builder for this field
     *
     * @param fieldDataContext the context for the fielddata
     * @throws IllegalArgumentException if the fielddata is not supported on this type.
     * An IllegalArgumentException is needed in order to return an http error 400
     * when this error occurs in a request. see: {@link ExceptionsHelper#status}
     */
    public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
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
     * Returns true if the field is indexed.
     */
    public final boolean isIndexed() {
        return isIndexed;
    }

    /**
     * Returns true if the field is stored separately.
     */
    public final boolean isStored() {
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

    /**
     * Returns true if the field is aggregatable.
     */
    public boolean isAggregatable() {
        return hasDocValues();
    }

    /**
     * @return true if field has been marked as a dimension field
     */
    public boolean isDimension() {
        return false;
    }

    /**
     * @return true if field has script values.
     */
    public boolean hasScriptValues() {
        return false;
    }

    /**
     * @return a list of dimension fields. Expected to be used by fields that have
     * nested fields or that, in some way, identify a collection of fields by means
     * of a top level field (like flattened fields).
     */
    public List<String> dimensions() {
        return Collections.emptyList();
    }

    /**
     * @return metric type or null if the field is not a metric field
     */
    public TimeSeriesParams.MetricType getMetricType() {
        return null;
    }

    /**
     * Returns the default highlighter type to use when highlighting the field.
     */
    public String getDefaultHighlighter() {
        return DefaultHighlighter.NAME;
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
        throw new QueryShardException(
            context,
            "[" + name + "] field which is of type [" + typeName() + "], does not support case insensitive term queries"
        );
    }

    /** Build a constant-scoring query that matches all values. The default implementation uses a
     * {@link ConstantScoreQuery} around a {@link BooleanQuery} whose {@link Occur#SHOULD} clauses
     * are generated with {@link #termQuery}. */
    public Query termsQuery(Collection<?> values, @Nullable SearchExecutionContext context) {
        Set<?> dedupe = new HashSet<>(values);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Object value : dedupe) {
            builder.add(termQuery(value, context), Occur.SHOULD);
        }
        return new ConstantScoreQuery(builder.build());
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
        throw new IllegalArgumentException("Field [" + name + "] of type [" + typeName() + "] does not support range queries");
    }

    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        SearchExecutionContext context,
        @Nullable MultiTermQuery.RewriteMethod rewriteMethod
    ) {
        throw new IllegalArgumentException(
            "Can only use fuzzy queries on keyword and text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        SearchExecutionContext context
    ) {
        return fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context, null);
    }

    // Case sensitive form of prefix query
    public final Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        return prefixQuery(value, method, false, context);
    }

    public Query prefixQuery(
        String value,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitve,
        SearchExecutionContext context
    ) {
        throw new QueryShardException(
            context,
            "Can only use prefix queries on keyword, text and wildcard fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    // Case sensitive form of wildcard query
    public final Query wildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        return wildcardQuery(value, method, false, context);
    }

    public Query wildcardQuery(
        String value,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitve,
        SearchExecutionContext context
    ) {
        throw new QueryShardException(
            context,
            "Can only use wildcard queries on keyword, text and wildcard fields - not on ["
                + name
                + "] which is of type ["
                + typeName()
                + "]"
        );
    }

    public Query normalizedWildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        throw new QueryShardException(
            context,
            "Can only use wildcard queries on keyword, text and wildcard fields - not on ["
                + name
                + "] which is of type ["
                + typeName()
                + "]"
        );
    }

    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        @Nullable MultiTermQuery.RewriteMethod method,
        SearchExecutionContext context
    ) {
        throw new QueryShardException(
            context,
            "Can only use regexp queries on keyword and text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query existsQuery(SearchExecutionContext context) {
        if (hasDocValues() || getTextSearchInfo().hasNorms()) {
            return new FieldExistsQuery(name());
        } else {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }
    }

    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context)
        throws IOException {
        throw new IllegalArgumentException(
            "Can only use phrase queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context)
        throws IOException {
        throw new IllegalArgumentException(
            "Can only use phrase queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext context) throws IOException {
        throw new IllegalArgumentException(
            "Can only use phrase prefix queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use span prefix queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Illegal data type of ["
                + typeName()
                + "]!"
                + "["
                + DistanceFeatureQueryBuilder.NAME
                + "] query can only be run on a date, date_nanos or geo_point field type!"
        );
    }

    /**
     * Create an {@link IntervalsSource} for the given term.
     */
    public IntervalsSource termIntervals(BytesRef term, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create an {@link IntervalsSource} for the given prefix.
     */
    public IntervalsSource prefixIntervals(BytesRef prefix, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
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
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a wildcard {@link IntervalsSource} for the given pattern.
     */
    public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a regexp {@link IntervalsSource} for the given pattern.
     */
    public IntervalsSource regexpIntervals(BytesRef pattern, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a range {@link IntervalsSource} for the given ranges
     */
    public IntervalsSource rangeIntervals(
        BytesRef lowerTerm,
        BytesRef upperTerm,
        boolean includeLower,
        boolean includeUpper,
        SearchExecutionContext context
    ) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
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
        Object from,
        Object to,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser dateMathParser,
        QueryRewriteContext context
    ) throws IOException {
        return Relation.INTERSECTS;
    }

    /** @throws IllegalArgumentException if the fielddata is not supported on this type.
     *  An IllegalArgumentException is needed in order to return an http error 400
     *  when this error occurs in a request. see: {@link ExceptionsHelper#status}
     **/
    protected final void failIfNoDocValues() {
        if (hasDocValues() == false) {
            throw new IllegalArgumentException(
                "Can't load fielddata on ["
                    + name()
                    + "] because fielddata is unsupported on fields of type ["
                    + typeName()
                    + "]. Use doc values instead."
            );
        }
    }

    protected final void failIfNotIndexed() {
        if (isIndexed == false) {
            // we throw an IAE rather than an ISE so that it translates to a 4xx code rather than 5xx code on the http layer
            throw new IllegalArgumentException("Cannot search on field [" + name() + "] since it is not indexed.");
        }
    }

    protected final void failIfNotIndexedNorDocValuesFallback(SearchExecutionContext context) {
        if (docValues == false && context.indexVersionCreated().isLegacyIndexVersion()) {
            throw new IllegalArgumentException(
                "Cannot search on field [" + name() + "] of legacy index since it does not have doc values."
            );
        } else if (isIndexed == false && docValues == false) {
            // we throw an IAE rather than an ISE so that it translates to a 4xx code rather than 5xx code on the http layer
            throw new IllegalArgumentException("Cannot search on field [" + name() + "] since it is not indexed nor has doc values.");
        } else if (isIndexed == false && docValues && context.allowExpensiveQueries() == false) {
            // if query can only run using doc values, ensure running expensive queries are allowed
            throw new ElasticsearchException(
                "Cannot search on field ["
                    + name()
                    + "] since it is not indexed and '"
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "' is set to false."
            );
        }
    }

    /**
     * @return if this field type should load global ordinals eagerly
     */
    public boolean eagerGlobalOrdinals() {
        return false;
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
        return true;
    }

    /**
     * Pick a {@link DocValueFormat} that can be used to display and parse
     * values of fields of this type.
     */
    public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
        checkNoFormat(format);
        checkNoTimeZone(timeZone);
        return DocValueFormat.RAW;
    }

    /**
     * Validate the provided {@code format} is null.
     */
    protected void checkNoFormat(@Nullable String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
        }
    }

    /**
     * Validate the provided {@code timeZone} is null.
     */
    protected void checkNoTimeZone(@Nullable ZoneId timeZone) {
        if (timeZone != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones");
        }
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
     * @param reader an index reader
     * @param prefix the partially complete word the user has typed (can be empty)
     * @param caseInsensitive if prefix matches should be case insensitive
     * @param searchAfter - usually null. If supplied the TermsEnum result must be positioned after the provided term (used for pagination)
     * @return null or an enumeration of matching terms
     * @throws IOException Errors accessing data
     */
    public TermsEnum getTerms(IndexReader reader, String prefix, boolean caseInsensitive, String searchAfter) throws IOException {
        return null;
    }

    /**
     * Validate that this field can be the target of {@link IndexMetadata#INDEX_ROUTING_PATH}.
     */
    public void validateMatchedRoutingPath(String routingPath) {
        if (hasScriptValues()) {
            throw new IllegalArgumentException(
                "All fields that match routing_path must be configured with [time_series_dimension: true] "
                    + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                    + "without the [script] parameter. ["
                    + name()
                    + "] has a [script] parameter."
            );
        }

        if (isDimension() == false) {
            throw new IllegalArgumentException(
                "All fields that match routing_path "
                    + "must be configured with [time_series_dimension: true] "
                    + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                    + "without the [script] parameter. ["
                    + name()
                    + "] was not a dimension."
            );
        }
    }

    /**
     * This method is used to support _field_caps when include_empty_fields is set to
     * {@code false}. In that case we return only fields with value in an index. This method
     * gets as input FieldInfos and returns if the field is non-empty. This method needs to
     * be overwritten where fields don't have footprint in Lucene or their name differs from
     * {@link MappedFieldType#name()}
     * @param fieldInfos field information
     * @return {@code true} if field is present in fieldInfos {@code false} otherwise
     */
    public boolean fieldHasValue(FieldInfos fieldInfos) {
        return fieldInfos.fieldInfo(name()) != null;
    }

    /**
     * Returns a loader for ESQL or {@code null} if the field doesn't support
     * ESQL.
     */
    public BlockLoader blockLoader(BlockLoaderContext blContext) {
        return null;
    }

    public enum FieldExtractPreference {
        /**
         * Load the field from doc-values into a BlockLoader supporting doc-values.
         */
        DOC_VALUES,
        /**
         *  Loads the field by extracting the extent from the binary encoded representation
         */
        EXTRACT_SPATIAL_BOUNDS,
        /**
         * No preference. Leave the choice of where to load the field from up to the FieldType.
         */
        NONE,
        /**
         * Prefer loading from stored fields like {@code _source} because we're
         * loading many fields. The {@link MappedFieldType} can chose a different
         * method to load the field if it needs to.
         */
        STORED;
    }

    /**
     * Arguments for {@link #blockLoader}.
     */
    public interface BlockLoaderContext {
        /**
         * The name of the index.
         */
        String indexName();

        /**
         * The index settings of the index
         */
        IndexSettings indexSettings();

        /**
         * How the field should be extracted into the BlockLoader. The default is {@link FieldExtractPreference#NONE}, which means
         * that the field type can choose where to load the field from. However, in some cases, the caller may have a preference.
         * For example, when loading a spatial field for usage in STATS, it is preferable to load from doc-values.
         */
        FieldExtractPreference fieldExtractPreference();

        /**
         * {@link SearchLookup} used for building scripts.
         */
        SearchLookup lookup();

        /**
         * Find the paths in {@code _source} that contain values for the field named {@code name}.
         */
        Set<String> sourcePaths(String name);

        /**
         * If field is a leaf multi-field return the path to the parent field. Otherwise, return null.
         */
        String parentField(String field);

        /**
         * The {@code _field_names} field mapper, mostly used to check if it is enabled.
         */
        FieldNamesFieldMapper.FieldNamesFieldType fieldNames();
    }

}
