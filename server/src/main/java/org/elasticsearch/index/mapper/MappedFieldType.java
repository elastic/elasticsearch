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

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This defines the core properties and functions to operate on a field.
 */
public abstract class MappedFieldType extends FieldType {

    private String name;
    private float boost;
    // TODO: remove this docvalues flag and use docValuesType
    private boolean docValues;
    private NamedAnalyzer indexAnalyzer;
    private NamedAnalyzer searchAnalyzer;
    private NamedAnalyzer searchQuoteAnalyzer;
    private SimilarityProvider similarity;
    private Object nullValue;
    private String nullValueAsString; // for sending null value to _all field
    private boolean eagerGlobalOrdinals;
    private Map<String, String> meta;

    protected MappedFieldType(MappedFieldType ref) {
        super(ref);
        this.name = ref.name();
        this.boost = ref.boost();
        this.docValues = ref.hasDocValues();
        this.indexAnalyzer = ref.indexAnalyzer();
        this.searchAnalyzer = ref.searchAnalyzer();
        this.searchQuoteAnalyzer = ref.searchQuoteAnalyzer;
        this.similarity = ref.similarity();
        this.nullValue = ref.nullValue();
        this.nullValueAsString = ref.nullValueAsString();
        this.eagerGlobalOrdinals = ref.eagerGlobalOrdinals;
        this.meta = ref.meta;
    }

    public MappedFieldType() {
        setTokenized(true);
        setStored(false);
        setStoreTermVectors(false);
        setOmitNorms(false);
        setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        setBoost(1.0f);
        meta = Collections.emptyMap();
    }

    @Override
    public abstract MappedFieldType clone();

    /**
     * Return a fielddata builder for this field
     *
     * @param fullyQualifiedIndexName the name of the index this field-data is build for
     *
     * @throws IllegalArgumentException if the fielddata is not supported on this type.
     * An IllegalArgumentException is needed in order to return an http error 400
     * when this error occurs in a request. see: {@link org.elasticsearch.ExceptionsHelper#status}
     */
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
        throw new IllegalArgumentException("Fielddata is not supported on field [" + name() + "] of type [" + typeName() + "]");
    }

    /**
     * Returns the {@link ValuesSourceType} which supports this field type.  This is tightly coupled to field data and aggregations support,
     * so any implementation that returns a value from {@link MappedFieldType#fielddataBuilder} should also return a value from  here.
     *
     * @return The appropriate {@link ValuesSourceType} for this field type.
     */
    public ValuesSourceType getValuesSourceType() {
        throw new IllegalArgumentException("Aggregations are not supported on field [" + name() + "] of type [" + typeName() + "]");
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;
        MappedFieldType fieldType = (MappedFieldType) o;

        return boost == fieldType.boost &&
            docValues == fieldType.docValues &&
            Objects.equals(name, fieldType.name) &&
            Objects.equals(indexAnalyzer, fieldType.indexAnalyzer) &&
            Objects.equals(searchAnalyzer, fieldType.searchAnalyzer) &&
            Objects.equals(searchQuoteAnalyzer(), fieldType.searchQuoteAnalyzer()) &&
            Objects.equals(eagerGlobalOrdinals, fieldType.eagerGlobalOrdinals) &&
            Objects.equals(nullValue, fieldType.nullValue) &&
            Objects.equals(nullValueAsString, fieldType.nullValueAsString) &&
            Objects.equals(similarity, fieldType.similarity) &&
            Objects.equals(meta, fieldType.meta);
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(super.hashCode(), name, boost, docValues, indexAnalyzer, searchAnalyzer, searchQuoteAnalyzer,
            eagerGlobalOrdinals, similarity == null ? null : similarity.name(), nullValue, nullValueAsString, meta);
        return hash;
    }

    // TODO: we need to override freeze() and add safety checks that all settings are actually set

    /** Returns the name of this type, as would be specified in mapping properties */
    public abstract String typeName();

    public String name() {
        return name;
    }

    public void setName(String name) {
        checkIfFrozen();
        this.name = name;
    }

    public float boost() {
        return boost;
    }

    public void setBoost(float boost) {
        checkIfFrozen();
        this.boost = boost;
    }

    public boolean hasDocValues() {
        return docValues;
    }

    public void setHasDocValues(boolean hasDocValues) {
        checkIfFrozen();
        this.docValues = hasDocValues;
    }

    public NamedAnalyzer indexAnalyzer() {
        return indexAnalyzer;
    }

    public void setIndexAnalyzer(NamedAnalyzer analyzer) {
        checkIfFrozen();
        this.indexAnalyzer = analyzer;
    }

    public NamedAnalyzer searchAnalyzer() {
        return searchAnalyzer;
    }

    public void setSearchAnalyzer(NamedAnalyzer analyzer) {
        checkIfFrozen();
        this.searchAnalyzer = analyzer;
    }

    public NamedAnalyzer searchQuoteAnalyzer() {
        return searchQuoteAnalyzer == null ? searchAnalyzer : searchQuoteAnalyzer;
    }

    public void setSearchQuoteAnalyzer(NamedAnalyzer analyzer) {
        checkIfFrozen();
        this.searchQuoteAnalyzer = analyzer;
    }

    public SimilarityProvider similarity() {
        return similarity;
    }

    public void setSimilarity(SimilarityProvider similarity) {
        checkIfFrozen();
        this.similarity = similarity;
    }

    /** Returns the value that should be added when JSON null is found, or null if no value should be added */
    public Object nullValue() {
        return nullValue;
    }

    /** Returns the null value stringified or null if there is no null value */
    public String nullValueAsString() {
        return nullValueAsString;
    }

    /** Sets the null value and initializes the string version */
    public void setNullValue(Object nullValue) {
        checkIfFrozen();
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
    }

    /** Given a value that comes from the stored fields API, convert it to the
     *  expected type. For instance a date field would store dates as longs and
     *  format it back to a string in this method. */
    public Object valueForDisplay(Object value) {
        return value;
    }

    /** Returns true if the field is searchable.
     *
     */
    public boolean isSearchable() {
        return indexOptions() != IndexOptions.NONE;
    }

    /** Returns true if the field is aggregatable.
     *
     */
    public boolean isAggregatable() {
        try {
            fielddataBuilder("");
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** Generates a query that will only match documents that contain the given value.
     *  The default implementation returns a {@link TermQuery} over the value bytes,
     *  boosted by {@link #boost()}.
     *  @throws IllegalArgumentException if {@code value} cannot be converted to the expected data type or if the field is not searchable
     *      due to the way it is configured (eg. not indexed)
     *  @throws ElasticsearchParseException if {@code value} cannot be converted to the expected data type
     *  @throws UnsupportedOperationException if the field is not searchable regardless of options
     *  @throws QueryShardException if the field is not searchable regardless of options
     */
    // TODO: Standardize exception types
    public abstract Query termQuery(Object value, @Nullable QueryShardContext context);

    /** Build a constant-scoring query that matches all values. The default implementation uses a
     * {@link ConstantScoreQuery} around a {@link BooleanQuery} whose {@link Occur#SHOULD} clauses
     * are generated with {@link #termQuery}. */
    public Query termsQuery(List<?> values, @Nullable QueryShardContext context) {
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
        QueryShardContext context) {
        throw new IllegalArgumentException("Field [" + name + "] of type [" + typeName() + "] does not support range queries");
    }

    public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions,
                            QueryShardContext context) {
        throw new IllegalArgumentException("Can only use fuzzy queries on keyword and text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        throw new QueryShardException(context, "Can only use prefix queries on keyword, text and wildcard fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query wildcardQuery(String value,
                               @Nullable MultiTermQuery.RewriteMethod method,
                               QueryShardContext context) {
        throw new QueryShardException(context, "Can only use wildcard queries on keyword, text and wildcard fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query regexpQuery(String value, int flags, int maxDeterminizedStates, @Nullable MultiTermQuery.RewriteMethod method,
                             QueryShardContext context) {
        throw new QueryShardException(context, "Can only use regexp queries on keyword and text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public abstract Query existsQuery(QueryShardContext context);

    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        throw new IllegalArgumentException("Can only use phrase queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        throw new IllegalArgumentException("Can only use phrase queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
        throw new IllegalArgumentException("Can only use phrase prefix queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
        throw new IllegalArgumentException("Can only use span prefix queries on text fields - not on [" + name
            + "] which is of type [" + typeName() + "]");
    }

    /**
     * Create an {@link IntervalsSource} to be used for proximity queries
     */
    public IntervalsSource intervals(String query, int max_gaps, boolean ordered,
                                     NamedAnalyzer analyzer, boolean prefix) throws IOException {
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
        DISJOINT;
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
        if (indexOptions() == IndexOptions.NONE && pointDimensionCount() == 0) {
            // we throw an IAE rather than an ISE so that it translates to a 4xx code rather than 5xx code on the http layer
            throw new IllegalArgumentException("Cannot search on field [" + name() + "] since it is not indexed.");
        }
    }

    public boolean eagerGlobalOrdinals() {
        return eagerGlobalOrdinals;
    }

    public void setEagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
        checkIfFrozen();
        this.eagerGlobalOrdinals = eagerGlobalOrdinals;
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
        if (termQuery instanceof TypeFieldMapper.TypesQuery) {
            assert ((TypeFieldMapper.TypesQuery) termQuery).getTerms().length == 1;
            return new Term(TypeFieldMapper.NAME, ((TypeFieldMapper.TypesQuery) termQuery).getTerms()[0]);
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
     * Associate metadata with this field.
     */
    public void setMeta(Map<String, String> meta) {
        checkIfFrozen();
        this.meta = Map.copyOf(Objects.requireNonNull(meta));
    }
}
