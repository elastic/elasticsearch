/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.elasticsearch.lucene.similarity.LegacyBM25Similarity;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A query that matches on multiple text fields, as if the field contents had been indexed
 * into a single combined field.
 */
public final class CombinedFieldsQueryBuilder extends AbstractQueryBuilder<CombinedFieldsQueryBuilder> {
    public static final String NAME = "combined_fields";

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField OPERATOR_FIELD = new ParseField("operator");
    private static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    private static final ParseField GENERATE_SYNONYMS_PHRASE_QUERY = new ParseField("auto_generate_synonyms_phrase_query");
    private static final ParseField ZERO_TERMS_QUERY_FIELD = new ParseField("zero_terms_query");

    private static final Operator DEFAULT_OPERATOR = Operator.OR;
    private static final ZeroTermsQueryOption DEFAULT_ZERO_TERMS_QUERY = ZeroTermsQueryOption.NONE;
    private static final boolean DEFAULT_GENERATE_SYNONYMS_PHRASE = true;

    private final Object value;
    private final Map<String, Float> fieldsAndBoosts;
    private Operator operator = DEFAULT_OPERATOR;
    private String minimumShouldMatch;
    private ZeroTermsQueryOption zeroTermsQuery = DEFAULT_ZERO_TERMS_QUERY;
    private boolean autoGenerateSynonymsPhraseQuery = DEFAULT_GENERATE_SYNONYMS_PHRASE;

    private static final ConstructingObjectParser<CombinedFieldsQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new CombinedFieldsQueryBuilder(a[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY_FIELD);
        PARSER.declareStringArray((builder, values) -> {
            Map<String, Float> fieldsAndBoosts = QueryParserHelper.parseFieldsAndWeights(values);
            builder.fields(fieldsAndBoosts);
        }, FIELDS_FIELD);

        PARSER.declareString(CombinedFieldsQueryBuilder::operator, Operator::fromString, OPERATOR_FIELD);
        PARSER.declareField(
            CombinedFieldsQueryBuilder::minimumShouldMatch,
            XContentParser::textOrNull,
            MINIMUM_SHOULD_MATCH_FIELD,
            // using INT_OR_NULL (which includes VALUE_NUMBER, VALUE_STRING, VALUE_NULL) to also allow for numeric values and null
            ValueType.INT_OR_NULL
        );
        PARSER.declareBoolean(CombinedFieldsQueryBuilder::autoGenerateSynonymsPhraseQuery, GENERATE_SYNONYMS_PHRASE_QUERY);
        PARSER.declareString(CombinedFieldsQueryBuilder::zeroTermsQuery, value -> {
            if ("none".equalsIgnoreCase(value)) {
                return ZeroTermsQueryOption.NONE;
            } else if ("all".equalsIgnoreCase(value)) {
                return ZeroTermsQueryOption.ALL;
            } else {
                throw new IllegalArgumentException("Unsupported [" + ZERO_TERMS_QUERY_FIELD.getPreferredName() + "] value [" + value + "]");
            }
        }, ZERO_TERMS_QUERY_FIELD);

        PARSER.declareFloat(CombinedFieldsQueryBuilder::boost, BOOST_FIELD);
        PARSER.declareString(CombinedFieldsQueryBuilder::queryName, NAME_FIELD);
    }

    /**
     * Constructs a new text query.
     */
    public CombinedFieldsQueryBuilder(Object value, String... fields) {
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        if (fields == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires field list");
        }
        this.value = value;
        this.fieldsAndBoosts = new TreeMap<>();
        for (String field : fields) {
            field(field);
        }
    }

    /**
     * Read from a stream.
     */
    public CombinedFieldsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        value = in.readGenericValue();
        int size = in.readVInt();
        fieldsAndBoosts = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            float boost = in.readFloat();
            fieldsAndBoosts.put(field, boost);
        }
        operator = Operator.readFromStream(in);
        minimumShouldMatch = in.readOptionalString();
        zeroTermsQuery = ZeroTermsQueryOption.readFromStream(in);
        autoGenerateSynonymsPhraseQuery = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeGenericValue(value);
        out.writeVInt(fieldsAndBoosts.size());
        for (Map.Entry<String, Float> fieldsEntry : fieldsAndBoosts.entrySet()) {
            out.writeString(fieldsEntry.getKey());
            out.writeFloat(fieldsEntry.getValue());
        }
        operator.writeTo(out);
        out.writeOptionalString(minimumShouldMatch);
        zeroTermsQuery.writeTo(out);
        out.writeBoolean(autoGenerateSynonymsPhraseQuery);
    }

    public Object value() {
        return value;
    }

    /**
     * Adds a field to run the query against.
     */
    public CombinedFieldsQueryBuilder field(String field) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty.");
        }
        this.fieldsAndBoosts.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
        return this;
    }

    /**
     * Adds a field to run the query against with a specific boost.
     */
    public CombinedFieldsQueryBuilder field(String field, float boost) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty.");
        }
        validateFieldBoost(boost);
        this.fieldsAndBoosts.put(field, boost);
        return this;
    }

    /**
     * Add several fields to run the query against with a specific boost.
     */
    public CombinedFieldsQueryBuilder fields(Map<String, Float> fields) {
        for (float fieldBoost : fields.values()) {
            validateFieldBoost(fieldBoost);
        }
        this.fieldsAndBoosts.putAll(fields);
        return this;
    }

    public Map<String, Float> fields() {
        return fieldsAndBoosts;
    }

    /**
     * Sets the operator to use for the top-level boolean query. Defaults to {@code OR}.
     */
    public CombinedFieldsQueryBuilder operator(Operator operator) {
        if (operator == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires operator to be non-null");
        }
        this.operator = operator;
        return this;
    }

    public Operator operator() {
        return operator;
    }

    public CombinedFieldsQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    public String minimumShouldMatch() {
        return minimumShouldMatch;
    }

    public CombinedFieldsQueryBuilder zeroTermsQuery(ZeroTermsQueryOption zeroTermsQuery) {
        if (zeroTermsQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires zero terms query to be non-null");
        }
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }

    public ZeroTermsQueryOption zeroTermsQuery() {
        return zeroTermsQuery;
    }

    public CombinedFieldsQueryBuilder autoGenerateSynonymsPhraseQuery(boolean enable) {
        this.autoGenerateSynonymsPhraseQuery = enable;
        return this;
    }

    /**
     * Whether phrase queries should be automatically generated for multi terms synonyms.
     * Defaults to {@code true}.
     */
    public boolean autoGenerateSynonymsPhraseQuery() {
        return autoGenerateSynonymsPhraseQuery;
    }

    private static void validateFieldBoost(float boost) {
        if (boost < 1.0f) {
            throw new IllegalArgumentException("[" + NAME + "] requires field boosts to be >= 1.0");
        }
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName(), value);
        builder.startArray(FIELDS_FIELD.getPreferredName());
        for (Map.Entry<String, Float> fieldEntry : this.fieldsAndBoosts.entrySet()) {
            builder.value(fieldEntry.getKey() + "^" + fieldEntry.getValue());
        }
        builder.endArray();
        if (operator != DEFAULT_OPERATOR) {
            builder.field(OPERATOR_FIELD.getPreferredName(), operator.toString());
        }
        if (minimumShouldMatch != null) {
            builder.field(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatch);
        }
        if (zeroTermsQuery != DEFAULT_ZERO_TERMS_QUERY) {
            builder.field(ZERO_TERMS_QUERY_FIELD.getPreferredName(), zeroTermsQuery.toString());
        }
        if (autoGenerateSynonymsPhraseQuery != DEFAULT_GENERATE_SYNONYMS_PHRASE) {
            builder.field(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), autoGenerateSynonymsPhraseQuery);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    public static CombinedFieldsQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (fieldsAndBoosts.isEmpty()) {
            throw new IllegalArgumentException("In [" + NAME + "] query, at least one field must be provided");
        }

        Map<String, Float> fields = QueryParserHelper.resolveMappingFields(context, fieldsAndBoosts);
        // If all fields are unmapped, then return an 'unmapped field query'.
        boolean hasMappedField = fields.keySet().stream().anyMatch(k -> context.getFieldType(k) != null);
        if (hasMappedField == false) {
            return Queries.newUnmappedFieldsQuery(fields.keySet());
        }

        validateSimilarity(context, fields);

        Analyzer sharedAnalyzer = null;
        List<FieldAndBoost> fieldsAndBoosts = new ArrayList<>();
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            String name = entry.getKey();
            MappedFieldType fieldType = context.getFieldType(name);
            if (fieldType == null) {
                continue;
            }

            if (fieldType.familyTypeName().equals(TextFieldMapper.CONTENT_TYPE) == false) {
                throw new IllegalArgumentException(
                    "Field [" + fieldType.name() + "] of type [" + fieldType.typeName() + "] does not support [" + NAME + "] queries"
                );
            }

            float boost = entry.getValue() == null ? 1.0f : entry.getValue();
            fieldsAndBoosts.add(new FieldAndBoost(fieldType, boost));

            Analyzer analyzer = fieldType.getTextSearchInfo().searchAnalyzer();
            if (sharedAnalyzer != null && analyzer.equals(sharedAnalyzer) == false) {
                throw new IllegalArgumentException("All fields in [" + NAME + "] query must have the same search analyzer");
            }
            sharedAnalyzer = analyzer;
        }

        assert fieldsAndBoosts.isEmpty() == false;
        String placeholderFieldName = fieldsAndBoosts.get(0).fieldType.name();
        boolean canGenerateSynonymsPhraseQuery = autoGenerateSynonymsPhraseQuery;
        for (FieldAndBoost fieldAndBoost : fieldsAndBoosts) {
            TextSearchInfo textSearchInfo = fieldAndBoost.fieldType.getTextSearchInfo();
            canGenerateSynonymsPhraseQuery &= textSearchInfo.hasPositions();
        }

        CombinedFieldsBuilder builder = new CombinedFieldsBuilder(fieldsAndBoosts, sharedAnalyzer, canGenerateSynonymsPhraseQuery, context);
        Query query = builder.createBooleanQuery(placeholderFieldName, value.toString(), operator.toBooleanClauseOccur());

        query = Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
        if (query == null) {
            query = zeroTermsQuery.asQuery();
        }
        return query;
    }

    private static void validateSimilarity(SearchExecutionContext context, Map<String, Float> fields) {
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            String name = entry.getKey();
            MappedFieldType fieldType = context.getFieldType(name);
            if (fieldType != null && fieldType.getTextSearchInfo().similarity() != null) {
                throw new IllegalArgumentException("[" + NAME + "] queries cannot be used with per-field similarities");
            }
        }

        Similarity defaultSimilarity = context.getDefaultSimilarity();
        if ((defaultSimilarity instanceof LegacyBM25Similarity || defaultSimilarity instanceof BM25Similarity) == false) {
            throw new IllegalArgumentException("[" + NAME + "] queries can only be used with the [BM25] similarity");
        }
    }

    private static final class FieldAndBoost {
        final MappedFieldType fieldType;
        final float boost;

        FieldAndBoost(MappedFieldType fieldType, float boost) {
            this.fieldType = Objects.requireNonNull(fieldType);
            this.boost = boost;
        }
    }

    private static class CombinedFieldsBuilder extends QueryBuilder {
        private final List<FieldAndBoost> fields;
        private final SearchExecutionContext context;

        CombinedFieldsBuilder(
            List<FieldAndBoost> fields,
            Analyzer analyzer,
            boolean autoGenerateSynonymsPhraseQuery,
            SearchExecutionContext context
        ) {
            super(analyzer);
            this.fields = fields;
            setAutoGenerateMultiTermSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
            this.context = context;
        }

        @Override
        protected Query createFieldQuery(TokenStream source, BooleanClause.Occur operator, String field, boolean quoted, int phraseSlop) {
            if (source.hasAttribute(DisableGraphAttribute.class)) {
                /*
                 * A {@link TokenFilter} in this {@link TokenStream} disabled the graph analysis to avoid
                 * paths explosion. See {@link org.elasticsearch.index.analysis.ShingleTokenFilterFactory} for details.
                 */
                setEnableGraphQueries(false);
            }
            try {
                return super.createFieldQuery(source, operator, field, quoted, phraseSlop);
            } finally {
                setEnableGraphQueries(true);
            }
        }

        @Override
        public Query createPhraseQuery(String field, String queryText, int phraseSlop) {
            throw new IllegalArgumentException("[combined_fields] queries don't support phrases");
        }

        @Override
        protected Query newSynonymQuery(String field, TermAndBoost[] terms) {
            CombinedFieldQuery.Builder query = new CombinedFieldQuery.Builder();
            for (TermAndBoost termAndBoost : terms) {
                assert termAndBoost.boost() == BoostAttribute.DEFAULT_BOOST;
                BytesRef bytes = termAndBoost.term();
                query.addTerm(bytes);
            }
            for (FieldAndBoost fieldAndBoost : fields) {
                MappedFieldType fieldType = fieldAndBoost.fieldType;
                float fieldBoost = fieldAndBoost.boost;
                query.addField(fieldType.name(), fieldBoost);
            }
            return query.build();
        }

        @Override
        protected Query newTermQuery(Term term, float boost) {
            TermAndBoost termAndBoost = new TermAndBoost(term.bytes(), boost);
            return newSynonymQuery(term.field(), new TermAndBoost[] { termAndBoost });
        }

        @Override
        protected Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (FieldAndBoost fieldAndBoost : fields) {
                Query query = fieldAndBoost.fieldType.phraseQuery(stream, slop, enablePositionIncrements, context);
                if (fieldAndBoost.boost != 1f) {
                    query = new BoostQuery(query, fieldAndBoost.boost);
                }
                builder.add(query, BooleanClause.Occur.SHOULD);
            }
            return builder.build();
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(value, fieldsAndBoosts, operator, minimumShouldMatch, zeroTermsQuery, autoGenerateSynonymsPhraseQuery);
    }

    @Override
    protected boolean doEquals(CombinedFieldsQueryBuilder other) {
        return Objects.equals(value, other.value)
            && Objects.equals(fieldsAndBoosts, other.fieldsAndBoosts)
            && Objects.equals(operator, other.operator)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
            && Objects.equals(zeroTermsQuery, other.zeroTermsQuery)
            && Objects.equals(autoGenerateSynonymsPhraseQuery, other.autoGenerateSynonymsPhraseQuery);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_13_0;
    }
}
