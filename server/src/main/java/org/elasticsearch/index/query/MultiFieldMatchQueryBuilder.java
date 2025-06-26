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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.lucene.similarity.LegacyBM25Similarity;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A query builder similar to MultiMatchQueryBuilder, but not exposed for external use, and translating to a combined query.
 */
public class MultiFieldMatchQueryBuilder extends AbstractQueryBuilder<MultiFieldMatchQueryBuilder> {

    public static final String NAME = "multi_field_match";

    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField OPERATOR_FIELD = new ParseField("operator");
    public static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    public static final ParseField GENERATE_SYNONYMS_PHRASE_QUERY = new ParseField("auto_generate_synonyms_phrase_query");
    public static final ParseField ZERO_TERMS_QUERY_FIELD = new ParseField("zero_terms_query");

    private static final Operator DEFAULT_OPERATOR = Operator.AND;
    private static final ZeroTermsQueryOption DEFAULT_ZERO_TERMS_QUERY = ZeroTermsQueryOption.NONE;
    private static final boolean DEFAULT_GENERATE_SYNONYMS_PHRASE = true;

    private final Object value;
    private final Map<String, Float> fieldsAndBoosts;
    private Operator operator = DEFAULT_OPERATOR;
    private String minimumShouldMatch;
    private ZeroTermsQueryOption zeroTermsQuery = DEFAULT_ZERO_TERMS_QUERY;
    private boolean autoGenerateSynonymsPhraseQuery = DEFAULT_GENERATE_SYNONYMS_PHRASE;

    private static final ConstructingObjectParser<MultiFieldMatchQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new MultiFieldMatchQueryBuilder(a[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY_FIELD);
        PARSER.declareStringArray((builder, values) -> {
            Map<String, Float> fieldsAndBoosts = QueryParserHelper.parseFieldsAndWeights(values);
            builder.fields(fieldsAndBoosts);
        }, FIELDS_FIELD);

        PARSER.declareString(MultiFieldMatchQueryBuilder::operator, Operator::fromString, OPERATOR_FIELD);
        PARSER.declareField(
            MultiFieldMatchQueryBuilder::minimumShouldMatch,
            XContentParser::textOrNull,
            MINIMUM_SHOULD_MATCH_FIELD,
            // using INT_OR_NULL (which includes VALUE_NUMBER, VALUE_STRING, VALUE_NULL) to also allow for numeric values and null
            ObjectParser.ValueType.INT_OR_NULL
        );
        PARSER.declareBoolean(MultiFieldMatchQueryBuilder::autoGenerateSynonymsPhraseQuery, GENERATE_SYNONYMS_PHRASE_QUERY);
        PARSER.declareString(MultiFieldMatchQueryBuilder::zeroTermsQuery, value -> {
            if ("none".equalsIgnoreCase(value)) {
                return ZeroTermsQueryOption.NONE;
            } else if ("all".equalsIgnoreCase(value)) {
                return ZeroTermsQueryOption.ALL;
            } else {
                throw new IllegalArgumentException("Unsupported [" + ZERO_TERMS_QUERY_FIELD.getPreferredName() + "] value [" + value + "]");
            }
        }, ZERO_TERMS_QUERY_FIELD);

        PARSER.declareFloat(MultiFieldMatchQueryBuilder::boost, BOOST_FIELD);
        PARSER.declareString(MultiFieldMatchQueryBuilder::queryName, NAME_FIELD);
    }

    /**
     * Constructs a new text query.
     */
    private MultiFieldMatchQueryBuilder(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.value = value;
        this.fieldsAndBoosts = new TreeMap<>();
    }

    public static MultiFieldMatchQueryBuilder create(Object value, String... fields) {
        if (fields == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires field list");
        }
        var result = new MultiFieldMatchQueryBuilder(value);
        for (String field : fields) {
            result = result.field(field);
        }
        return result;
    }

    /**
     * Read from a stream.
     */
    public MultiFieldMatchQueryBuilder(StreamInput in) throws IOException {
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

    /**
     * Adds a field to run the query against.
     */
    public MultiFieldMatchQueryBuilder field(String field) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty.");
        }
        this.fieldsAndBoosts.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
        return this;
    }

    /**
     * Adds a field to run the query against with a specific boost.
     */
    public MultiFieldMatchQueryBuilder field(String field, float boost) {
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
    public MultiFieldMatchQueryBuilder fields(Map<String, Float> fields) {
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
    public MultiFieldMatchQueryBuilder operator(Operator operator) {
        if (operator == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires operator to be non-null");
        }
        this.operator = operator;
        return this;
    }

    public Operator operator() {
        return operator;
    }

    public MultiFieldMatchQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    public String minimumShouldMatch() {
        return minimumShouldMatch;
    }

    public MultiFieldMatchQueryBuilder zeroTermsQuery(ZeroTermsQueryOption zeroTermsQuery) {
        if (zeroTermsQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires zero terms query to be non-null");
        }
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }

    public MultiFieldMatchQueryBuilder autoGenerateSynonymsPhraseQuery(boolean enable) {
        this.autoGenerateSynonymsPhraseQuery = enable;
        return this;
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

    public static MultiFieldMatchQueryBuilder fromXContent(XContentParser parser) throws IOException {
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

        Map<Analyzer, List<String>> groups = new HashMap<>();
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

            // TODO: handle per-field boosts.

            Analyzer analyzer = fieldType.getTextSearchInfo().searchAnalyzer();
            if (groups.containsKey(analyzer) == false) {
                groups.put(analyzer, new ArrayList<>());
            }
            groups.get(analyzer).add(name);
        }

        // TODO: For now assume we have one group.
        assert groups.size() == 1;

        var disMax = new DisMaxQueryBuilder();
        for (Map.Entry<Analyzer, List<String>> group : groups.entrySet()) {
            /*
            TODO
            String placeholderFieldName = fieldsAndBoosts.get(0).fieldType.name();
            boolean canGenerateSynonymsPhraseQuery = autoGenerateSynonymsPhraseQuery;
            for (FieldAndBoost fieldAndBoost : fieldsAndBoosts) {
                TextSearchInfo textSearchInfo = fieldAndBoost.fieldType.getTextSearchInfo();
                canGenerateSynonymsPhraseQuery &= textSearchInfo.hasPositions();
            }
            */

            var combinedFields = new CombinedFieldsQueryBuilder(value, group.getValue().toArray(new String[0]));
            combinedFields = combinedFields.operator(operator);
            disMax.add(combinedFields);
        }

        /*
        TODO
        Query query = disMax.createBooleanQuery(placeholderFieldName, value.toString(), operator.toBooleanClauseOccur());
        query = Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
        if (query == null) {
            query = zeroTermsQuery.asQuery();
        }

         */
        return disMax.doToQuery(context);
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

    @Override
    protected int doHashCode() {
        return Objects.hash(value, fieldsAndBoosts, operator, minimumShouldMatch, zeroTermsQuery, autoGenerateSynonymsPhraseQuery);
    }

    @Override
    protected boolean doEquals(MultiFieldMatchQueryBuilder other) {
        return Objects.equals(value, other.value)
            && Objects.equals(fieldsAndBoosts, other.fieldsAndBoosts)
            && Objects.equals(operator, other.operator)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
            && Objects.equals(zeroTermsQuery, other.zeroTermsQuery)
            && Objects.equals(autoGenerateSynonymsPhraseQuery, other.autoGenerateSynonymsPhraseQuery);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
