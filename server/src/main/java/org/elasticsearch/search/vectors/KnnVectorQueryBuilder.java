/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query that performs kNN search using Lucene's {@link org.apache.lucene.search.KnnFloatVectorQuery} or
 * {@link org.apache.lucene.search.KnnByteVectorQuery}.
 *
 */
public class KnnVectorQueryBuilder extends AbstractQueryBuilder<KnnVectorQueryBuilder> {
    public static final String NAME = "knn";
    private static final int NUM_CANDS_LIMIT = 10_000;
    private static final float NUM_CANDS_MULTIPLICATIVE_FACTOR = 1.5f;

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField VECTOR_SIMILARITY_FIELD = new ParseField("similarity");
    public static final ParseField FILTER_FIELD = new ParseField("filter");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<KnnVectorQueryBuilder, Void> PARSER = new ConstructingObjectParser<>("knn", args -> {
        List<Float> vector = (List<Float>) args[1];
        final float[] vectorArray;
        if (vector != null) {
            vectorArray = new float[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                vectorArray[i] = vector.get(i);
            }
        } else {
            vectorArray = null;
        }
        return new KnnVectorQueryBuilder((String) args[0], vectorArray, (Integer) args[2], (Float) args[3]);
    });

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareFloatArray(constructorArg(), QUERY_VECTOR_FIELD);
        PARSER.declareInt(optionalConstructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), VECTOR_SIMILARITY_FIELD);
        PARSER.declareFieldArray(
            KnnVectorQueryBuilder::addFilterQueries,
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            FILTER_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        declareStandardFields(PARSER);
    }

    public static KnnVectorQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String fieldName;
    private final float[] queryVector;
    private Integer numCands;
    private final List<QueryBuilder> filterQueries = new ArrayList<>();
    private final Float vectorSimilarity;

    public KnnVectorQueryBuilder(String fieldName, float[] queryVector, Integer numCands, Float vectorSimilarity) {
        if (numCands != null && numCands > NUM_CANDS_LIMIT) {
            throw new IllegalArgumentException("[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot exceed [" + NUM_CANDS_LIMIT + "]");
        }
        if (queryVector == null) {
            throw new IllegalArgumentException("[" + QUERY_VECTOR_FIELD.getPreferredName() + "] must be provided");
        }
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.numCands = numCands;
        this.vectorSimilarity = vectorSimilarity;
    }

    public KnnVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.KNN_QUERY_NUMCANDS_AS_OPTIONAL_PARAM)) {
            this.numCands = in.readOptionalVInt();
        } else {
            this.numCands = in.readVInt();
        }
        if (in.getTransportVersion().before(TransportVersions.V_8_7_0) || in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            this.queryVector = in.readFloatArray();
        } else {
            in.readBoolean();
            this.queryVector = in.readFloatArray();
            in.readBoolean(); // used for byteQueryVector, which was always null
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            this.filterQueries.addAll(readQueries(in));
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            this.vectorSimilarity = in.readOptionalFloat();
        } else {
            this.vectorSimilarity = null;
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    public float[] queryVector() {
        return queryVector;
    }

    @Nullable
    public Float getVectorSimilarity() {
        return vectorSimilarity;
    }

    public Integer numCands() {
        return numCands;
    }

    public List<QueryBuilder> filterQueries() {
        return filterQueries;
    }

    public KnnVectorQueryBuilder addFilterQuery(QueryBuilder filterQuery) {
        Objects.requireNonNull(filterQuery);
        this.filterQueries.add(filterQuery);
        return this;
    }

    public KnnVectorQueryBuilder addFilterQueries(List<QueryBuilder> filterQueries) {
        Objects.requireNonNull(filterQueries);
        this.filterQueries.addAll(filterQueries);
        return this;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        if (out.getTransportVersion().onOrAfter(TransportVersions.KNN_QUERY_NUMCANDS_AS_OPTIONAL_PARAM)) {
            out.writeOptionalVInt(numCands);
        } else {
            if (numCands == null) {
                throw new IllegalArgumentException(
                    "["
                        + NUM_CANDS_FIELD.getPreferredName()
                        + "] field was mandatory in previous releases "
                        + "and is required to be non-null by some nodes. "
                        + "Please make sure to provide the parameter as part of the request."
                );
            } else {
                out.writeVInt(numCands);
            }
        }
        if (out.getTransportVersion().before(TransportVersions.V_8_7_0)
            || out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeFloatArray(queryVector);
        } else {
            out.writeBoolean(true);
            out.writeFloatArray(queryVector);
            out.writeBoolean(false); // used for byteQueryVector, which was always null
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            writeQueries(out, filterQueries);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeOptionalFloat(vectorSimilarity);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        if (numCands != null) {
            builder.field(NUM_CANDS_FIELD.getPreferredName(), numCands);
        }
        if (vectorSimilarity != null) {
            builder.field(VECTOR_SIMILARITY_FIELD.getPreferredName(), vectorSimilarity);
        }
        if (filterQueries.isEmpty() == false) {
            builder.startArray(FILTER_FIELD.getPreferredName());
            for (QueryBuilder filterQuery : filterQueries) {
                filterQuery.toXContent(builder, params);
            }
            builder.endArray();
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext ctx) throws IOException {
        if (ctx.convertToInnerHitsRewriteContext() != null) {
            return new ExactKnnQueryBuilder(queryVector, fieldName).boost(boost).queryName(queryName);
        }
        boolean changed = false;
        List<QueryBuilder> rewrittenQueries = new ArrayList<>(filterQueries.size());
        for (QueryBuilder query : filterQueries) {
            QueryBuilder rewrittenQuery = query.rewrite(ctx);
            if (rewrittenQuery instanceof MatchNoneQueryBuilder) {
                return rewrittenQuery;
            }
            if (rewrittenQuery != query) {
                changed = true;
            }
            rewrittenQueries.add(rewrittenQuery);
        }
        if (changed) {
            return new KnnVectorQueryBuilder(fieldName, queryVector, numCands, vectorSimilarity).boost(boost)
                .queryName(queryName)
                .addFilterQueries(rewrittenQueries);
        }
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        int requestSize = context.requestSize() == null || context.requestSize() < 0 ? DEFAULT_SIZE : context.requestSize();
        int adjustedNumCands = numCands == null
            ? Math.round(Math.min(NUM_CANDS_MULTIPLICATIVE_FACTOR * requestSize, NUM_CANDS_LIMIT))
            : numCands;
        if (fieldType == null) {
            throw new IllegalArgumentException("field [" + fieldName + "] does not exist in the mapping");
        }

        if (fieldType instanceof DenseVectorFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + DenseVectorFieldMapper.CONTENT_TYPE + "] fields"
            );
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryBuilder query : this.filterQueries) {
            builder.add(query.toQuery(context), BooleanClause.Occur.FILTER);
        }
        if (context.getAliasFilter() != null) {
            builder.add(context.getAliasFilter().toQuery(context), BooleanClause.Occur.FILTER);
        }
        BooleanQuery booleanQuery = builder.build();
        Query filterQuery = booleanQuery.clauses().isEmpty() ? null : booleanQuery;

        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) fieldType;
        String parentPath = context.nestedLookup().getNestedParent(fieldName);

        if (parentPath != null) {
            final BitSetProducer parentBitSet;
            final Query parentFilter;
            NestedObjectMapper originalObjectMapper = context.nestedScope().getObjectMapper();
            if (originalObjectMapper != null) {
                try {
                    // we are in a nested context, to get the parent filter we need to go up one level
                    context.nestedScope().previousLevel();
                    NestedObjectMapper objectMapper = context.nestedScope().getObjectMapper();
                    parentFilter = objectMapper == null
                        ? Queries.newNonNestedFilter(context.indexVersionCreated())
                        : objectMapper.nestedTypeFilter();
                } finally {
                    context.nestedScope().nextLevel(originalObjectMapper);
                }
            } else {
                // we are NOT in a nested context, coming from the top level knn search
                parentFilter = Queries.newNonNestedFilter(context.indexVersionCreated());
            }
            parentBitSet = context.bitsetFilter(parentFilter);
            if (filterQuery != null) {
                NestedHelper nestedHelper = new NestedHelper(context.nestedLookup(), context::isFieldMapped);
                // We treat the provided filter as a filter over PARENT documents, so if it might match nested documents
                // we need to adjust it.
                if (nestedHelper.mightMatchNestedDocs(filterQuery)) {
                    // Ensure that the query only returns parent documents matching `filterQuery`
                    filterQuery = Queries.filtered(filterQuery, parentFilter);
                }
                // Now join the filterQuery & parentFilter to provide the matching blocks of children
                filterQuery = new ToChildBlockJoinQuery(filterQuery, parentBitSet);
            }
            return vectorFieldType.createKnnQuery(queryVector, adjustedNumCands, filterQuery, vectorSimilarity, parentBitSet);
        }
        return vectorFieldType.createKnnQuery(queryVector, adjustedNumCands, filterQuery, vectorSimilarity, null);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(queryVector), numCands, filterQueries, vectorSimilarity);
    }

    @Override
    protected boolean doEquals(KnnVectorQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Arrays.equals(queryVector, other.queryVector)
            && Objects.equals(numCands, other.numCands)
            && Objects.equals(filterQueries, other.filterQueries)
            && Objects.equals(vectorSimilarity, other.vectorSimilarity);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_0_0;
    }
}
