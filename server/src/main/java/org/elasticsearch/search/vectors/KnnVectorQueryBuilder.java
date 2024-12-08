/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
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
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query that performs kNN search using Lucene's {@link org.apache.lucene.search.KnnFloatVectorQuery} or
 * {@link org.apache.lucene.search.KnnByteVectorQuery}.
 */
public class KnnVectorQueryBuilder extends AbstractQueryBuilder<KnnVectorQueryBuilder> {
    public static final NodeFeature K_PARAM_SUPPORTED = new NodeFeature("search.vectors.k_param_supported");

    public static final String NAME = "knn";
    private static final int NUM_CANDS_LIMIT = 10_000;
    private static final float NUM_CANDS_MULTIPLICATIVE_FACTOR = 1.5f;

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField K_FIELD = new ParseField("k");
    public static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField VECTOR_SIMILARITY_FIELD = new ParseField("similarity");
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<KnnVectorQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "knn",
        args -> new KnnVectorQueryBuilder(
            (String) args[0],
            (VectorData) args[1],
            (QueryVectorBuilder) args[5],
            null,
            (Integer) args[2],
            (Integer) args[3],
            (Float) args[4]
        )
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> VectorData.parseXContent(p),
            QUERY_VECTOR_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_STRING_OR_NUMBER
        );
        PARSER.declareInt(optionalConstructorArg(), K_FIELD);
        PARSER.declareInt(optionalConstructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), VECTOR_SIMILARITY_FIELD);
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(QueryVectorBuilder.class, n, c),
            QUERY_VECTOR_BUILDER_FIELD
        );
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
    private final VectorData queryVector;
    private final Integer k;
    private Integer numCands;
    private final List<QueryBuilder> filterQueries = new ArrayList<>();
    private final Float vectorSimilarity;
    private final QueryVectorBuilder queryVectorBuilder;
    private final Supplier<float[]> queryVectorSupplier;

    public KnnVectorQueryBuilder(String fieldName, float[] queryVector, Integer k, Integer numCands, Float vectorSimilarity) {
        this(fieldName, VectorData.fromFloats(queryVector), null, null, k, numCands, vectorSimilarity);
    }

    public KnnVectorQueryBuilder(
        String fieldName,
        QueryVectorBuilder queryVectorBuilder,
        Integer k,
        Integer numCands,
        Float vectorSimilarity
    ) {
        this(fieldName, null, queryVectorBuilder, null, k, numCands, vectorSimilarity);
    }

    public KnnVectorQueryBuilder(String fieldName, byte[] queryVector, Integer k, Integer numCands, Float vectorSimilarity) {
        this(fieldName, VectorData.fromBytes(queryVector), null, null, k, numCands, vectorSimilarity);
    }

    public KnnVectorQueryBuilder(String fieldName, VectorData queryVector, Integer k, Integer numCands, Float vectorSimilarity) {
        this(fieldName, queryVector, null, null, k, numCands, vectorSimilarity);
    }

    private KnnVectorQueryBuilder(
        String fieldName,
        VectorData queryVector,
        QueryVectorBuilder queryVectorBuilder,
        Supplier<float[]> queryVectorSupplier,
        Integer k,
        Integer numCands,
        Float vectorSimilarity
    ) {
        if (k != null && k < 1) {
            throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (numCands != null && numCands > NUM_CANDS_LIMIT) {
            throw new IllegalArgumentException("[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot exceed [" + NUM_CANDS_LIMIT + "]");
        }
        if (k != null && numCands != null && numCands < k) {
            throw new IllegalArgumentException(
                "[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot be less than [" + K_FIELD.getPreferredName() + "]"
            );
        }
        if (queryVector == null && queryVectorBuilder == null) {
            throw new IllegalArgumentException(
                format(
                    "either [%s] or [%s] must be provided",
                    QUERY_VECTOR_FIELD.getPreferredName(),
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName()
                )
            );
        } else if (queryVector != null && queryVectorBuilder != null) {
            throw new IllegalArgumentException(
                format(
                    "only one of [%s] and [%s] must be provided",
                    QUERY_VECTOR_FIELD.getPreferredName(),
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName()
                )
            );
        }
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.k = k;
        this.numCands = numCands;
        this.vectorSimilarity = vectorSimilarity;
        this.queryVectorBuilder = queryVectorBuilder;
        this.queryVectorSupplier = queryVectorSupplier;
    }

    public KnnVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            this.k = in.readOptionalVInt();
        } else {
            this.k = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            this.numCands = in.readOptionalVInt();
        } else {
            this.numCands = in.readVInt();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            this.queryVector = in.readOptionalWriteable(VectorData::new);
        } else {
            if (in.getTransportVersion().before(TransportVersions.V_8_7_0)
                || in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                this.queryVector = VectorData.fromFloats(in.readFloatArray());
            } else {
                in.readBoolean();
                this.queryVector = VectorData.fromFloats(in.readFloatArray());
                in.readBoolean(); // used for byteQueryVector, which was always null
            }
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            this.filterQueries.addAll(readQueries(in));
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            this.vectorSimilarity = in.readOptionalFloat();
        } else {
            this.vectorSimilarity = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            this.queryVectorBuilder = in.readOptionalNamedWriteable(QueryVectorBuilder.class);
        } else {
            this.queryVectorBuilder = null;
        }
        this.queryVectorSupplier = null;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    public VectorData queryVector() {
        return queryVector;
    }

    @Nullable
    public Float getVectorSimilarity() {
        return vectorSimilarity;
    }

    public Integer k() {
        return k;
    }

    public Integer numCands() {
        return numCands;
    }

    public List<QueryBuilder> filterQueries() {
        return filterQueries;
    }

    @Nullable
    public QueryVectorBuilder queryVectorBuilder() {
        return queryVectorBuilder;
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
        if (queryVectorSupplier != null) {
            throw new IllegalStateException("missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeOptionalVInt(k);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeOptionalWriteable(queryVector);
        } else {
            if (out.getTransportVersion().before(TransportVersions.V_8_7_0)
                || out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeFloatArray(queryVector.asFloatVector());
            } else {
                out.writeBoolean(true);
                out.writeFloatArray(queryVector.asFloatVector());
                out.writeBoolean(false); // used for byteQueryVector, which was always null
            }
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            writeQueries(out, filterQueries);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeOptionalFloat(vectorSimilarity);
        }
        if (out.getTransportVersion().before(TransportVersions.V_8_14_0) && queryVectorBuilder != null) {
            throw new IllegalArgumentException(
                format(
                    "cannot serialize [%s] to older node of version [%s]",
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                    out.getTransportVersion()
                )
            );
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeOptionalNamedWriteable(queryVectorBuilder);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (queryVectorSupplier != null) {
            throw new IllegalStateException("missing a rewriteAndFetch?");
        }
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        if (queryVector != null) {
            builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        }
        if (k != null) {
            builder.field(K_FIELD.getPreferredName(), k);
        }
        if (numCands != null) {
            builder.field(NUM_CANDS_FIELD.getPreferredName(), numCands);
        }
        if (vectorSimilarity != null) {
            builder.field(VECTOR_SIMILARITY_FIELD.getPreferredName(), vectorSimilarity);
        }
        if (queryVectorBuilder != null) {
            builder.startObject(QUERY_VECTOR_BUILDER_FIELD.getPreferredName());
            builder.field(queryVectorBuilder.getWriteableName(), queryVectorBuilder);
            builder.endObject();
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
        if (queryVectorSupplier != null) {
            if (queryVectorSupplier.get() == null) {
                return this;
            }
            return new KnnVectorQueryBuilder(fieldName, queryVectorSupplier.get(), k, numCands, vectorSimilarity).boost(boost)
                .queryName(queryName)
                .addFilterQueries(filterQueries);
        }
        if (queryVectorBuilder != null) {
            SetOnce<float[]> toSet = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> queryVectorBuilder.buildVector(c, l.delegateFailureAndWrap((ll, v) -> {
                toSet.set(v);
                if (v == null) {
                    ll.onFailure(
                        new IllegalArgumentException(
                            format(
                                "[%s] with name [%s] returned null query_vector",
                                QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                                queryVectorBuilder.getWriteableName()
                            )
                        )
                    );
                    return;
                }
                ll.onResponse(null);
            })));
            return new KnnVectorQueryBuilder(fieldName, queryVector, queryVectorBuilder, toSet::get, k, numCands, vectorSimilarity).boost(
                boost
            ).queryName(queryName).addFilterQueries(filterQueries);
        }
        if (ctx.convertToInnerHitsRewriteContext() != null) {
            return new ExactKnnQueryBuilder(queryVector, fieldName, vectorSimilarity).boost(boost).queryName(queryName);
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
            return new KnnVectorQueryBuilder(fieldName, queryVector, queryVectorBuilder, queryVectorSupplier, k, numCands, vectorSimilarity)
                .boost(boost)
                .queryName(queryName)
                .addFilterQueries(rewrittenQueries);
        }
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        int requestSize;
        if (k != null) {
            requestSize = k;
        } else {
            requestSize = context.requestSize() == null || context.requestSize() < 0 ? DEFAULT_SIZE : context.requestSize();
        }
        int adjustedNumCands = numCands == null
            ? Math.round(Math.min(NUM_CANDS_MULTIPLICATIVE_FACTOR * requestSize, NUM_CANDS_LIMIT))
            : numCands;
        if (fieldType == null) {
            return new MatchNoDocsQuery();
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
                // We treat the provided filter as a filter over PARENT documents, so if it might match nested documents
                // we need to adjust it.
                if (NestedHelper.mightMatchNestedDocs(filterQuery, context)) {
                    // Ensure that the query only returns parent documents matching `filterQuery`
                    filterQuery = Queries.filtered(filterQuery, parentFilter);
                }
                // Now join the filterQuery & parentFilter to provide the matching blocks of children
                filterQuery = new ToChildBlockJoinQuery(filterQuery, parentBitSet);
            }
            return vectorFieldType.createKnnQuery(queryVector, k, adjustedNumCands, filterQuery, vectorSimilarity, parentBitSet);
        }
        return vectorFieldType.createKnnQuery(queryVector, k, adjustedNumCands, filterQuery, vectorSimilarity, null);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Objects.hashCode(queryVector), k, numCands, filterQueries, vectorSimilarity, queryVectorBuilder);
    }

    @Override
    protected boolean doEquals(KnnVectorQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(queryVector, other.queryVector)
            && Objects.equals(k, other.k)
            && Objects.equals(numCands, other.numCands)
            && Objects.equals(filterQueries, other.filterQueries)
            && Objects.equals(vectorSimilarity, other.vectorSimilarity)
            && Objects.equals(queryVectorBuilder, other.queryVectorBuilder);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_0_0;
    }
}
