/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A user-facing query that performs exact (brute-force) scoring over every document with a
 * {@code dense_vector} field.
 * <p>
 * By default, scoring is computed against the original full-precision vectors. Setting
 * {@code quantized: true} switches to the codec's quantized scorer (a no-op on non-quantized
 * fields). The {@code similarity_function} parameter can override the scoring metric per query;
 * by default the field's mapped similarity is used.
 * <p>
 * {@code quantized: true} and {@code similarity_function} are mutually exclusive: the codec scorer
 * uses the index-configured similarity and does not support per-query overrides.
 */
public class DenseVectorQueryBuilder extends LeafQueryBuilder<DenseVectorQueryBuilder> {

    public static final String NAME = "dense_vector";

    public static final TransportVersion DENSE_VECTOR_QUERY = TransportVersion.fromName("dense_vector_query");

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField SIMILARITY_FUNCTION_FIELD = new ParseField("similarity_function");
    public static final ParseField QUANTIZED_FIELD = new ParseField("quantized");

    public static final ConstructingObjectParser<DenseVectorQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new DenseVectorQueryBuilder(
            (String) args[0],
            (VectorData) args[1],
            (QueryVectorBuilder) args[2],
            null,
            args[3] == null ? null : parseSimilarityFunction((String) args[3]),
            (Boolean) args[4]
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
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(QueryVectorBuilder.class, n, c),
            QUERY_VECTOR_BUILDER_FIELD
        );
        PARSER.declareString(optionalConstructorArg(), SIMILARITY_FUNCTION_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), QUANTIZED_FIELD);
        declareStandardFields(PARSER);
    }

    public static DenseVectorQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static VectorSimilarity parseSimilarityFunction(String value) {
        try {
            return VectorSimilarity.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown [" + SIMILARITY_FUNCTION_FIELD.getPreferredName() + "] value [" + value + "]");
        }
    }

    private final String fieldName;
    private final VectorData queryVector;
    private final QueryVectorBuilder queryVectorBuilder;
    private final Supplier<float[]> queryVectorSupplier;
    private final VectorSimilarity similarityFunction;
    private final Boolean quantized;

    public DenseVectorQueryBuilder(
        String fieldName,
        VectorData queryVector,
        QueryVectorBuilder queryVectorBuilder,
        VectorSimilarity similarityFunction,
        Boolean quantized
    ) {
        this(fieldName, queryVector, queryVectorBuilder, null, similarityFunction, quantized);
    }

    public DenseVectorQueryBuilder(String fieldName, float[] queryVector, VectorSimilarity similarityFunction, Boolean quantized) {
        this(fieldName, VectorData.fromFloats(queryVector), null, null, similarityFunction, quantized);
    }

    private DenseVectorQueryBuilder(
        String fieldName,
        VectorData queryVector,
        QueryVectorBuilder queryVectorBuilder,
        Supplier<float[]> queryVectorSupplier,
        VectorSimilarity similarityFunction,
        Boolean quantized
    ) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a [" + FIELD_FIELD.getPreferredName() + "]");
        }
        if (queryVector == null && queryVectorBuilder == null) {
            throw new IllegalArgumentException(
                format(
                    "[%s] requires either [%s] or [%s]",
                    NAME,
                    QUERY_VECTOR_FIELD.getPreferredName(),
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName()
                )
            );
        }
        if (queryVector != null && queryVectorBuilder != null) {
            throw new IllegalArgumentException(
                format(
                    "[%s] only one of [%s] and [%s] may be provided",
                    NAME,
                    QUERY_VECTOR_FIELD.getPreferredName(),
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName()
                )
            );
        }
        if (Boolean.TRUE.equals(quantized) && similarityFunction != null) {
            throw new IllegalArgumentException(
                format(
                    "[%s] [%s] cannot be used when [%s] is true",
                    NAME,
                    SIMILARITY_FUNCTION_FIELD.getPreferredName(),
                    QUANTIZED_FIELD.getPreferredName()
                )
            );
        }
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.queryVectorBuilder = queryVectorBuilder;
        this.queryVectorSupplier = queryVectorSupplier;
        this.similarityFunction = similarityFunction;
        this.quantized = quantized;
    }

    public DenseVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.queryVector = in.readOptionalWriteable(VectorData::new);
        this.queryVectorBuilder = in.readOptionalNamedWriteable(QueryVectorBuilder.class);
        String similarityFn = in.readOptionalString();
        this.similarityFunction = similarityFn == null ? null : VectorSimilarity.valueOf(similarityFn);
        this.quantized = in.readOptionalBoolean();
        if (Boolean.TRUE.equals(this.quantized) && this.similarityFunction != null) {
            throw new IllegalArgumentException(
                format(
                    "[%s] [%s] cannot be used when [%s] is true",
                    NAME,
                    SIMILARITY_FUNCTION_FIELD.getPreferredName(),
                    QUANTIZED_FIELD.getPreferredName()
                )
            );
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
    public QueryVectorBuilder queryVectorBuilder() {
        return queryVectorBuilder;
    }

    @Nullable
    public VectorSimilarity getSimilarityFunction() {
        return similarityFunction;
    }

    @Nullable
    public Boolean getQuantized() {
        return quantized;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (queryVectorSupplier != null) {
            throw new IllegalStateException("missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeOptionalWriteable(queryVector);
        out.writeOptionalNamedWriteable(queryVectorBuilder);
        out.writeOptionalString(similarityFunction == null ? null : similarityFunction.name());
        out.writeOptionalBoolean(quantized);
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
        if (queryVectorBuilder != null) {
            builder.startObject(QUERY_VECTOR_BUILDER_FIELD.getPreferredName());
            builder.field(queryVectorBuilder.getWriteableName(), queryVectorBuilder);
            builder.endObject();
        }
        if (similarityFunction != null) {
            builder.field(SIMILARITY_FUNCTION_FIELD.getPreferredName(), similarityFunction.toString());
        }
        if (quantized != null) {
            builder.field(QUANTIZED_FIELD.getPreferredName(), quantized);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext ctx) throws IOException {
        if (queryVectorSupplier != null) {
            if (queryVectorSupplier.get() == null) {
                return this;
            }
            return new DenseVectorQueryBuilder(
                fieldName,
                VectorData.fromFloats(queryVectorSupplier.get()),
                null,
                null,
                similarityFunction,
                quantized
            ).boost(boost).queryName(queryName);
        }
        if (queryVectorBuilder != null) {
            SetOnce<float[]> toSet = new SetOnce<>();
            ctx.registerUniqueAsyncAction(new QueryVectorBuilderAsyncAction(queryVectorBuilder), toSet::set);
            return new DenseVectorQueryBuilder(fieldName, queryVector, queryVectorBuilder, toSet::get, similarityFunction, quantized).boost(
                boost
            ).queryName(queryName);
        }
        // Simple-case rewrite: when the user opts in to quantized scoring without a similarity override,
        // the existing internal ExactKnnQueryBuilder produces the exact same Lucene query. This only holds
        // for indexed fields, which have a codec-bound scorer. Non-indexed (index:false) fields have no
        // quantized representation and are scored from doc values by doToQuery, so they must not take this
        // shortcut. When the field type isn't resolvable yet (e.g. coordinator-node rewrite), fall through;
        // the shard-level rewrite re-resolves it.
        if (Boolean.TRUE.equals(quantized) && similarityFunction == null) {
            MappedFieldType fieldType = ctx.getFieldType(fieldName);
            if (fieldType instanceof DenseVectorFieldType vectorFieldType && vectorFieldType.isSearchable()) {
                return new ExactKnnQueryBuilder(queryVector, fieldName, null).boost(boost).queryName(queryName);
            }
        }
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            throw new IllegalArgumentException("field [" + fieldName + "] does not exist in the mapping");
        }
        if (fieldType instanceof DenseVectorFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + DenseVectorFieldMapper.CONTENT_TYPE + "] fields"
            );
        }
        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) fieldType;
        return vectorFieldType.createExactKnnQuery(queryVector, null, similarityFunction, Boolean.TRUE.equals(quantized));
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, queryVector, queryVectorBuilder, similarityFunction, quantized);
    }

    @Override
    protected boolean doEquals(DenseVectorQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(queryVector, other.queryVector)
            && Objects.equals(queryVectorBuilder, other.queryVectorBuilder)
            && Objects.equals(similarityFunction, other.similarityFunction)
            && Objects.equals(quantized, other.quantized);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return DENSE_VECTOR_QUERY;
    }
}
