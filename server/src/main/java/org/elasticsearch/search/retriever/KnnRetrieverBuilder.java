/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class KnnRetrieverBuilder extends RetrieverBuilder<KnnRetrieverBuilder> {

    public static final String NAME = "knn";

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField K_FIELD = new ParseField("k");
    public static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField VECTOR_SIMILARITY = new ParseField("similarity");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<KnnRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        "knn",
        args -> {
            // TODO optimize parsing for when BYTE values are provided
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
            return new KnnRetrieverBuilder(
                (String) args[0],
                vectorArray,
                (QueryVectorBuilder) args[2],
                (int) args[3],
                (int) args[4],
                (Float) args[5]
            );
        }
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareFloatArray(optionalConstructorArg(), QUERY_VECTOR_FIELD);
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(QueryVectorBuilder.class, n, c),
            QUERY_VECTOR_BUILDER_FIELD
        );
        PARSER.declareInt(constructorArg(), K_FIELD);
        PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), VECTOR_SIMILARITY);
        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static KnnRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }

    private final String field;
    private final float[] queryVector;
    private final QueryVectorBuilder queryVectorBuilder;
    private final Supplier<float[]> querySupplier;
    private final int k;
    private final int numCands;
    private final Float similarity;

    public KnnRetrieverBuilder(
        String field,
        float[] queryVector,
        QueryVectorBuilder queryVectorBuilder,
        int k,
        int numCands,
        Float similarity
    ) {
        this.field = field;
        this.queryVector = queryVector;
        this.queryVectorBuilder = queryVectorBuilder;
        this.querySupplier = null;
        this.k = k;
        this.numCands = numCands;
        this.similarity = similarity;
    }

    private KnnRetrieverBuilder(String field, Supplier<float[]> querySupplier, int k, int numCands, Float similarity) {
        this.field = field;
        this.queryVector = new float[0];
        this.queryVectorBuilder = null;
        this.k = k;
        this.numCands = numCands;
        this.querySupplier = querySupplier;
        this.similarity = similarity;
    }

    public KnnRetrieverBuilder(KnnRetrieverBuilder original) {
        super(original);
        field = original.field;
        queryVector = original.queryVector;
        queryVectorBuilder = original.queryVectorBuilder;
        querySupplier = original.querySupplier;
        k = original.k;
        numCands = original.numCands;
        similarity = original.similarity;
    }

    @SuppressWarnings("unchecked")
    public KnnRetrieverBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.k = in.readVInt();
        this.numCands = in.readVInt();
        this.queryVector = in.readFloatArray();
        this.queryVectorBuilder = in.readOptionalNamedWriteable(QueryVectorBuilder.class);
        this.querySupplier = null;
        this.similarity = in.readOptionalFloat();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        if (querySupplier != null) {
            throw new IllegalStateException("missing a rewriteAndFetch?");
        }
        out.writeString(field);
        out.writeVInt(k);
        out.writeVInt(numCands);
        out.writeFloatArray(queryVector);
        out.writeOptionalNamedWriteable(queryVectorBuilder);
        out.writeOptionalFloat(similarity);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.RETRIEVERS_ADDED;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field)
            .field(K_FIELD.getPreferredName(), k)
            .field(NUM_CANDS_FIELD.getPreferredName(), numCands);
        if (queryVectorBuilder != null) {
            builder.startObject(QUERY_VECTOR_BUILDER_FIELD.getPreferredName());
            builder.field(queryVectorBuilder.getWriteableName(), queryVectorBuilder);
            builder.endObject();
        } else {
            builder.array(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        }
        if (similarity != null) {
            builder.field(VECTOR_SIMILARITY.getPreferredName(), similarity);
        }
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public KnnRetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        // Currently this method is never used, as KnnRetrieverBuilder is converted to KnnSearchBuilder during parsing,
        // and rewrite happens on KnnSearchBuilder instead.
        if (querySupplier != null) {
            if (querySupplier.get() == null) {
                return this;
            }
            return new KnnRetrieverBuilder(field, querySupplier.get(), null, k, numCands, similarity).preFilterQueryBuilder(
                preFilterQueryBuilder
            )._name(_name);
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
            return new KnnRetrieverBuilder(field, toSet::get, k, numCands, similarity).preFilterQueryBuilder(preFilterQueryBuilder)
                ._name(_name);
        }
        return super.rewrite(ctx);
    }

    @Override
    protected KnnRetrieverBuilder shallowCopyInstance() {
        return new KnnRetrieverBuilder(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        KnnRetrieverBuilder that = (KnnRetrieverBuilder) o;
        return k == that.k
            && numCands == that.numCands
            && Objects.equals(field, that.field)
            && Arrays.equals(queryVector, that.queryVector)
            && Objects.equals(queryVectorBuilder, that.queryVectorBuilder)
            && Objects.equals(querySupplier, that.querySupplier)
            && Objects.equals(similarity, that.similarity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            field,
            k,
            numCands,
            querySupplier,
            queryVectorBuilder,
            similarity,
            Arrays.hashCode(queryVector)
        );
    }

    @Override
    public void doExtractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        // TODO: add support for multiple knn retrievers per search request
        if (searchSourceBuilder.knnSearch().size() == 0) {
            KnnSearchBuilder knnSearchBuilder = new KnnSearchBuilder(field, queryVector, queryVectorBuilder, k, numCands, similarity);
            if (preFilterQueryBuilder != null) {
                knnSearchBuilder.addFilterQuery(preFilterQueryBuilder);
            }
            this.preFilterQueryBuilder(null);
            // TODO: add support for _name
            searchSourceBuilder.knnSearch(List.of(knnSearchBuilder));
        } else {
            throw new IllegalStateException("[knn] cannot be declared as a retriever value and as a global value");
        }
    }

}
