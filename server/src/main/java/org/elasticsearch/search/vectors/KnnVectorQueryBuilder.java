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
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A query that performs kNN search using Lucene's {@link KnnVectorQuery}.
 *
 * NOTE: this is an internal class and should not be used outside of core Elasticsearch code.
 */
public class KnnVectorQueryBuilder extends AbstractQueryBuilder<KnnVectorQueryBuilder> {
    public static final String NAME = "knn";

    private final String fieldName;
    private final float[] queryVector;
    private final byte[] byteQueryVector;
    private final int numCands;
    private final List<QueryBuilder> filterQueries;

    public KnnVectorQueryBuilder(String fieldName, float[] queryVector, int numCands) {
        this.fieldName = fieldName;
        this.queryVector = Objects.requireNonNull(queryVector);
        this.byteQueryVector = null;
        this.numCands = numCands;
        this.filterQueries = new ArrayList<>();
    }

    public KnnVectorQueryBuilder(String fieldName, byte[] queryVector, int numCands) {
        this.fieldName = fieldName;
        this.queryVector = null;
        this.byteQueryVector = Objects.requireNonNull(queryVector);
        this.numCands = numCands;
        this.filterQueries = new ArrayList<>();
    }

    public KnnVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.numCands = in.readVInt();
        if (in.getVersion().before(Version.V_8_7_0)) {
            this.queryVector = in.readFloatArray();
            this.byteQueryVector = null;
        } else {
            this.queryVector = in.readBoolean() ? in.readFloatArray() : null;
            this.byteQueryVector = in.readBoolean() ? in.readByteArray() : null;
        }
        if (in.getVersion().before(Version.V_8_2_0)) {
            this.filterQueries = new ArrayList<>();
        } else {
            this.filterQueries = readQueries(in);
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
    public byte[] getByteQueryVector() {
        return byteQueryVector;
    }

    public int numCands() {
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
        out.writeVInt(numCands);
        if (out.getVersion().onOrAfter(Version.V_8_7_0)) {
            boolean queryVectorNotNull = queryVector != null;
            out.writeBoolean(queryVectorNotNull);
            if (queryVectorNotNull) {
                out.writeFloatArray(queryVector);
            }
            boolean byteVectorNotNull = byteQueryVector != null;
            out.writeBoolean(byteVectorNotNull);
            if (byteVectorNotNull) {
                out.writeByteArray(byteQueryVector);
            }
        } else {
            final float[] f;
            if (queryVector != null) {
                f = queryVector;
            } else {
                f = new float[byteQueryVector.length];
                for (int i = 0; i < byteQueryVector.length; i++) {
                    f[i] = byteQueryVector[i];
                }
            }
            out.writeFloatArray(f);
        }
        if (out.getVersion().onOrAfter(Version.V_8_2_0)) {
            writeQueries(out, filterQueries);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME)
            .field("field", fieldName)
            .field("vector", queryVector != null ? queryVector : byteQueryVector)
            .field("num_candidates", numCands);
        if (filterQueries.isEmpty() == false) {
            builder.startArray("filters");
            for (QueryBuilder filterQuery : filterQueries) {
                filterQuery.toXContent(builder, params);
            }
            builder.endArray();
        }

        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        boolean changed = false;
        List<QueryBuilder> rewrittenQueries = new ArrayList<>(filterQueries.size());
        for (QueryBuilder query : filterQueries) {
            QueryBuilder rewrittenQuery = query.rewrite(queryRewriteContext);
            if (rewrittenQuery instanceof MatchNoneQueryBuilder) {
                return rewrittenQuery;
            }
            if (rewrittenQuery != query) {
                changed = true;
            }
            rewrittenQueries.add(rewrittenQuery);
        }
        if (changed) {
            return byteQueryVector != null
                ? new KnnVectorQueryBuilder(fieldName, byteQueryVector, numCands).addFilterQueries(rewrittenQueries)
                : new KnnVectorQueryBuilder(fieldName, queryVector, numCands).addFilterQueries(rewrittenQueries);
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

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryBuilder query : this.filterQueries) {
            builder.add(query.toQuery(context), BooleanClause.Occur.FILTER);
        }
        BooleanQuery booleanQuery = builder.build();
        Query filterQuery = booleanQuery.clauses().isEmpty() ? null : booleanQuery;

        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) fieldType;
        return queryVector != null
            ? vectorFieldType.createKnnQuery(queryVector, numCands, filterQuery)
            : vectorFieldType.createKnnQuery(new BytesRef(byteQueryVector), numCands, filterQuery);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(queryVector), Arrays.hashCode(byteQueryVector), numCands, filterQueries);
    }

    @Override
    protected boolean doEquals(KnnVectorQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Arrays.equals(queryVector, other.queryVector)
            && Arrays.equals(byteQueryVector, other.byteQueryVector)
            && numCands == other.numCands
            && Objects.equals(filterQueries, other.filterQueries);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_0_0;
    }
}
