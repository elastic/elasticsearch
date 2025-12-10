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
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.ToChildBlockJoinQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A query that matches the provided docs with their scores. This query is used
 * when executing a kNN search during the search query phase, to include the documents
 * that matched the initial kNN query during the DFS phase.
 */
public class KnnScoreDocQueryBuilder extends AbstractQueryBuilder<KnnScoreDocQueryBuilder> {
    public static final String NAME = "knn_score_doc";

    private static final TransportVersion TO_CHILD_BLOCK_JOIN_QUERY = TransportVersion.fromName("to_child_block_join_query");

    private final ScoreDoc[] scoreDocs;
    private final String fieldName;
    private final VectorData queryVector;
    private final Float vectorSimilarity;
    private final List<QueryBuilder> filterQueries;

    /**
     * Creates a query builder.
     *
     * @param scoreDocs the docs and scores this query should match. The array must be
     *                  sorted in order of ascending doc IDs.
     */
    public KnnScoreDocQueryBuilder(
        ScoreDoc[] scoreDocs,
        String fieldName,
        VectorData queryVector,
        Float vectorSimilarity,
        List<QueryBuilder> filterQueries
    ) {
        this.scoreDocs = scoreDocs;
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.vectorSimilarity = vectorSimilarity;
        this.filterQueries = filterQueries;
    }

    public KnnScoreDocQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.scoreDocs = in.readArray(Lucene::readScoreDoc, ScoreDoc[]::new);
        this.fieldName = in.readOptionalString();
        if (in.readBoolean()) {
            this.queryVector = in.readOptionalWriteable(VectorData::new);
        } else {
            this.queryVector = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            this.vectorSimilarity = in.readOptionalFloat();
        } else {
            this.vectorSimilarity = null;
        }
        if (in.getTransportVersion().supports(TO_CHILD_BLOCK_JOIN_QUERY)) {
            this.filterQueries = readQueries(in);
        } else {
            this.filterQueries = List.of();
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public ScoreDoc[] scoreDocs() {
        return scoreDocs;
    }

    String fieldName() {
        return fieldName;
    }

    VectorData queryVector() {
        return queryVector;
    }

    Float vectorSimilarity() {
        return vectorSimilarity;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeArray(Lucene::writeScoreDoc, scoreDocs);
        out.writeOptionalString(fieldName);
        if (queryVector != null) {
            out.writeBoolean(true);
            out.writeOptionalWriteable(queryVector);
        } else {
            out.writeBoolean(false);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeOptionalFloat(vectorSimilarity);
        }
        if (out.getTransportVersion().supports(TO_CHILD_BLOCK_JOIN_QUERY)) {
            writeQueries(out, filterQueries);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("values");
        for (ScoreDoc scoreDoc : scoreDocs) {
            builder.startObject().field("doc", scoreDoc.doc).field("score", scoreDoc.score).endObject();
        }
        builder.endArray();
        if (fieldName != null) {
            builder.field("field", fieldName);
        }
        if (queryVector != null) {
            builder.field("query", queryVector);
        }
        if (vectorSimilarity != null) {
            builder.field("similarity", vectorSimilarity);
        }
        if (filterQueries.isEmpty() == false) {
            builder.startArray("filter");
            for (QueryBuilder filterQuery : filterQueries) {
                filterQuery.toXContent(builder, params);
            }
            builder.endArray();
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return new KnnScoreDocQuery(scoreDocs, context.getIndexReader());
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (scoreDocs.length == 0) {
            return new MatchNoneQueryBuilder("The \"" + getName() + "\" query was rewritten to a \"match_none\" query.");
        }
        if (queryRewriteContext.convertToInnerHitsRewriteContext() != null && queryVector != null && fieldName != null) {
            QueryBuilder exactKnnQuery = new ExactKnnQueryBuilder(queryVector, fieldName, vectorSimilarity);
            if (filterQueries.isEmpty()) {
                return exactKnnQuery;
            } else {
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                boolQuery.must(exactKnnQuery);
                for (QueryBuilder filter : this.filterQueries) {
                    // filter can be both over parents or nested docs, so add them as should clauses to a filter
                    BoolQueryBuilder adjustedFilter = new BoolQueryBuilder().should(filter)
                        .should(new ToChildBlockJoinQueryBuilder(filter));
                    boolQuery.filter(adjustedFilter);
                }
                return boolQuery;
            }
        }
        return super.doRewrite(queryRewriteContext);
    }

    @Override
    protected boolean doEquals(KnnScoreDocQueryBuilder other) {
        if (scoreDocs.length != other.scoreDocs.length) {
            return false;
        }

        for (int i = 0; i < scoreDocs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            ScoreDoc otherScoreDoc = other.scoreDocs[i];

            if ((scoreDoc.doc == otherScoreDoc.doc
                && scoreDoc.score == otherScoreDoc.score
                && scoreDoc.shardIndex == otherScoreDoc.shardIndex) == false) {
                return false;
            }
        }
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(queryVector, other.queryVector)
            && Objects.equals(vectorSimilarity, other.vectorSimilarity)
            && Objects.equals(filterQueries, other.filterQueries);
    }

    @Override
    protected int doHashCode() {
        int result = 1;
        for (ScoreDoc scoreDoc : scoreDocs) {
            int hashCode = Objects.hash(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
            result = 31 * result + hashCode;
        }
        return Objects.hash(result, fieldName, vectorSimilarity, Objects.hashCode(queryVector), filterQueries);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }
}
