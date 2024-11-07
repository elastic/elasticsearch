/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A query that matches the provided docs with their scores. This query is used
 * when executing a kNN search during the search query phase, to include the documents
 * that matched the initial kNN query during the DFS phase.
 */
public class KnnScoreDocQueryBuilder extends AbstractQueryBuilder<KnnScoreDocQueryBuilder> {
    public static final String NAME = "knn_score_doc";
    private final ScoreDoc[] scoreDocs;
    private final String fieldName;
    private final VectorData queryVector;
    private final Float vectorSimilarity;

    /**
     * Creates a query builder.
     *
     * @param scoreDocs the docs and scores this query should match. The array must be
     *                  sorted in order of ascending doc IDs.
     */
    public KnnScoreDocQueryBuilder(ScoreDoc[] scoreDocs, String fieldName, VectorData queryVector, Float vectorSimilarity) {
        this.scoreDocs = scoreDocs;
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.vectorSimilarity = vectorSimilarity;
    }

    public KnnScoreDocQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.scoreDocs = in.readArray(Lucene::readScoreDoc, ScoreDoc[]::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            this.fieldName = in.readOptionalString();
            if (in.readBoolean()) {
                if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                    this.queryVector = in.readOptionalWriteable(VectorData::new);
                } else {
                    this.queryVector = VectorData.fromFloats(in.readFloatArray());
                }
            } else {
                this.queryVector = null;
            }
        } else {
            this.fieldName = null;
            this.queryVector = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.FIX_VECTOR_SIMILARITY_INNER_HITS)
            || in.getTransportVersion().isPatchFrom(TransportVersions.V_8_15_0)) {
            this.vectorSimilarity = in.readOptionalFloat();
        } else {
            this.vectorSimilarity = null;
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeOptionalString(fieldName);
            if (queryVector != null) {
                out.writeBoolean(true);
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                    out.writeOptionalWriteable(queryVector);
                } else {
                    out.writeFloatArray(queryVector.asFloatVector());
                }
            } else {
                out.writeBoolean(false);
            }
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.FIX_VECTOR_SIMILARITY_INNER_HITS)
            || out.getTransportVersion().isPatchFrom(TransportVersions.V_8_15_0)) {
            out.writeOptionalFloat(vectorSimilarity);
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
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        int numDocs = scoreDocs.length;
        int[] docs = new int[numDocs];
        float[] scores = new float[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = scoreDocs[i].doc;
            scores[i] = scoreDocs[i].score;
        }

        IndexReader reader = context.getIndexReader();
        int[] segmentStarts = findSegmentStarts(reader, docs);
        return new KnnScoreDocQuery(docs, scores, segmentStarts, reader.getContext().id());
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (scoreDocs.length == 0) {
            return new MatchNoneQueryBuilder("The \"" + getName() + "\" query was rewritten to a \"match_none\" query.");
        }
        if (queryRewriteContext.convertToInnerHitsRewriteContext() != null && queryVector != null && fieldName != null) {
            return new ExactKnnQueryBuilder(queryVector, fieldName, vectorSimilarity);
        }
        return super.doRewrite(queryRewriteContext);
    }

    private static int[] findSegmentStarts(IndexReader reader, int[] docs) {
        int[] starts = new int[reader.leaves().size() + 1];
        starts[starts.length - 1] = docs.length;
        if (starts.length == 2) {
            return starts;
        }
        int resultIndex = 0;
        for (int i = 1; i < starts.length - 1; i++) {
            int upper = reader.leaves().get(i).docBase;
            resultIndex = Arrays.binarySearch(docs, resultIndex, docs.length, upper);
            if (resultIndex < 0) {
                resultIndex = -1 - resultIndex;
            }
            starts[i] = resultIndex;
        }
        return starts;
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
            && Objects.equals(vectorSimilarity, other.vectorSimilarity);
    }

    @Override
    protected int doHashCode() {
        int result = 1;
        for (ScoreDoc scoreDoc : scoreDocs) {
            int hashCode = Objects.hash(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
            result = 31 * result + hashCode;
        }
        return Objects.hash(result, fieldName, vectorSimilarity, Objects.hashCode(queryVector));
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_4_0;
    }
}
