/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.Version;
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

    /**
     * Creates a query builder.
     *
     * @param scoreDocs the docs and scores this query should match. The array must be
     *                  sorted in order of ascending doc IDs.
     */
    public KnnScoreDocQueryBuilder(ScoreDoc[] scoreDocs) {
        this.scoreDocs = scoreDocs;
    }

    public KnnScoreDocQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.scoreDocs = in.readArray(Lucene::readScoreDoc, ScoreDoc[]::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public ScoreDoc[] scoreDocs() {
        return scoreDocs;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeArray(Lucene::writeScoreDoc, scoreDocs);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("values");
        for (ScoreDoc scoreDoc : scoreDocs) {
            builder.startObject().field("doc", scoreDoc.doc).field("score", scoreDoc.score).endObject();
        }
        builder.endArray();
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
            return new MatchNoneQueryBuilder();
        }
        return super.doRewrite(queryRewriteContext);
    }

    private int[] findSegmentStarts(IndexReader reader, int[] docs) {
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
        return true;
    }

    @Override
    protected int doHashCode() {
        int result = 1;
        for (ScoreDoc scoreDoc : scoreDocs) {
            int hashCode = Objects.hash(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
            result = 31 * result + hashCode;
        }
        return result;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_4_0;
    }
}
