/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * This is an extension of {@link DiversifyingChildrenByteKnnVectorQuery} that gathers the top {@code numChildrenPerParent} children,
 * not just the single nearest. This is useful for gathering inner hits
 * It will return the top {@code numChildrenPerParent} children for each parent, this is done as a separate step after
 * the initial query is executed.
 */
public class ESDiversifyingChildrenByteKnnVectorQuery extends DiversifyingChildrenByteKnnVectorQuery {

    private final int numChildrenPerParent;
    private final BitSetProducer parentsFilter;
    private final byte[] query;

    /**
     * @param field         the query field
     * @param query         the vector query
     * @param childFilter   the child filter
     * @param k             how many parent documents to return given the matching children
     * @param parentsFilter identifying the parent documents.
     * @param numChildrenPerParent how many nearest children to return for each parent
     */
    public ESDiversifyingChildrenByteKnnVectorQuery(
        String field,
        byte[] query,
        Query childFilter,
        int k,
        BitSetProducer parentsFilter,
        int numChildrenPerParent
    ) {
        super(field, query, childFilter, k, parentsFilter);
        this.numChildrenPerParent = numChildrenPerParent;
        if (this.numChildrenPerParent < 0) {
            throw new IllegalArgumentException("numChildrenPerParent must be >= 0");
        }
        this.parentsFilter = parentsFilter;
        this.query = query;
    }

    /**
     * This will first execute as a normal {@link DiversifyingChildrenByteKnnVectorQuery} and then gather the top
     * {@code numChildrenPerParent} children for each parent given the results of the initial query.
     * The returned query will be a {@link KnnScoreDocQuery} which will be used to score the top {@code numChildrenPerParent} children
     * for each parent.
     */
    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        IndexReader reader = indexSearcher.getIndexReader();

        // This gathers the nearest single child for each parent
        Query rewritten = super.rewrite(indexSearcher);
        if (numChildrenPerParent <= 1) {
            return rewritten;
        }
        // Iterate these matched docs to attempt to gather and score at least numChildrenPerParent children for each parent
        // returning the top numChildrenPerParent children for each parent
        // The matchingChildren used to iterate the current matching children to gather the appropriate parents & children
        Weight matchingChildren = rewritten.createWeight(indexSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        Weight childrenFilter = getFilter() == null
            ? null
            : indexSearcher.rewrite(
                new BooleanQuery.Builder().add(getFilter(), BooleanClause.Occur.FILTER)
                    .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
                    .build()
            ).createWeight(indexSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        ChildBlockJoinVectorScorerProvider childBlockJoinVectorScorerProvider = new ChildBlockJoinVectorScorerProvider(
            childrenFilter,
            matchingChildren,
            context -> new ChildBlockJoinVectorScorerProvider.VectorScorer() {
                private final ByteVectorValues values = context.reader().getByteVectorValues(field);
                private final VectorSimilarityFunction function = context.reader()
                    .getFieldInfos()
                    .fieldInfo(field)
                    .getVectorSimilarityFunction();

                @Override
                public float score() throws IOException {
                    return function.compare(query, values.vectorValue());
                }

                @Override
                public DocIdSetIterator iterator() {
                    return values;
                }
            },
            parentsFilter,
            numChildrenPerParent
        );

        List<ScoreDoc> topChildren = new ArrayList<>();
        for (LeafReaderContext context : reader.leaves()) {
            final FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
            if (fi == null || fi.getVectorDimension() == 0) {
                // The field does not exist or does not index vectors
                continue;
            }
            if (fi.getVectorEncoding() != VectorEncoding.BYTE) {
                continue;
            }
            ChildBlockJoinVectorScorerProvider.ChildBlockJoinVectorScorer scorer = childBlockJoinVectorScorerProvider.scorer(context);
            if (scorer == null) {
                continue;
            }
            while (scorer.nextParent() != DocIdSetIterator.NO_MORE_DOCS) {
                PriorityQueue<ScoreDoc> childDoc = scorer.scoredChildren();
                while (childDoc.size() > 0) {
                    topChildren.add(childDoc.pop());
                }
            }
        }
        topChildren.sort(Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
        ScoreDoc[] scoreDocs = topChildren.toArray(new ScoreDoc[0]);
        int numDocs = scoreDocs.length;
        int[] docs = new int[numDocs];
        float[] scores = new float[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = scoreDocs[i].doc;
            scores[i] = scoreDocs[i].score;
        }

        int[] segmentStarts = KnnScoreDocQueryBuilder.findSegmentStarts(reader, docs);
        return new KnnScoreDocQuery(docs, scores, segmentStarts, reader.getContext().id());
    }

}
