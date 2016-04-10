/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.HashMap;
import java.util.List;

public final class QueryRescorer implements Rescorer {

    public static final Rescorer INSTANCE = new QueryRescorer();
    public static final String NAME = "query";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TopDocs rescore(TopDocs topDocs, SearchContext context, RescoreSearchContext rescoreContext) throws IOException {

        assert rescoreContext != null;
        if (topDocs == null || topDocs.totalHits == 0 || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }

        final QueryRescoreContext rescore = (QueryRescoreContext) rescoreContext;

        // First take top slice of incoming docs, to be rescored:
        TopDocs topNFirstPass = topN(topDocs, rescore.window());

        // Rescore them:
        HashMap<Integer, Float> rescored = rescore(context.searcher(), topNFirstPass, rescore.query(), rescore);

        // Splice back to non-topN hits and resort all of them:
        return combine(topDocs, rescored, rescore);
    }

    @Override
    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext,
                               Explanation sourceExplanation) throws IOException {
        QueryRescoreContext rescore = (QueryRescoreContext) rescoreContext;
        ContextIndexSearcher searcher = context.searcher();
        if (sourceExplanation == null) {
            // this should not happen but just in case
            return Explanation.noMatch("nothing matched");
        }
        // TODO: this isn't right?  I.e., we are incorrectly pretending all first pass hits were rescored?  If the requested docID was
        // beyond the top rescoreContext.window() in the first pass hits, we don't rescore it now?
        Explanation rescoreExplain = searcher.explain(rescore.query(), topLevelDocId);
        float primaryWeight = rescore.queryWeight();

        Explanation prim;
        if (sourceExplanation.isMatch()) {
            prim = Explanation.match(
                    sourceExplanation.getValue() * primaryWeight,
                    "product of:", sourceExplanation, Explanation.match(primaryWeight, "primaryWeight"));
        } else {
            prim = Explanation.noMatch("First pass did not match", sourceExplanation);
        }

        // NOTE: we don't use Lucene's Rescorer.explain because we want to insert our own description with which ScoreMode was used.  Maybe
        // we should add QueryRescorer.explainCombine to Lucene?
        if (rescoreExplain != null && rescoreExplain.isMatch()) {
            float secondaryWeight = rescore.rescoreQueryWeight();
            Explanation sec = Explanation.match(
                    rescoreExplain.getValue() * secondaryWeight,
                    "product of:",
                    rescoreExplain, Explanation.match(secondaryWeight, "secondaryWeight"));
            QueryRescoreMode scoreMode = rescore.scoreMode();
            return Explanation.match(
                    scoreMode.combine(prim.getValue(), sec.getValue()),
                    scoreMode + " of:",
                    prim, sec);
        } else {
            return prim;
        }
    }

    private static final ObjectParser<QueryRescoreContext, QueryShardContext> RESCORE_PARSER = new ObjectParser<>("query", null);
    private float topScore = 0.0f;
    private float bottomScore = 0.0f;
    private float denominator = Float.NaN;
    private final float getDenominator() {
        if (Float.isNaN(denominator))
        {
            denominator = (topScore == bottomScore) ? 1 : topScore - bottomScore;
        }

        return denominator;
    }

    static {
        RESCORE_PARSER.declareObject(QueryRescoreContext::setQuery, (p, c) -> c.parse(p).query(), new ParseField("rescore_query"));
        RESCORE_PARSER.declareFloat(QueryRescoreContext::setQueryWeight, new ParseField("query_weight"));
        RESCORE_PARSER.declareFloat(QueryRescoreContext::setRescoreQueryWeight, new ParseField("rescore_query_weight"));
        RESCORE_PARSER.declareString(QueryRescoreContext::setScoreMode, new ParseField("score_mode"));
        RESCORE_PARSER.declareBoolean(QueryRescoreContext::setNormalization, new ParseField("normalization"));
    }

    @Override
    public RescoreSearchContext parse(XContentParser parser, QueryShardContext context) throws IOException {
        return RESCORE_PARSER.parse(parser, new QueryRescoreContext(this), context);
    }

    private final static Comparator<ScoreDoc> SCORE_DOC_COMPARATOR = new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc o1, ScoreDoc o2) {
            int cmp = Float.compare(o2.score, o1.score);
            return cmp == 0 ?  Integer.compare(o1.doc, o2.doc) : cmp;
        }
    };

    /** Returns a new {@link TopDocs} with the topN from the incoming one, or the same TopDocs if the number of hits is already &lt;=
     *  topN. */
    private TopDocs topN(TopDocs in, int topN) {
        if (in.totalHits < topN) {
            assert in.scoreDocs.length == in.totalHits;
            return in;
        }

        ScoreDoc[] subset = new ScoreDoc[topN];
        System.arraycopy(in.scoreDocs, 0, subset, 0, topN);

        return new TopDocs(in.totalHits, subset, in.getMaxScore());
    }

    /** Returns rescored records that matched the TopDocs returned from topN. This is overriding the Lucene rescore method which requires
     *  additional knowledge of the query and the RescoreContext
     * */
    public HashMap<Integer, Float> rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, Query query, QueryRescoreContext ctx)
        throws IOException {
        ScoreDoc[] hits = firstPassTopDocs.scoreDocs.clone();
        Arrays.sort(hits, new Comparator<ScoreDoc>() {
            @Override
            public int compare(ScoreDoc a, ScoreDoc b) {
                return a.doc - b.doc;
            }
        });

        HashMap<Integer, Float> rescoreHits = new HashMap<Integer, Float>();
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

        Weight weight = searcher.createNormalizedWeight(query, true);

        // Now merge sort docIDs from hits, with reader's leaves:
        int hitUpto = 0;
        int readerUpto = -1;
        int endDoc = 0;
        int docBase = 0;
        boolean firstMatch = true;

        // Scorer returns results by document ID, when looping through, there is no Random Access so scorer.advance(int)
        // has to be the next ID in the TopDocs
        for(Scorer scorer = null; hitUpto < hits.length; ++hitUpto) {
            ScoreDoc hit = hits[hitUpto];
            int docID = hit.doc;

            LeafReaderContext readerContext = null;
            while (docID >= endDoc) {
                readerUpto++;
                readerContext = leaves.get(readerUpto);
                endDoc = readerContext.docBase + readerContext.reader().maxDoc();
            }

            if(readerContext != null) {
                // We advanced to another segment:
                docBase = readerContext.docBase;

                scorer = weight.scorer(readerContext);
            }

            if(scorer != null) {
                int targetDoc = docID - docBase;
                int actualDoc = scorer.docID();
                if(actualDoc < targetDoc) {
                    actualDoc = scorer.iterator().advance(targetDoc);
                }

                if(actualDoc == targetDoc) {
                    float score = scorer.score();

                    if (firstMatch) {
                        topScore = score;
                        bottomScore = score;
                        firstMatch = false;
                    }

                    if (topScore < score)
                        topScore = score;
                    if (bottomScore > score)
                        bottomScore = score;

                    // Query did match this doc:
                    rescoreHits.put(docID, score);
                } else {
                    // Query did not match this doc:
                    assert actualDoc > targetDoc;
                }
            }
        }

        // Moved Arrays.sort() from Lucene engine into our local .combine() method to sort
        // after calculations are defined. This is to prevent sorting unnecessarily
        return rescoreHits;
    }

    /** Modifies incoming TopDocs (in) by replacing the top hits with resorted's hits, and then resorting all hits. */
    private TopDocs combine(TopDocs in, HashMap<Integer, Float> resorted, QueryRescoreContext ctx) {
        for(int i=0;i<in.scoreDocs.length;i++) {
            if (resorted.size() != 0 && resorted.containsKey(in.scoreDocs[i].doc))
            {
                float score = (!ctx.normalize) ? resorted.get(in.scoreDocs[i].doc) :
                    ((resorted.get(in.scoreDocs[i].doc) - bottomScore) / getDenominator());
                in.scoreDocs[i].score = ctx.scoreMode.combine(in.scoreDocs[i].score * ctx.queryWeight(), score * ctx.rescoreQueryWeight());
            }
            else
            {
                in.scoreDocs[i].score *= ctx.queryWeight();
            }
        }

        Arrays.sort(in.scoreDocs, SCORE_DOC_COMPARATOR);

        return in;
    }

    public static class QueryRescoreContext extends RescoreSearchContext {

        static final int DEFAULT_WINDOW_SIZE = 10;
        static final boolean DEFAULT_NORMALIZE = false;

        public QueryRescoreContext(QueryRescorer rescorer) {
            super(NAME, DEFAULT_WINDOW_SIZE, rescorer, DEFAULT_NORMALIZE);
            this.scoreMode = QueryRescoreMode.Total;
        }

        private Query query;
        private float queryWeight = 1.0f;
        private float rescoreQueryWeight = 1.0f;
        private QueryRescoreMode scoreMode;
        private boolean normalize = DEFAULT_NORMALIZE;

        public void setQuery(Query query) {
            this.query = query;
        }

        public Query query() {
            return query;
        }

        public float queryWeight() {
            return queryWeight;
        }

        public float rescoreQueryWeight() {
            return rescoreQueryWeight;
        }

        public QueryRescoreMode scoreMode() {
            return scoreMode;
        }

        public boolean getNormalization() { return normalize; }

        public void setRescoreQueryWeight(float rescoreQueryWeight) {
            this.rescoreQueryWeight = rescoreQueryWeight;
        }

        public void setQueryWeight(float queryWeight) {
            this.queryWeight = queryWeight;
        }

        public void setScoreMode(QueryRescoreMode scoreMode) {
            this.scoreMode = scoreMode;
        }

        public void setScoreMode(String scoreMode) {
            setScoreMode(QueryRescoreMode.fromString(scoreMode));
        }

        public void setNormalization(boolean normalize) { this.normalize = normalize; }
    }

    @Override
    public void extractTerms(SearchContext context, RescoreSearchContext rescoreContext, Set<Term> termsSet) {
        try {
            context.searcher().createNormalizedWeight(((QueryRescoreContext) rescoreContext).query(), false).extractTerms(termsSet);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to extract terms", e);
        }
    }

}
