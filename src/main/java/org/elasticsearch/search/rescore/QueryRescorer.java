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
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;

public final class QueryRescorer implements Rescorer {

    private static enum ScoreMode {
        Avg {
            @Override
            public float combine(float primary, float secondary) {
                return (primary + secondary) / 2;
            }

            @Override
            public String toString() {
                return "avg";
            }
        },
        Max {
            @Override
            public float combine(float primary, float secondary) {
                return Math.max(primary, secondary);
            }

            @Override
            public String toString() {
                return "max";
            }
        },
        Min {
            @Override
            public float combine(float primary, float secondary) {
                return Math.min(primary, secondary);
            }

            @Override
            public String toString() {
                return "min";
            }
        },
        Total {
            @Override
            public float combine(float primary, float secondary) {
                return primary + secondary;
            }

            @Override
            public String toString() {
                return "sum";
            }
        },
        Multiply {
            @Override
            public float combine(float primary, float secondary) {
                return primary * secondary;
            }

            @Override
            public String toString() {
                return "product";
            }
        };

        public abstract float combine(float primary, float secondary);
    }

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
        TopDocs topNFirstPass = topN(topDocs, rescoreContext.window());

        // Rescore them:
        HashMap<Integer, Float> rescored = rescoreElastic(context.searcher(), topNFirstPass, rescore.query());

        // Splice back to non-topN hits and resort all of them:
        return combine(topDocs, rescored, (QueryRescoreContext) rescoreContext);
    }

    @Override
    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext,
                               Explanation sourceExplanation) throws IOException {
        QueryRescoreContext rescore = (QueryRescoreContext) rescoreContext;
        ContextIndexSearcher searcher = context.searcher();
        if (sourceExplanation == null) {
            // this should not happen but just in case
            return new ComplexExplanation(false, 0.0f, "nothing matched");
        }
        // TODO: this isn't right?  I.e., we are incorrectly pretending all first pass hits were rescored?  If the requested docID was
        // beyond the top rescoreContext.window() in the first pass hits, we don't rescore it now?
        Explanation rescoreExplain = searcher.explain(rescore.query(), topLevelDocId);
        float primaryWeight = rescore.queryWeight();
        ComplexExplanation prim = new ComplexExplanation(sourceExplanation.isMatch(),
                sourceExplanation.getValue() * primaryWeight,
                "product of:");
        prim.addDetail(sourceExplanation);
        prim.addDetail(new Explanation(primaryWeight, "primaryWeight"));

        // NOTE: we don't use Lucene's Rescorer.explain because we want to insert our own description with which ScoreMode was used.  Maybe
        // we should add QueryRescorer.explainCombine to Lucene?
        if (rescoreExplain != null && rescoreExplain.isMatch()) {
            float secondaryWeight = rescore.rescoreQueryWeight();
            ComplexExplanation sec = new ComplexExplanation(rescoreExplain.isMatch(),
                    rescoreExplain.getValue() * secondaryWeight,
                    "product of:");
            sec.addDetail(rescoreExplain);
            sec.addDetail(new Explanation(secondaryWeight, "secondaryWeight"));
            ScoreMode scoreMode = rescore.scoreMode();
            ComplexExplanation calcExpl = new ComplexExplanation();
            calcExpl.setDescription(scoreMode + " of:");
            calcExpl.addDetail(prim);
            calcExpl.setMatch(prim.isMatch());
            calcExpl.addDetail(sec);
            calcExpl.setValue(scoreMode.combine(prim.getValue(), sec.getValue()));
            return calcExpl;
        } else {
            return prim;
        }
    }

    @Override
    public RescoreSearchContext parse(XContentParser parser, SearchContext context) throws IOException {
        Token token;
        String fieldName = null;
        QueryRescoreContext rescoreContext = new QueryRescoreContext(this);
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                if ("rescore_query".equals(fieldName)) {
                    ParsedQuery parsedQuery = context.queryParserService().parse(parser);
                    rescoreContext.setParsedQuery(parsedQuery);
                }
            } else if (token.isValue()) {
                if ("query_weight".equals(fieldName)) {
                    rescoreContext.setQueryWeight(parser.floatValue());
                } else if ("rescore_query_weight".equals(fieldName)) {
                    rescoreContext.setRescoreQueryWeight(parser.floatValue());
                } else if ("score_mode".equals(fieldName)) {
                    String sScoreMode = parser.text();
                    if ("avg".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Avg);
                    } else if ("max".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Max);
                    } else if ("min".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Min);
                    } else if ("total".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Total);
                    } else if ("multiply".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Multiply);
                    } else {
                        throw new ElasticsearchIllegalArgumentException("[rescore] illegal score_mode [" + sScoreMode + "]");
                    }
                } else if ("normalize".equals(fieldName)) {
                    rescoreContext.setNormalize(parser.booleanValue());
                } else {
                    throw new ElasticsearchIllegalArgumentException("rescore doesn't support [" + fieldName + "]");
                }
            }
        }
        return rescoreContext;
    }

    private final static Comparator<ScoreDoc> SCORE_DOC_COMPARATOR = new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc o1, ScoreDoc o2) {
            int cmp = Float.compare(o2.score, o1.score);
            return cmp == 0 ?  Integer.compare(o1.doc, o2.doc) : cmp;
        }
    };

    /** Returns a new {@link TopDocs} with the topN from the incoming one, or the same TopDocs if the number of hits is already <=
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


    public HashMap<Integer, Float> rescoreElastic(IndexSearcher searcher, TopDocs firstPassTopDocs, Query query) throws IOException {
        ScoreDoc[] topByID = (ScoreDoc[])firstPassTopDocs.scoreDocs.clone();
        //Scorer returns results by document ID, when looping through, there is no Random Access so scorer.advance(int) has to be the next
        // ID in the TopDocs
        Arrays.sort(topByID, new Comparator<ScoreDoc>() {
            public int compare(ScoreDoc a, ScoreDoc b) {
                return a.doc - b.doc;
            }
        });
        HashMap<Integer, Float> hits = new HashMap<Integer, Float>();
        List leaves = searcher.getIndexReader().leaves();
        Weight weight = searcher.createNormalizedWeight(query);
        int hitUpto = 0;
        int readerUpto = -1;
        int endDoc = 0;
        int docBase = 0;
        for(Scorer scorer = null; hitUpto < topByID.length; ++hitUpto) {
            int docID = topByID[hitUpto].doc;

            LeafReaderContext readerContext;
            for(readerContext = null; docID >= endDoc; endDoc = readerContext.docBase + readerContext.reader().maxDoc()) {
                ++readerUpto;
                readerContext = (LeafReaderContext)leaves.get(readerUpto);
            }

            if(readerContext != null) {
                docBase = readerContext.docBase;
                scorer = weight.scorer(readerContext, (Bits)null);
            }

            if(scorer != null) {
                int targetDoc = docID - docBase;
                int actualDoc = scorer.docID();
                if(actualDoc < targetDoc) {
                    actualDoc = scorer.advance(targetDoc);
                }

                if(actualDoc == targetDoc) {
                    hits.put(docID, scorer.score());
                } else {
                    assert actualDoc > targetDoc;
                }
            }
        }
        return hits;
    }

    /** Modifies incoming TopDocs (in) by replacing the top hits with resorted's hits, and then resorting all hits. */
    private TopDocs combine(TopDocs in, HashMap<Integer, Float> resorted, QueryRescoreContext ctx) {
        float bottomScore = 0;
        float denominator = 0;

        if (resorted.size() != 0 && ctx.normalize)
        {
            float topScore = Collections.max(resorted.values());
            bottomScore = Collections.min(resorted.values());
            denominator = (topScore == bottomScore) ? 1 : (topScore - bottomScore);
        }

        for(int i=0;i<in.scoreDocs.length;i++) {
            if (resorted.size() != 0 && resorted.containsKey(in.scoreDocs[i].doc))
            {
                float score = (!ctx.normalize) ? resorted.get(in.scoreDocs[i].doc) :
                                                ((resorted.get(in.scoreDocs[i].doc) - bottomScore) / denominator);
                in.scoreDocs[i].score = ctx.scoreMode.combine(in.scoreDocs[i].score * ctx.queryWeight(), score * ctx.rescoreQueryWeight());
            }
            else
            {
                in.scoreDocs[i].score *= ctx.queryWeight();
            }
        }

        // TODO: this is wrong, i.e. we are comparing apples and oranges at this point.  It would be better if we always rescored all
        // incoming first pass hits, instead of allowing recoring of just the top subset:
        Arrays.sort(in.scoreDocs, SCORE_DOC_COMPARATOR);

        return in;
    }

    public static class QueryRescoreContext extends RescoreSearchContext {

        public QueryRescoreContext(QueryRescorer rescorer) {
            super(NAME, 10, rescorer);
            this.scoreMode = ScoreMode.Total;
        }

        private ParsedQuery parsedQuery;
        private float queryWeight = 1.0f;
        private float rescoreQueryWeight = 1.0f;
        private ScoreMode scoreMode;
        private boolean normalize;


        public void setParsedQuery(ParsedQuery parsedQuery) {
            this.parsedQuery = parsedQuery;
        }

        public Query query() {
            return parsedQuery.query();
        }

        public float queryWeight() {
            return queryWeight;
        }

        public float rescoreQueryWeight() {
            return rescoreQueryWeight;
        }

        public ScoreMode scoreMode() {
            return scoreMode;
        }

        public boolean normalize() {
            return normalize;
        }

        public void setRescoreQueryWeight(float rescoreQueryWeight) {
            this.rescoreQueryWeight = rescoreQueryWeight;
        }

        public void setQueryWeight(float queryWeight) {
            this.queryWeight = queryWeight;
        }

        public void setScoreMode(ScoreMode scoreMode) {
            this.scoreMode = scoreMode;
        }

        public void setNormalize(boolean normalize) {
            this.normalize = normalize;
        }
    }

    @Override
    public void extractTerms(SearchContext context, RescoreSearchContext rescoreContext, Set<Term> termsSet) {
        ((QueryRescoreContext) rescoreContext).query().extractTerms(termsSet);
    }

}
