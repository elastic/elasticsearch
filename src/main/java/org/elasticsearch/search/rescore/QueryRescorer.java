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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
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
    public TopDocs rescore(TopDocs topDocs, SearchContext context, final RescoreSearchContext rescoreContext) throws IOException {
        assert rescoreContext != null;
        if (topDocs == null || topDocs.totalHits == 0 || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }
        org.apache.lucene.search.Rescorer rescorer = newRescorer((QueryRescoreContext) rescoreContext);
        if (rescoreContext.window() < topDocs.scoreDocs.length) {
            ScoreDoc[] subset = new ScoreDoc[rescoreContext.window()];
            System.arraycopy(topDocs.scoreDocs, 0, subset, 0, rescoreContext.window());
            final TopDocs rescore = rescorer.rescore(context.searcher(), new TopDocs(topDocs.totalHits, subset, topDocs.getMaxScore()), rescoreContext.window());
            return combine(topDocs, rescore, (QueryRescoreContext) rescoreContext);
        }
        return rescorer.rescore(context.searcher(), topDocs, Math.max(topDocs.scoreDocs.length, rescoreContext.window()));
    }

    // THIS RETRUNS BOGUS explains
//    @Override
//    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext,
//            Explanation sourceExplanation) throws IOException {
//        if (sourceExplanation == null) {
//            // this should not happen but just in case
//            return new ComplexExplanation(false, 0.0f, "nothing matched");
//        }
//        return newRescorer((QueryRescoreContext) rescoreContext).explain(context.searcher(), sourceExplanation, topLevelDocId);
//    }


    @Override
    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext,
                               Explanation sourceExplanation) throws IOException {
        QueryRescoreContext rescore = ((QueryRescoreContext) rescoreContext);
        ContextIndexSearcher searcher = context.searcher();
        if (sourceExplanation == null) {
            // this should not happen but just in case
            return new ComplexExplanation(false, 0.0f, "nothing matched");
        }
        Explanation rescoreExplain = searcher.explain(rescore.query(), topLevelDocId);
        float primaryWeight = rescore.queryWeight();
        ComplexExplanation prim = new ComplexExplanation(sourceExplanation.isMatch(),
                sourceExplanation.getValue() * primaryWeight,
                "product of:");
        prim.addDetail(sourceExplanation);
        prim.addDetail(new Explanation(primaryWeight, "primaryWeight"));
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

    private org.apache.lucene.search.Rescorer newRescorer(final QueryRescoreContext ctx) {
        return new org.apache.lucene.search.QueryRescorer(ctx.query()) {

            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                if (secondPassMatches) {
                    return ctx.scoreMode.combine(firstPassScore * ctx.queryWeight(), secondPassScore * ctx.rescoreQueryWeight());
                }
                return firstPassScore * ctx.queryWeight();
            }
        };
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
                } else {
                    throw new ElasticsearchIllegalArgumentException("rescore doesn't support [" + fieldName + "]");
                }
            }
        }
        return rescoreContext;
    }
    private final static Comparator<ScoreDoc> DOC_ID_COMPARATOR = new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc o1, ScoreDoc o2) {
            return Integer.compare(o1.doc, o2.doc);
        }
    };

    private final static Comparator<ScoreDoc> SCORE_DOC_COMPARATOR = new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc o1, ScoreDoc o2) {
            int cmp = Float.compare(o2.score, o1.score);
            return cmp == 0 ?  Integer.compare(o1.doc, o2.doc) : cmp;
        }
    };

    private TopDocs combine(TopDocs primary, TopDocs secondary, QueryRescoreContext context) {
        ScoreDoc[] secondaryDocs = secondary.scoreDocs;
        ScoreDoc[] primaryDocs = primary.scoreDocs;
        Arrays.sort(primary.scoreDocs, DOC_ID_COMPARATOR);
        Arrays.sort(secondary.scoreDocs, DOC_ID_COMPARATOR);
        int j = 0;
        for (int i = 0; i < primaryDocs.length; i++) {
            if (j < secondaryDocs.length && primaryDocs[i].doc == secondaryDocs[j].doc) {
                primaryDocs[i].score =  secondaryDocs[j++].score;
            }
        }
        Arrays.sort(primaryDocs, SCORE_DOC_COMPARATOR);
        primary.setMaxScore(primaryDocs[0].score);
        return primary;
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

        public void setRescoreQueryWeight(float rescoreQueryWeight) {
            this.rescoreQueryWeight = rescoreQueryWeight;
        }

        public void setQueryWeight(float queryWeight) {
            this.queryWeight = queryWeight;
        }

        public void setScoreMode(ScoreMode scoreMode) {
            this.scoreMode = scoreMode;
        }

    }

    @Override
    public void extractTerms(SearchContext context, RescoreSearchContext rescoreContext, Set<Term> termsSet) {
        ((QueryRescoreContext) rescoreContext).query().extractTerms(termsSet);
    }

}
