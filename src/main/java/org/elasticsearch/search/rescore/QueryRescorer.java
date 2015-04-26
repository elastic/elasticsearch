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
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
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
    public TopDocs rescore(TopDocs topDocs, SearchContext context, RescoreSearchContext rescoreContext) throws IOException {

        assert rescoreContext != null;
        if (topDocs == null || topDocs.totalHits == 0 || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }

        final QueryRescoreContext rescore = (QueryRescoreContext) rescoreContext;

        org.apache.lucene.search.Rescorer rescorer = new org.apache.lucene.search.QueryRescorer(rescore.query()) {

            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                if (secondPassMatches) {
                    return rescore.scoreMode.combine(firstPassScore * rescore.queryWeight(), secondPassScore * rescore.rescoreQueryWeight());
                }
                // TODO: shouldn't this be up to the ScoreMode?  I.e., we should just invoke ScoreMode.combine, passing 0.0f for the
                // secondary score?
                return firstPassScore * rescore.queryWeight();
            }
        };

        // First take top slice of incoming docs, to be rescored:
        TopDocs topNFirstPass = topN(topDocs, rescoreContext.window());

        // Rescore them:
        TopDocs rescored = rescorer.rescore(context.searcher(), topNFirstPass, rescoreContext.window());

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
            ScoreMode scoreMode = rescore.scoreMode();
            return Explanation.match(
                    scoreMode.combine(prim.getValue(), sec.getValue()),
                    scoreMode + " of:",
                    prim, sec);
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

    /** Modifies incoming TopDocs (in) by replacing the top hits with resorted's hits, and then resorting all hits. */
    private TopDocs combine(TopDocs in, TopDocs resorted, QueryRescoreContext ctx) {

        System.arraycopy(resorted.scoreDocs, 0, in.scoreDocs, 0, resorted.scoreDocs.length);
        if (in.scoreDocs.length > resorted.scoreDocs.length) {
            // These hits were not rescored (beyond the rescore window), so we treat them the same as a hit that did get rescored but did
            // not match the 2nd pass query:
            for(int i=resorted.scoreDocs.length;i<in.scoreDocs.length;i++) {
                // TODO: shouldn't this be up to the ScoreMode?  I.e., we should just invoke ScoreMode.combine, passing 0.0f for the
                // secondary score?
                in.scoreDocs[i].score *= ctx.queryWeight();
            }
            
            // TODO: this is wrong, i.e. we are comparing apples and oranges at this point.  It would be better if we always rescored all
            // incoming first pass hits, instead of allowing recoring of just the top subset:
            Arrays.sort(in.scoreDocs, SCORE_DOC_COMPARATOR);
        }
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
        try {
            context.searcher().createNormalizedWeight(((QueryRescoreContext) rescoreContext).query(), false).extractTerms(termsSet);
        } catch (IOException e) {
            throw new ElasticsearchIllegalStateException("Failed to extract terms", e);
        }
    }

}
