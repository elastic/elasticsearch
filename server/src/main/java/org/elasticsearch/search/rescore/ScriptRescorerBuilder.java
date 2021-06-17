/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.rescore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.rescore.ScriptRescorer.ScriptRescoreContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ScriptRescorerBuilder extends RescorerBuilder<ScriptRescorerBuilder> {
    public static final String NAME = "script";
    private static final ParseField SCRIPT_FIELD = new ParseField("script");
    private final Script script;

    public ScriptRescorerBuilder(Script script) {
        this.script = script;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        script.writeTo(out);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(SCRIPT_FIELD.getPreferredName(), script);
        builder.endObject();
    }

    public static ScriptRescorerBuilder fromXContent(XContentParser parser) throws IOException {
        Script script = Script.parse(parser);
        return new ScriptRescorerBuilder(script);
    }

    @Override
    protected RescoreContext innerBuildContext(int windowSize, SearchExecutionContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException("[script] queries cannot be executed when '" +
                    SearchService.ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false.");
        }
        ScoreScript.Factory factory = context.compile(script, ScoreScript.CONTEXT);
        ScoreScript.LeafFactory scoreScriptFactory = factory.newFactory(script.getParams(), context.lookup());

        ScriptQuery query = new ScriptQuery(script, scoreScriptFactory, context.index().getName(),
                context.getShardId(), context.indexVersionCreated());

        ScriptRescoreContext scriptRescoreContext = new ScriptRescoreContext(windowSize);
        scriptRescoreContext.setQuery(query);
        scriptRescoreContext.setNeedsScores(scoreScriptFactory.needs_score());
        return scriptRescoreContext;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public RescorerBuilder<ScriptRescorerBuilder> rewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return this;
    }

    public static class ScriptQuery extends Query {

        private final Script script;
        private final ScoreScript.LeafFactory scriptBuilder;
        private final String indexName;
        private final int shardId;
        private final Version indexVersion;
        private Map<Integer, Float> hitsMap;

        ScriptQuery(Script script, ScoreScript.LeafFactory scriptBuilder, String indexName, int shardId, Version indexVersion) {
            this.script = script;
            this.scriptBuilder = scriptBuilder;
            this.indexName = indexName;
            this.shardId = shardId;
            this.indexVersion = indexVersion;
        }

        @Override
        public String toString(String field) {
            StringBuilder buffer = new StringBuilder();
            buffer.append("ScriptQuery(");
            buffer.append(script);
            buffer.append(")");
            return buffer.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (sameClassAs(obj) == false) {
                return false;
            }
            ScriptQuery other = (ScriptQuery) obj;
            return Objects.equals(script, other.script);
        }

        @Override
        public int hashCode() {
            int h = classHash();
            h = 31 * h + script.hashCode();
            return h;
        }

        public void setHits(ScoreDoc[] hits) {
            hitsMap = new HashMap<>();
            for (ScoreDoc hit : hits) {
                hitsMap.put(hit.doc, hit.score);
            }
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            boolean needsScore = scriptBuilder.needs_score();
            ScoreMode queryScoreMode = needsScore ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
            return new ScriptWeight(this, needsScore, queryScoreMode);
        }

        // script weight
        private class ScriptWeight extends Weight {
            boolean needsScore;
            ScoreMode queryScoreMode;

            ScriptWeight(Query query, boolean needsScore, ScoreMode queryScoreMode) {
                super(query);
                this.needsScore = needsScore;
                this.queryScoreMode = queryScoreMode;
            }

            @Override
            public void extractTerms(Set<Term> terms) {

            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                final ScoreScript.ExplanationHolder explanationHolder = new ScoreScript.ExplanationHolder();
                final ScoreAndDoc scorable = new ScoreAndDoc();

                Scorer scorer = new ScriptScorer(this, context, makeScoreScript(context),
                        queryScoreMode, scorable, explanationHolder);

                int newDoc = scorer.iterator().advance(doc);
                assert doc == newDoc; // subquery should have already matched above
                float score = scorer.score(); // score without boost

                Explanation explanation = explanationHolder.get(score, null);
                if (explanation == null) {
                    // no explanation provided by user; give a simple one
                    String desc = "script score function, computed with script:\"" + script + "\"";
                    explanation = Explanation.match(score, desc);
                }
                return explanation;
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final ScoreAndDoc scorable = new ScoreAndDoc();
                Scorer scriptScorer = new ScriptScorer(this, context, makeScoreScript(context),
                        queryScoreMode, scorable, null);
                return scriptScorer;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            private ScoreScript makeScoreScript(LeafReaderContext context) throws IOException {
                final ScoreScript scoreScript = scriptBuilder.newInstance(context);
                scoreScript._setIndexName(indexName);
                scoreScript._setShard(shardId);
                return scoreScript;
            }
        }

        // script scorer
        private class ScriptScorer extends Scorer {
            private final LeafReaderContext context;
            private final DocIdSetIterator disi;
            private final ScoreScript scoreScript;
            private final ScoreAndDoc queryScorer;
            private final ScoreScript.ExplanationHolder explanation;

            ScriptScorer(Weight weight, LeafReaderContext context, ScoreScript scoreScript,
                         ScoreMode queryScoreMode, ScoreAndDoc scorable, ScoreScript.ExplanationHolder explanation) {
                super(weight);
                this.context = context;
                this.disi = DocIdSetIterator.all(context.reader().maxDoc());;
                this.scoreScript = scoreScript;
                this.explanation = explanation;
                this.queryScorer = scorable;
                if (queryScoreMode == ScoreMode.COMPLETE) {
                    scoreScript.setScorer(scorable);
                }
            }

            @Override
            public float score() throws IOException {
                int docId = docID();
                if (scriptBuilder.needs_score() && hitsMap != null &&  hitsMap.containsKey(context.docBase + docId)) {
                    queryScorer.doc = docId;
                    queryScorer.score = hitsMap.get(context.docBase + docId);
                }
                scoreScript.setDocument(docId);
                float score = (float) scoreScript.execute(explanation);
                if (score < 0f || Float.isNaN(score)) {
                    throw new IllegalArgumentException("rescorer script returned an invalid score [" + score + "] " +
                            "for doc [" + docId + "]. Must be a non-negative score!");
                }
                return score;
            }

            @Override
            public int docID() {
                return disi.docID();
            }

            @Override
            public DocIdSetIterator iterator() {
                return disi;
            }

            @Override
            public float getMaxScore(int upTo) {
                return Float.MAX_VALUE;
            }
        }

        private class ScoreAndDoc extends Scorable {
            float score;
            int doc = -1;

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public float score() {
                return score;
            }
        }

    }
}
