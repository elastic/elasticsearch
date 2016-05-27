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

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.script.ExplainableSearchScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * A query that allows for a pluggable boost function / filter. If it matches
 * the filter, it will be boosted by the formula.
 */
public class FiltersFunctionScoreQuery extends Query {

    public static class FilterFunction {
        public final Query filter;
        public final ScoreFunction function;
        public final String varName;
        public final Float noMatchScore;

        public FilterFunction(Query filter, ScoreFunction function) {
            this(filter, function, null, null);
        }

        public FilterFunction(Query filter, ScoreFunction function, String varName, Float noMatchScore) {
            this.filter = filter;
            this.function = function;
            this.varName = varName;
            this.noMatchScore = noMatchScore;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FilterFunction that = (FilterFunction) o;
            return Objects.equals(this.filter, that.filter) &&
                Objects.equals(this.function, that.function) &&
                Objects.equals(this.varName, that.varName) &&
                Objects.equals(this.noMatchScore, that.noMatchScore) ;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), filter, function, varName, noMatchScore);
        }
    }

    static final class CannedScorer extends Scorer {
        protected int docid;
        protected float score;

        public CannedScorer() {
            super(null);
        }

        public CannedScorer(float score) {
            super(null);
            this.score = score;
        }

        @Override
        public int docID() {
            return docid;
        }

        @Override
        public float score() throws IOException {
            return score;
        }

        @Override
        public int freq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocIdSetIterator iterator() {
            throw new UnsupportedOperationException();
        }
    }

    public static class ScoreScript {
        private final Script sScript;
        private final SearchScript script;

        public ScoreScript(Script sScript, SearchScript script) {
            this.sScript = sScript;
            this.script = script;
        }

        public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
            LeafSearchScript leafSearchScript = script.getLeafSearchScript(context);
            return leafSearchScript;
        }
    }
    public enum ScoreMode implements Writeable {
        FIRST, AVG, MAX, SUM, MIN, MULTIPLY, SCRIPT;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal());
        }

        public static ScoreMode readFromStream(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown ScoreMode ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        public static ScoreMode fromString(String scoreMode) {
            return valueOf(scoreMode.toUpperCase(Locale.ROOT));
        }
    }

    final Query subQuery;
    final FilterFunction[] filterFunctions;
    final ScoreMode scoreMode;
    final ScoreScript scoreScript;
    final float maxBoost;

    private final Float minScore;

    protected final CombineFunction combineFunction;

    public FiltersFunctionScoreQuery(Query subQuery, ScoreMode scoreMode, FilterFunction[] filterFunctions, float maxBoost, Float minScore, CombineFunction combineFunction) {
        this(subQuery,scoreMode, null, filterFunctions, maxBoost, minScore, combineFunction);
    }

    public FiltersFunctionScoreQuery(Query subQuery, ScoreMode scoreMode, ScoreScript searchScript, FilterFunction[] filterFunctions,
                                     float maxBoost, Float minScore, CombineFunction combineFunction) {
        this.subQuery = subQuery;
        this.scoreMode = scoreMode;
        this.scoreScript = searchScript;
        this.filterFunctions = filterFunctions;
        this.maxBoost = maxBoost;
        this.combineFunction = combineFunction;
        this.minScore = minScore;
    }

    public Query getSubQuery() {
        return subQuery;
    }

    public FilterFunction[] getFilterFunctions() {
        return filterFunctions;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = super.rewrite(reader);
        if (rewritten != this) {
            return rewritten;
        }
        Query newQ = subQuery.rewrite(reader);
        if (newQ == subQuery)
            return this;
        return new FiltersFunctionScoreQuery(newQ, scoreMode, filterFunctions, maxBoost, minScore, combineFunction);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        if (needsScores == false && minScore == null) {
            return subQuery.createWeight(searcher, needsScores);
        }

        boolean subQueryNeedsScores = combineFunction != CombineFunction.REPLACE;
        Weight[] filterWeights = new Weight[filterFunctions.length];
        for (int i = 0; i < filterFunctions.length; ++i) {
            subQueryNeedsScores |= filterFunctions[i].function.needsScores();
            filterWeights[i] = searcher.createNormalizedWeight(filterFunctions[i].filter, false);
        }
        Weight subQueryWeight = subQuery.createWeight(searcher, subQueryNeedsScores);
        return new CustomBoostFactorWeight(this, subQueryWeight, filterWeights, scoreScript, subQueryNeedsScores);
    }

    class CustomBoostFactorWeight extends Weight {

        final Weight subQueryWeight;
        final Weight[] filterWeights;
        final ScoreScript scoreScript;
        final boolean needsScores;

        public CustomBoostFactorWeight(Query parent, Weight subQueryWeight, Weight[] filterWeights, ScoreScript scoreScript, boolean needsScores)
            throws IOException {
            super(parent);
            this.subQueryWeight = subQueryWeight;
            this.filterWeights = filterWeights;
            this.scoreScript = scoreScript;
            this.needsScores = needsScores;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            subQueryWeight.extractTerms(terms);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return subQueryWeight.getValueForNormalization();
        }

        @Override
        public void normalize(float norm, float boost) {
            subQueryWeight.normalize(norm, boost);
        }

        private FiltersFunctionFactorScorer functionScorer(LeafReaderContext context) throws IOException {
            Scorer subQueryScorer = subQueryWeight.scorer(context);
            if (subQueryScorer == null) {
                return null;
            }
            final LeafScoreFunction[] functions = new LeafScoreFunction[filterFunctions.length];
            final Bits[] docSets = new Bits[filterFunctions.length];
            for (int i = 0; i < filterFunctions.length; i++) {
                FilterFunction filterFunction = filterFunctions[i];
                functions[i] = filterFunction.function.getLeafScoreFunction(context);
                Scorer filterScorer = filterWeights[i].scorer(context);
                docSets[i] = Lucene.asSequentialAccessBits(context.reader().maxDoc(), filterScorer);
            }
            LeafSearchScript leafScoreScript = null;
            CannedScorer cannedScorer = new CannedScorer();
            if (scoreScript != null) {
                leafScoreScript = scoreScript.getLeafSearchScript(context);
                leafScoreScript.setScorer(cannedScorer);
            }

            return new FiltersFunctionFactorScorer(this, subQueryScorer, scoreMode, leafScoreScript, cannedScorer, filterFunctions, maxBoost,
                functions, docSets, combineFunction, needsScores);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer scorer = functionScorer(context);
            if (scorer != null && minScore != null) {
                scorer = new MinScoreScorer(this, scorer, minScore);
            }
            return scorer;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {

            LeafSearchScript leafScript = null;
            if (scoreMode.equals(ScoreMode.SCRIPT)) {
                leafScript = scoreScript.getLeafSearchScript(context);
                leafScript.setDocument(doc);
            }
            Explanation expl = subQueryWeight.explain(context, doc);
            if (!expl.isMatch()) {
                return expl;
            }
            // First: Gather explanations for all filters
            List<Explanation> filterExplanations = getFilterFunctionsExplanations(context, doc, leafScript, expl);

            expl = getFunctionScoreExplanation(context, doc, leafScript, expl, filterExplanations);
            return expl;
        }

        private Explanation getFunctionScoreExplanation(LeafReaderContext context, int doc, LeafSearchScript leafScript, Explanation expl, List<Explanation> filterExplanations) throws IOException {
            FiltersFunctionFactorScorer scorer = functionScorer(context);
            int actualDoc = scorer.iterator().advance(doc);
            assert (actualDoc == doc);
            double score = scorer.computeScore(doc, expl.getValue());
            Explanation factorExplanation;
            if (filterExplanations.size() > 0) {
                String scoreModeExplanation = "[" + scoreMode.toString().toLowerCase(Locale.ROOT);
                if (scoreMode.equals(ScoreMode.SCRIPT)) {
                    leafScript.setScorer(new CannedScorer(expl.getValue()));
                    if (leafScript instanceof ExplainableSearchScript) {
                        leafScript.setDocument(doc);
                        scoreModeExplanation += ":" + ((ExplainableSearchScript) leafScript).explain(expl).toString();
                    } else {
                        double scoreModeScore = scorer.getScriptFactor(doc, expl.getValue());
                        String explanation = ":\"" + scoreScript.sScript + "\"";
                        if (scoreScript.sScript.getParams() != null) {
                            explanation += " and parameters: \n" + scoreScript.sScript.getParams().toString();
                        }
                        Explanation scoreExp = Explanation.match(
                            expl.getValue(), "_score: ",
                            expl);
                        scoreModeExplanation += Explanation.match(
                            CombineFunction.toFloat(scoreModeScore), explanation,
                            scoreExp).toString();
                    }
                }
                scoreModeExplanation += "]";
                factorExplanation = Explanation.match(
                    CombineFunction.toFloat(score), "function score, score mode " + scoreModeExplanation, filterExplanations);
            } else {
                // it is a little weird to add a match although no function matches but that is the way function_score behaves right now
                factorExplanation = Explanation.match(1.0f,
                    "No function matched", Collections.emptyList());
            }
            expl = combineFunction.explain(expl, factorExplanation, maxBoost);
            if (minScore != null && minScore > expl.getValue()) {
                expl = Explanation.noMatch("Score value is too low, expected at least " + minScore + " but got " + expl.getValue(), expl);
            }
            return expl;
        }

        private List<Explanation> getFilterFunctionsExplanations(LeafReaderContext context, int doc, LeafSearchScript leafScript,
                                                                 Explanation expl) throws IOException {
            List<Explanation> filterExplanations = new ArrayList<>();
            for (int i = 0; i < filterFunctions.length; ++i) {
                // figure out if it matches or not
                Bits docSet = Lucene.asSequentialAccessBits(context.reader().maxDoc(),
                    filterWeights[i].scorer(context));
                if (docSet.get(doc)) {
                    FilterFunction filterFunction = filterFunctions[i];
                    Explanation functionExplanation = filterFunction.function.getLeafScoreFunction(context).explainScore(doc, expl);
                    double factor = functionExplanation.getValue();
                    float sc = CombineFunction.toFloat(factor);
                    Explanation filterExplanation = Explanation.match(sc, "function score" + (scoreMode.equals(ScoreMode.SCRIPT) ?
                            " (var_name: " + filterFunction.varName + ")" : "") + ", product of:",
                        Explanation.match(1.0f, "match filter: " + filterFunction.filter.toString()),
                        functionExplanation);
                    filterExplanations.add(filterExplanation);
                    if (scoreMode.equals(ScoreMode.SCRIPT)) {
                        leafScript.setNextVar(filterFunction.varName, filterExplanation.getValue());
                    }
                } else if (filterFunctions[i].noMatchScore != null) {
                    filterExplanations.add(Explanation.match(filterFunctions[i].noMatchScore, "no_match_score" + (filterFunctions[i]
                        .varName != null ? "(var_name: " + filterFunctions[i].varName + ")" : "") + filterFunctions[i].noMatchScore
                        + " (filter did not match)"));
                }
            }
            return filterExplanations;
        }
    }

    static class FiltersFunctionFactorScorer extends FilterScorer {
        private CannedScorer cannedScorer;
        private final FilterFunction[] filterFunctions;
        private final ScoreMode scoreMode;
        private final LeafSearchScript scoreScript;
        private final LeafScoreFunction[] functions;
        private final Bits[] docSets;
        private final CombineFunction scoreCombiner;
        private final float maxBoost;
        private final boolean needsScores;

        private FiltersFunctionFactorScorer(CustomBoostFactorWeight w, Scorer scorer, ScoreMode scoreMode, LeafSearchScript scoreScript,
                CannedScorer cannedScorer, FilterFunction[] filterFunctions, float maxBoost, LeafScoreFunction[] functions, Bits[] docSets,
                CombineFunction scoreCombiner, boolean needsScores) throws IOException {
            super(scorer, w);
            this.scoreMode = scoreMode;
            this.scoreScript = scoreScript;
            this.cannedScorer = cannedScorer;
            this.filterFunctions = filterFunctions;
            this.functions = functions;
            this.docSets = docSets;
            this.scoreCombiner = scoreCombiner;
            this.maxBoost = maxBoost;
            this.needsScores = needsScores;
        }

        @Override
        public float score() throws IOException {
            int docId = docID();
            // Even if the weight is created with needsScores=false, it might
            // be costly to call score(), so we explicitly check if scores
            // are needed
            float subQueryScore = needsScores ? super.score() : 0f;

            double factor = computeScore(docId, subQueryScore);
            return scoreCombiner.combine(subQueryScore, factor, maxBoost);
        }

        protected double computeScore(int docId, float subQueryScore) {
            double factor = 1d;
            switch(scoreMode) {
                case FIRST:
                    for (int i = 0; i < filterFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            factor = functions[i].score(docId, subQueryScore);
                            break;
                        }
                    }
                    break;
                case MAX:
                    double maxFactor = Double.NEGATIVE_INFINITY;
                    for (int i = 0; i < filterFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            maxFactor = Math.max(functions[i].score(docId, subQueryScore), maxFactor);
                        }
                    }
                    if (maxFactor != Float.NEGATIVE_INFINITY) {
                        factor = maxFactor;
                    }
                    break;
                case MIN:
                    double minFactor = Double.POSITIVE_INFINITY;
                    for (int i = 0; i < filterFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            minFactor = Math.min(functions[i].score(docId, subQueryScore), minFactor);
                        }
                    }
                    if (minFactor != Float.POSITIVE_INFINITY) {
                        factor = minFactor;
                    }
                    break;
                case MULTIPLY:
                    for (int i = 0; i < filterFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            factor *= functions[i].score(docId, subQueryScore);
                        }
                    }
                    break;
                case SCRIPT:
                    factor = getScriptFactor(docId, subQueryScore);
                    break;
                default: // Avg / Total
                    double totalFactor = 0.0f;
                    double weightSum = 0;
                    for (int i = 0; i < filterFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            totalFactor += functions[i].score(docId, subQueryScore);
                            if (filterFunctions[i].function instanceof WeightFactorFunction) {
                                weightSum += ((WeightFactorFunction) filterFunctions[i].function).getWeight();
                            } else {
                                weightSum += 1.0;
                            }
                        }
                    }
                    if (weightSum != 0) {
                        factor = totalFactor;
                        if (scoreMode == ScoreMode.AVG) {
                            factor /= weightSum;
                        }
                    }
                    break;
            }
            return factor;
        }

        double getScriptFactor(int docId, float subQueryScore) {
            double factor;
            for (int i = 0; i < filterFunctions.length; i++) {
                if (docSets[i].get(docId)) {
                    scoreScript.setNextVar(filterFunctions[i].varName, functions[i].score(docId, subQueryScore));
                } else if (filterFunctions[i].noMatchScore != null) {
                    scoreScript.setNextVar(filterFunctions[i].varName,filterFunctions[i].noMatchScore);
                }
            }
            scoreScript.setDocument(docId);
            cannedScorer.docid = docId;
            cannedScorer.score = subQueryScore;
            factor = scoreScript.runAsDouble();
            return factor;
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("function score (").append(subQuery.toString(field)).append(", functions: [");
        for (FilterFunction filterFunction : filterFunctions) {
            sb.append("{filter(").append(filterFunction.filter).append("), function [").append(filterFunction.function).append("]}");
        }
        sb.append("])");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        FiltersFunctionScoreQuery other = (FiltersFunctionScoreQuery) o;
        return Objects.equals(this.subQuery, other.subQuery) && this.maxBoost == other.maxBoost &&
            Objects.equals(this.combineFunction, other.combineFunction) && Objects.equals(this.minScore, other.minScore) &&
            Objects.equals(this.scoreMode, other.scoreMode) &&
            Arrays.equals(this.filterFunctions, other.filterFunctions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), subQuery, maxBoost, combineFunction, minScore, scoreMode, Arrays.hashCode(filterFunctions));
    }
}
