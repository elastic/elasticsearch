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

package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RandomApproximationQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.FilterFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.ScoreMode;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionBuilder;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class FunctionScoreTests extends ESTestCase {

    private static final String UNSUPPORTED = "Method not implemented. This is just a stub for testing.";


    /**
     * Stub for IndexFieldData. Needed by some score functions. Returns 1 as count always.
     */
    private static class IndexFieldDataStub implements IndexFieldData<AtomicFieldData> {
        @Override
        public String getFieldName() {
            return "test";
        }

        @Override
        public FieldDataType getFieldDataType() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public AtomicFieldData load(LeafReaderContext context) {
            return new AtomicFieldData() {

                @Override
                public ScriptDocValues getScriptValues() {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public SortedBinaryDocValues getBytesValues() {
                    return new SortedBinaryDocValues() {
                        @Override
                        public void setDocument(int docId) {
                        }

                        @Override
                        public int count() {
                            return 1;
                        }

                        @Override
                        public BytesRef valueAt(int index) {
                            return new BytesRef("0");
                        }
                    };
                }

                @Override
                public long ramBytesUsed() {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public Collection<Accountable> getChildResources() {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public AtomicFieldData loadDirect(LeafReaderContext context) throws Exception {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public IndexFieldData.XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, IndexFieldData.XFieldComparatorSource.Nested nested) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public Index index() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }
    }

    /**
     * Stub for IndexNumericFieldData needed by some score functions. Returns 1 as value always.
     */
    private static class IndexNumericFieldDataStub implements IndexNumericFieldData {

        @Override
        public NumericType getNumericType() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public String getFieldName() {
            return "test";
        }

        @Override
        public FieldDataType getFieldDataType() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public AtomicNumericFieldData load(LeafReaderContext context) {
            return new AtomicNumericFieldData() {
                @Override
                public SortedNumericDocValues getLongValues() {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public SortedNumericDoubleValues getDoubleValues() {
                    return new SortedNumericDoubleValues() {
                        @Override
                        public void setDocument(int doc) {
                        }

                        @Override
                        public double valueAt(int index) {
                            return 1;
                        }

                        @Override
                        public int count() {
                            return 1;
                        }
                    };
                }

                @Override
                public ScriptDocValues getScriptValues() {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public SortedBinaryDocValues getBytesValues() {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public long ramBytesUsed() {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public Collection<Accountable> getChildResources() {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public AtomicNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public Index index() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }
    }

    private static final ScoreFunction RANDOM_SCORE_FUNCTION = new RandomScoreFunction(0, 0, new IndexFieldDataStub());
    private static final ScoreFunction FIELD_VALUE_FACTOR_FUNCTION = new FieldValueFactorFunction("test", 1, FieldValueFactorFunction.Modifier.LN, new Double(1), null);
    private static final ScoreFunction GAUSS_DECAY_FUNCTION = new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, GaussDecayFunctionBuilder.GAUSS_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX);
    private static final ScoreFunction EXP_DECAY_FUNCTION = new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, ExponentialDecayFunctionBuilder.EXP_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX);
    private static final ScoreFunction LIN_DECAY_FUNCTION = new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, LinearDecayFunctionBuilder.LINEAR_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX);
    private static final ScoreFunction WEIGHT_FACTOR_FUNCTION = new WeightFactorFunction(4);
    private static final String TEXT = "The way out is through.";
    private static final String FIELD = "test";
    private static final Term TERM = new Term(FIELD, "through");
    private Directory dir;
    private IndexWriter w;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    @Before
    public void initSearcher() throws IOException {
        dir = newDirectory();
        w = new IndexWriter(dir, newIndexWriterConfig(new StandardAnalyzer()));
        Document d = new Document();
        d.add(new TextField(FIELD, TEXT, Field.Store.YES));
        d.add(new TextField("_uid", "1", Field.Store.YES));
        w.addDocument(d);
        w.commit();
        reader = DirectoryReader.open(w, true);
        searcher = newSearcher(reader);
    }

    @After
    public void closeAllTheReaders() throws IOException {
        reader.close();
        w.close();
        dir.close();
    }

    public void testExplainFunctionScoreQuery() throws IOException {

        Explanation functionExplanation = getFunctionScoreExplanation(searcher, RANDOM_SCORE_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "random score function (seed: 0)");
        assertThat(functionExplanation.getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, FIELD_VALUE_FACTOR_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "field value function: ln(doc['test'].value?:1.0 * factor=1.0)");
        assertThat(functionExplanation.getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, GAUSS_DECAY_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "Function for field test:");
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].toString(), equalTo("0.1 = exp(-0.5*pow(MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)],2.0)/0.21714724095162594)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, EXP_DECAY_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "Function for field test:");
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].toString(), equalTo("0.1 = exp(- MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)] * 2.3025850929940455)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, LIN_DECAY_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "Function for field test:");
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].toString(), equalTo("0.1 = max(0.0, ((1.1111111111111112 - MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)])/1.1111111111111112)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, WEIGHT_FACTOR_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "product of:");
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].toString(), equalTo("1.0 = constant score 1.0 - no function provided\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[1].toString(), equalTo("4.0 = weight\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));
        assertThat(functionExplanation.getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));
    }

    public Explanation getFunctionScoreExplanation(IndexSearcher searcher, ScoreFunction scoreFunction) throws IOException {
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(new TermQuery(TERM), scoreFunction, 0.0f, CombineFunction.AVG, 100);
        Weight weight = searcher.createNormalizedWeight(functionScoreQuery, true);
        Explanation explanation = weight.explain(searcher.getIndexReader().leaves().get(0), 0);
        return explanation.getDetails()[1];
    }

    public void checkFunctionScoreExplanation(Explanation randomExplanation, String functionExpl) {
        assertThat(randomExplanation.getDescription(), equalTo("min of:"));
        assertThat(randomExplanation.getDetails()[0].getDescription(), equalTo(functionExpl));
    }

    public void testExplainFiltersFunctionScoreQuery() throws IOException {
        Explanation functionExplanation = getFiltersFunctionScoreExplanation(searcher, RANDOM_SCORE_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "random score function (seed: 0)", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, FIELD_VALUE_FACTOR_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "field value function: ln(doc['test'].value?:1.0 * factor=1.0)", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, GAUSS_DECAY_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].toString(), equalTo("0.1 = exp(-0.5*pow(MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)],2.0)/0.21714724095162594)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, EXP_DECAY_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].toString(), equalTo("0.1 = exp(- MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)] * 2.3025850929940455)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, LIN_DECAY_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].toString(), equalTo("0.1 = max(0.0, ((1.1111111111111112 - MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)])/1.1111111111111112)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        // now test all together
        functionExplanation = getFiltersFunctionScoreExplanation(searcher
                , RANDOM_SCORE_FUNCTION
                , FIELD_VALUE_FACTOR_FUNCTION
                , GAUSS_DECAY_FUNCTION
                , EXP_DECAY_FUNCTION
                , LIN_DECAY_FUNCTION
        );

        checkFiltersFunctionScoreExplanation(functionExplanation, "random score function (seed: 0)", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));

        checkFiltersFunctionScoreExplanation(functionExplanation, "field value function: ln(doc['test'].value?:1.0 * factor=1.0)", 1);
        assertThat(functionExplanation.getDetails()[0].getDetails()[1].getDetails()[1].getDetails().length, equalTo(0));

        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 2);
        assertThat(functionExplanation.getDetails()[0].getDetails()[2].getDetails()[1].getDetails()[0].toString(), equalTo("0.1 = exp(-0.5*pow(MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)],2.0)/0.21714724095162594)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[2].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 3);
        assertThat(functionExplanation.getDetails()[0].getDetails()[3].getDetails()[1].getDetails()[0].toString(), equalTo("0.1 = exp(- MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)] * 2.3025850929940455)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[3].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 4);
        assertThat(functionExplanation.getDetails()[0].getDetails()[4].getDetails()[1].getDetails()[0].toString(), equalTo("0.1 = max(0.0, ((1.1111111111111112 - MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)])/1.1111111111111112)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[4].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));
    }

    public Explanation getFiltersFunctionScoreExplanation(IndexSearcher searcher, ScoreFunction... scoreFunctions) throws IOException {
        FiltersFunctionScoreQuery filtersFunctionScoreQuery = getFiltersFunctionScoreQuery(FiltersFunctionScoreQuery.ScoreMode.AVG, CombineFunction.AVG, scoreFunctions);
        return getExplanation(searcher, filtersFunctionScoreQuery).getDetails()[1];
    }

    protected Explanation getExplanation(IndexSearcher searcher, FiltersFunctionScoreQuery filtersFunctionScoreQuery) throws IOException {
        Weight weight = searcher.createNormalizedWeight(filtersFunctionScoreQuery, true);
        return weight.explain(searcher.getIndexReader().leaves().get(0), 0);
    }

    public FiltersFunctionScoreQuery getFiltersFunctionScoreQuery(FiltersFunctionScoreQuery.ScoreMode scoreMode, CombineFunction combineFunction, ScoreFunction... scoreFunctions) {
        FiltersFunctionScoreQuery.FilterFunction[] filterFunctions = new FiltersFunctionScoreQuery.FilterFunction[scoreFunctions.length];
        for (int i = 0; i < scoreFunctions.length; i++) {
            filterFunctions[i] = new FiltersFunctionScoreQuery.FilterFunction(
                    new TermQuery(TERM), scoreFunctions[i]);
        }
        return new FiltersFunctionScoreQuery(new TermQuery(TERM), scoreMode, filterFunctions, Float.MAX_VALUE, Float.MAX_VALUE * -1, combineFunction);
    }

    public void checkFiltersFunctionScoreExplanation(Explanation randomExplanation, String functionExpl, int whichFunction) {
        assertThat(randomExplanation.getDescription(), equalTo("min of:"));
        assertThat(randomExplanation.getDetails()[0].getDescription(), equalTo("function score, score mode [avg]"));
        assertThat(randomExplanation.getDetails()[0].getDetails()[whichFunction].getDescription(), equalTo("function score, product of:"));
        assertThat(randomExplanation.getDetails()[0].getDetails()[whichFunction].getDetails()[0].getDescription(), equalTo("match filter: " + FIELD + ":" + TERM.text()));
        assertThat(randomExplanation.getDetails()[0].getDetails()[whichFunction].getDetails()[1].getDescription(), equalTo(functionExpl));
    }

    private static float[] randomFloats(int size) {
        float[] values = new float[size];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomFloat() * (randomBoolean() ? 1.0f : -1.0f) * randomInt(100) + 1.e-5f;
        }
        return values;
    }

    private static double[] randomDoubles(int size) {
        double[] values = new double[size];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomDouble() * (randomBoolean() ? 1.0d : -1.0d) * randomInt(100) + 1.e-5d;
        }
        return values;
    }

    private static class ScoreFunctionStub extends ScoreFunction {
        private double score;

        ScoreFunctionStub(double score) {
            super(CombineFunction.REPLACE);
            this.score = score;
        }

        @Override
        public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
            return new LeafScoreFunction() {
                @Override
                public double score(int docId, float subQueryScore) {
                    return score;
                }

                @Override
                public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                    return Explanation.match((float) score, "a random score for testing");
                }
            };
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        protected boolean doEquals(ScoreFunction other) {
            return false;
        }
    }

    public void testSimpleWeightedFunction() throws IOException, ExecutionException, InterruptedException {
        int numFunctions = randomIntBetween(1, 3);
        float[] weights = randomFloats(numFunctions);
        double[] scores = randomDoubles(numFunctions);
        ScoreFunctionStub[] scoreFunctionStubs = new ScoreFunctionStub[numFunctions];
        for (int i = 0; i < numFunctions; i++) {
            scoreFunctionStubs[i] = new ScoreFunctionStub(scores[i]);
        }
        WeightFactorFunction[] weightFunctionStubs = new WeightFactorFunction[numFunctions];
        for (int i = 0; i < numFunctions; i++) {
            weightFunctionStubs[i] = new WeightFactorFunction(weights[i], scoreFunctionStubs[i]);
        }

        FiltersFunctionScoreQuery filtersFunctionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
                FiltersFunctionScoreQuery.ScoreMode.MULTIPLY
                , CombineFunction.REPLACE
                , weightFunctionStubs
        );

        TopDocs topDocsWithWeights = searcher.search(filtersFunctionScoreQueryWithWeights, 1);
        float scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double score = 1;
        for (int i = 0; i < weights.length; i++) {
            score *= weights[i] * scores[i];
        }
        assertThat(scoreWithWeight / (float) score, is(1f));
        float explainedScore = getExplanation(searcher, filtersFunctionScoreQueryWithWeights).getValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));

        filtersFunctionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
                FiltersFunctionScoreQuery.ScoreMode.SUM
                , CombineFunction.REPLACE
                , weightFunctionStubs
        );

        topDocsWithWeights = searcher.search(filtersFunctionScoreQueryWithWeights, 1);
        scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double sum = 0;
        for (int i = 0; i < weights.length; i++) {
            sum += weights[i] * scores[i];
        }
        assertThat(scoreWithWeight / (float) sum, is(1f));
        explainedScore = getExplanation(searcher, filtersFunctionScoreQueryWithWeights).getValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));

        filtersFunctionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
                FiltersFunctionScoreQuery.ScoreMode.AVG
                , CombineFunction.REPLACE
                , weightFunctionStubs
        );

        topDocsWithWeights = searcher.search(filtersFunctionScoreQueryWithWeights, 1);
        scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double norm = 0;
        sum = 0;
        for (int i = 0; i < weights.length; i++) {
            norm += weights[i];
            sum += weights[i] * scores[i];
        }
        assertThat(scoreWithWeight / (float) (sum / norm), is(1f));
        explainedScore = getExplanation(searcher, filtersFunctionScoreQueryWithWeights).getValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));

        filtersFunctionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
                FiltersFunctionScoreQuery.ScoreMode.MIN
                , CombineFunction.REPLACE
                , weightFunctionStubs
        );

        topDocsWithWeights = searcher.search(filtersFunctionScoreQueryWithWeights, 1);
        scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < weights.length; i++) {
            min = Math.min(min, weights[i] * scores[i]);
        }
        assertThat(scoreWithWeight / (float) min, is(1f));
        explainedScore = getExplanation(searcher, filtersFunctionScoreQueryWithWeights).getValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));

        filtersFunctionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
                FiltersFunctionScoreQuery.ScoreMode.MAX
                , CombineFunction.REPLACE
                , weightFunctionStubs
        );

        topDocsWithWeights = searcher.search(filtersFunctionScoreQueryWithWeights, 1);
        scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < weights.length; i++) {
            max = Math.max(max, weights[i] * scores[i]);
        }
        assertThat(scoreWithWeight / (float) max, is(1f));
        explainedScore = getExplanation(searcher, filtersFunctionScoreQueryWithWeights).getValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));
    }

    public void testWeightOnlyCreatesBoostFunction() throws IOException {
        FunctionScoreQuery filtersFunctionScoreQueryWithWeights = new FunctionScoreQuery(new MatchAllDocsQuery(), new WeightFactorFunction(2), 0.0f, CombineFunction.MULTIPLY, 100);
        TopDocs topDocsWithWeights = searcher.search(filtersFunctionScoreQueryWithWeights, 1);
        float score = topDocsWithWeights.scoreDocs[0].score;
        assertThat(score, equalTo(2.0f));
    }

    public void testMinScoreExplain() throws IOException {
        Query query = new MatchAllDocsQuery();
        Explanation queryExpl = searcher.explain(query, 0);

        FunctionScoreQuery fsq = new FunctionScoreQuery(query, null, 0f, null, Float.POSITIVE_INFINITY);
        Explanation fsqExpl = searcher.explain(fsq, 0);
        assertTrue(fsqExpl.isMatch());
        assertEquals(queryExpl.getValue(), fsqExpl.getValue(), 0f);
        assertEquals(queryExpl.getDescription(), fsqExpl.getDescription());

        fsq = new FunctionScoreQuery(query, null, 10f, null, Float.POSITIVE_INFINITY);
        fsqExpl = searcher.explain(fsq, 0);
        assertFalse(fsqExpl.isMatch());
        assertEquals("Score value is too low, expected at least 10.0 but got 1.0", fsqExpl.getDescription());

        FiltersFunctionScoreQuery ffsq = new FiltersFunctionScoreQuery(query, ScoreMode.SUM, new FilterFunction[0], Float.POSITIVE_INFINITY, 0f, CombineFunction.MULTIPLY);
        Explanation ffsqExpl = searcher.explain(ffsq, 0);
        assertTrue(ffsqExpl.isMatch());
        assertEquals(queryExpl.getValue(), ffsqExpl.getValue(), 0f);
        assertEquals(queryExpl.getDescription(), ffsqExpl.getDescription());

        ffsq = new FiltersFunctionScoreQuery(query, ScoreMode.SUM, new FilterFunction[0], Float.POSITIVE_INFINITY, 10f, CombineFunction.MULTIPLY);
        ffsqExpl = searcher.explain(ffsq, 0);
        assertFalse(ffsqExpl.isMatch());
        assertEquals("Score value is too low, expected at least 10.0 but got 1.0", ffsqExpl.getDescription());
    }

    public void testPropagatesApproximations() throws IOException {
        Query query = new RandomApproximationQuery(new MatchAllDocsQuery(), random());
        IndexSearcher searcher = newSearcher(reader);
        searcher.setQueryCache(null); // otherwise we could get a cached entry that does not have approximations

        FunctionScoreQuery fsq = new FunctionScoreQuery(query, null, null, null, Float.POSITIVE_INFINITY);
        for (boolean needsScores : new boolean[] {true, false}) {
            Weight weight = searcher.createWeight(fsq, needsScores);
            Scorer scorer = weight.scorer(reader.leaves().get(0));
            assertNotNull(scorer.twoPhaseIterator());
        }

        FiltersFunctionScoreQuery ffsq = new FiltersFunctionScoreQuery(query, ScoreMode.SUM, new FilterFunction[0], Float.POSITIVE_INFINITY, null, CombineFunction.MULTIPLY);
        for (boolean needsScores : new boolean[] {true, false}) {
            Weight weight = searcher.createWeight(ffsq, needsScores);
            Scorer scorer = weight.scorer(reader.leaves().get(0));
            assertNotNull(scorer.twoPhaseIterator());
        }
    }
}
