/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.RandomApproximationQuery;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery.FilterScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;

public class FunctionScoreTests extends ESTestCase {

    private static final String UNSUPPORTED = "Method not implemented. This is just a stub for testing.";

    /**
     * Stub for IndexFieldData. Needed by some score functions. Returns 1 as count always.
     */
    private static class IndexFieldDataStub implements IndexFieldData<LeafFieldData> {
        @Override
        public String getFieldName() {
            return "test";
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public LeafFieldData load(LeafReaderContext context) {
            return new LeafFieldData() {

                @Override
                public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public SortedBinaryDocValues getBytesValues() {
                    return new SortedBinaryDocValues() {
                        @Override
                        public boolean advanceExact(int docId) {
                            return true;
                        }

                        @Override
                        public int docValueCount() {
                            return 1;
                        }

                        @Override
                        public BytesRef nextValue() {
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
                public void close() {}
            };
        }

        @Override
        public LeafFieldData loadDirect(LeafReaderContext context) throws Exception {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public SortField sortField(
            @Nullable Object missingValue,
            MultiValueMode sortMode,
            XFieldComparatorSource.Nested nested,
            boolean reverse
        ) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public BucketedSort newBucketedSort(
            BigArrays bigArrays,
            Object missingValue,
            MultiValueMode sortMode,
            Nested nested,
            SortOrder sortOrder,
            DocValueFormat format,
            int bucketSize,
            BucketedSort.ExtraData extra
        ) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }
    }

    /**
     * Stub for IndexNumericFieldData needed by some score functions. Returns 1 as value always.
     */
    private static class IndexNumericFieldDataStub extends IndexNumericFieldData {

        @Override
        public NumericType getNumericType() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public String getFieldName() {
            return "test";
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public LeafNumericFieldData load(LeafReaderContext context) {
            return new LeafNumericFieldData() {
                @Override
                public SortedNumericDocValues getLongValues() {
                    throw new UnsupportedOperationException(UNSUPPORTED);
                }

                @Override
                public SortedNumericDoubleValues getDoubleValues() {
                    return new SortedNumericDoubleValues() {
                        @Override
                        public boolean advanceExact(int docId) {
                            return true;
                        }

                        @Override
                        public int docValueCount() {
                            return 1;
                        }

                        @Override
                        public double nextValue() {
                            return 1d;
                        }
                    };
                }

                @Override
                public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
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
                public void close() {}
            };
        }

        @Override
        public LeafNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        protected boolean sortRequiresCustomComparator() {
            return false;
        }
    }

    private static final ScoreFunction RANDOM_SCORE_FUNCTION = new RandomScoreFunction(0, 0, new IndexFieldDataStub());
    private static final ScoreFunction FIELD_VALUE_FACTOR_FUNCTION = new FieldValueFactorFunction(
        "test",
        1,
        FieldValueFactorFunction.Modifier.LN,
        1.0,
        null
    );
    private static final ScoreFunction GAUSS_DECAY_FUNCTION = new DecayFunctionBuilder.NumericFieldDataScoreFunction(
        0,
        1,
        0.1,
        0,
        GaussDecayFunctionBuilder.GAUSS_DECAY_FUNCTION,
        new IndexNumericFieldDataStub(),
        MultiValueMode.MAX
    );
    private static final ScoreFunction EXP_DECAY_FUNCTION = new DecayFunctionBuilder.NumericFieldDataScoreFunction(
        0,
        1,
        0.1,
        0,
        ExponentialDecayFunctionBuilder.EXP_DECAY_FUNCTION,
        new IndexNumericFieldDataStub(),
        MultiValueMode.MAX
    );
    private static final ScoreFunction LIN_DECAY_FUNCTION = new DecayFunctionBuilder.NumericFieldDataScoreFunction(
        0,
        1,
        0.1,
        0,
        LinearDecayFunctionBuilder.LINEAR_DECAY_FUNCTION,
        new IndexNumericFieldDataStub(),
        MultiValueMode.MAX
    );
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
        reader = DirectoryReader.open(w);
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
        checkFunctionScoreExplanation(functionExplanation, "random score function (seed: 0, field: test)");
        assertThat(functionExplanation.getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, FIELD_VALUE_FACTOR_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "field value function: ln(doc['test'].value?:1.0 * factor=1.0)");
        assertThat(functionExplanation.getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, GAUSS_DECAY_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "Function for field test:");
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[0].toString(),
            equalTo(
                "0.1 = exp(-0.5*pow(MAX[Math.max(Math.abs"
                    + "(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)],2.0)/0.21714724095162594)\n"
            )
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, EXP_DECAY_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "Function for field test:");
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[0].toString(),
            equalTo("0.1 = exp(- MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)] * 2.3025850929940455)\n")
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, LIN_DECAY_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "Function for field test:");
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[0].toString(),
            equalTo(
                "0.1 = max(0.0, ((1.1111111111111112"
                    + " - MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)])/1.1111111111111112)\n"
            )
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, WEIGHT_FACTOR_FUNCTION);
        checkFunctionScoreExplanation(functionExplanation, "product of:");
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[0].toString(),
            equalTo("1.0 = constant score 1.0 - no function provided\n")
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[1].toString(), equalTo("4.0 = weight\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));
        assertThat(functionExplanation.getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));
    }

    public Explanation getFunctionScoreExplanation(IndexSearcher searcher, ScoreFunction scoreFunction) throws IOException {
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(new TermQuery(TERM), scoreFunction, CombineFunction.AVG, 0.0f, 100);
        Weight weight = searcher.createWeight(searcher.rewrite(functionScoreQuery), org.apache.lucene.search.ScoreMode.COMPLETE, 1f);
        Explanation explanation = weight.explain(searcher.getIndexReader().leaves().get(0), 0);
        return explanation.getDetails()[1];
    }

    public void checkFunctionScoreExplanation(Explanation randomExplanation, String functionExpl) {
        assertThat(randomExplanation.getDescription(), equalTo("min of:"));
        assertThat(randomExplanation.getDetails()[0].getDescription(), containsString(functionExpl));
    }

    public void testExplainFiltersFunctionScoreQuery() throws IOException {
        Explanation functionExplanation = getFiltersFunctionScoreExplanation(searcher, RANDOM_SCORE_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "random score function (seed: 0, field: test)", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, FIELD_VALUE_FACTOR_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "field value function: ln(doc['test'].value?:1.0 * factor=1.0)", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, GAUSS_DECAY_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 0);
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].toString(),
            equalTo(
                "0.1 = "
                    + "exp(-0.5*pow(MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)],2.0)/0.21714724095162594)\n"
            )
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, EXP_DECAY_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 0);
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].toString(),
            equalTo("0.1 = exp(- MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)] * 2.3025850929940455)\n")
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, LIN_DECAY_FUNCTION);
        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 0);
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].toString(),
            equalTo(
                "0.1 = max(0.0, ((1.1111111111111112 - MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin)))"
                    + " - 0.0(=offset), 0)])/1.1111111111111112)\n"
            )
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        // now test all together
        functionExplanation = getFiltersFunctionScoreExplanation(
            searcher,
            RANDOM_SCORE_FUNCTION,
            FIELD_VALUE_FACTOR_FUNCTION,
            GAUSS_DECAY_FUNCTION,
            EXP_DECAY_FUNCTION,
            LIN_DECAY_FUNCTION
        );

        checkFiltersFunctionScoreExplanation(functionExplanation, "random score function (seed: 0, field: test)", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));

        checkFiltersFunctionScoreExplanation(functionExplanation, "field value function: ln(doc['test'].value?:1.0 * factor=1.0)", 1);
        assertThat(functionExplanation.getDetails()[0].getDetails()[1].getDetails()[1].getDetails().length, equalTo(0));

        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 2);
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[2].getDetails()[1].getDetails()[0].toString(),
            equalTo(
                "0.1 = "
                    + "exp(-0.5*pow(MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)],2.0)/0.21714724095162594)\n"
            )
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[2].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 3);
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[3].getDetails()[1].getDetails()[0].toString(),
            equalTo("0.1 = exp(- " + "MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)] * 2.3025850929940455)\n")
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[3].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 4);
        assertThat(
            functionExplanation.getDetails()[0].getDetails()[4].getDetails()[1].getDetails()[0].toString(),
            equalTo(
                "0.1 = max(0.0, ((1.1111111111111112 - MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin)))"
                    + " - 0.0(=offset), 0)])/1.1111111111111112)\n"
            )
        );
        assertThat(functionExplanation.getDetails()[0].getDetails()[4].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));
    }

    public Explanation getFiltersFunctionScoreExplanation(IndexSearcher searcher, ScoreFunction... scoreFunctions) throws IOException {
        FunctionScoreQuery functionScoreQuery = getFiltersFunctionScoreQuery(
            FunctionScoreQuery.ScoreMode.AVG,
            CombineFunction.AVG,
            scoreFunctions
        );
        return getExplanation(searcher, functionScoreQuery).getDetails()[1];
    }

    protected Explanation getExplanation(IndexSearcher searcher, FunctionScoreQuery functionScoreQuery) throws IOException {
        Weight weight = searcher.createWeight(searcher.rewrite(functionScoreQuery), org.apache.lucene.search.ScoreMode.COMPLETE, 1f);
        return weight.explain(searcher.getIndexReader().leaves().get(0), 0);
    }

    public FunctionScoreQuery getFiltersFunctionScoreQuery(
        FunctionScoreQuery.ScoreMode scoreMode,
        CombineFunction combineFunction,
        ScoreFunction... scoreFunctions
    ) {
        ScoreFunction[] filterFunctions = new ScoreFunction[scoreFunctions.length];
        for (int i = 0; i < scoreFunctions.length; i++) {
            filterFunctions[i] = new FunctionScoreQuery.FilterScoreFunction(new TermQuery(TERM), scoreFunctions[i]);
        }
        return new FunctionScoreQuery(
            new TermQuery(TERM),
            scoreMode,
            filterFunctions,
            combineFunction,
            Float.MAX_VALUE * -1,
            Float.MAX_VALUE
        );
    }

    public void checkFiltersFunctionScoreExplanation(Explanation randomExplanation, String functionExpl, int whichFunction) {
        assertThat(randomExplanation.getDescription(), equalTo("min of:"));
        Explanation explanation = randomExplanation.getDetails()[0];
        assertThat(explanation.getDescription(), equalTo("function score, score mode [avg]"));
        Explanation functionExplanation = randomExplanation.getDetails()[0].getDetails()[whichFunction];
        assertThat(functionExplanation.getDescription(), equalTo("function score, product of:"));
        assertThat(functionExplanation.getDetails()[0].getDescription(), equalTo("match filter: " + FIELD + ":" + TERM.text()));
        assertThat(functionExplanation.getDetails()[1].getDescription(), equalTo(functionExpl));
    }

    private static float[] randomPositiveFloats(int size) {
        float[] values = new float[size];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomFloat() * randomInt(100) + 1.e-5f;
        }
        return values;
    }

    private static double[] randomPositiveDoubles(int size) {
        double[] values = new double[size];
        for (int i = 0; i < values.length; i++) {
            double rand = randomValueOtherThanMany((d) -> Double.compare(d, 0) < 0, ESTestCase::randomDouble);
            values[i] = rand * randomInt(100) + 1.e-5d;
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

        @Override
        protected int doHashCode() {
            return 0;
        }
    }

    public void testSimpleWeightedFunction() throws IOException, ExecutionException, InterruptedException {
        int numFunctions = randomIntBetween(1, 3);
        float[] weights = randomPositiveFloats(numFunctions);
        double[] scores = randomPositiveDoubles(numFunctions);
        ScoreFunctionStub[] scoreFunctionStubs = new ScoreFunctionStub[numFunctions];
        for (int i = 0; i < numFunctions; i++) {
            scoreFunctionStubs[i] = new ScoreFunctionStub(scores[i]);
        }
        WeightFactorFunction[] weightFunctionStubs = new WeightFactorFunction[numFunctions];
        for (int i = 0; i < numFunctions; i++) {
            weightFunctionStubs[i] = new WeightFactorFunction(weights[i], scoreFunctionStubs[i]);
        }

        FunctionScoreQuery functionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
            FunctionScoreQuery.ScoreMode.MULTIPLY,
            CombineFunction.REPLACE,
            weightFunctionStubs
        );

        TopDocs topDocsWithWeights = searcher.search(functionScoreQueryWithWeights, 1);
        float scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double score = 1;
        for (int i = 0; i < weights.length; i++) {
            score *= weights[i] * scores[i];
        }
        assertThat(scoreWithWeight / (float) score, is(1f));
        float explainedScore = getExplanation(searcher, functionScoreQueryWithWeights).getValue().floatValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));

        functionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
            FunctionScoreQuery.ScoreMode.SUM,
            CombineFunction.REPLACE,
            weightFunctionStubs
        );

        topDocsWithWeights = searcher.search(functionScoreQueryWithWeights, 1);
        scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double sum = 0;
        for (int i = 0; i < weights.length; i++) {
            sum += weights[i] * scores[i];
        }
        assertThat(scoreWithWeight / (float) sum, is(1f));
        explainedScore = getExplanation(searcher, functionScoreQueryWithWeights).getValue().floatValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));

        functionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
            FunctionScoreQuery.ScoreMode.AVG,
            CombineFunction.REPLACE,
            weightFunctionStubs
        );

        topDocsWithWeights = searcher.search(functionScoreQueryWithWeights, 1);
        scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double norm = 0;
        sum = 0;
        for (int i = 0; i < weights.length; i++) {
            norm += weights[i];
            sum += weights[i] * scores[i];
        }
        assertThat(scoreWithWeight / (float) (sum / norm), is(1f));
        explainedScore = getExplanation(searcher, functionScoreQueryWithWeights).getValue().floatValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));

        functionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
            FunctionScoreQuery.ScoreMode.MIN,
            CombineFunction.REPLACE,
            weightFunctionStubs
        );

        topDocsWithWeights = searcher.search(functionScoreQueryWithWeights, 1);
        scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < weights.length; i++) {
            min = Math.min(min, weights[i] * scores[i]);
        }
        assertThat(scoreWithWeight / (float) min, is(1f));
        explainedScore = getExplanation(searcher, functionScoreQueryWithWeights).getValue().floatValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));

        functionScoreQueryWithWeights = getFiltersFunctionScoreQuery(
            FunctionScoreQuery.ScoreMode.MAX,
            CombineFunction.REPLACE,
            weightFunctionStubs
        );

        topDocsWithWeights = searcher.search(functionScoreQueryWithWeights, 1);
        scoreWithWeight = topDocsWithWeights.scoreDocs[0].score;
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < weights.length; i++) {
            max = Math.max(max, weights[i] * scores[i]);
        }
        assertThat(scoreWithWeight / (float) max, is(1f));
        explainedScore = getExplanation(searcher, functionScoreQueryWithWeights).getValue().floatValue();
        assertThat(explainedScore / scoreWithWeight, is(1f));
    }

    public void testWeightOnlyCreatesBoostFunction() throws IOException {
        FunctionScoreQuery filtersFunctionScoreQueryWithWeights = new FunctionScoreQuery(
            new MatchAllDocsQuery(),
            new WeightFactorFunction(2),
            CombineFunction.MULTIPLY,
            0.0f,
            100
        );
        TopDocs topDocsWithWeights = searcher.search(filtersFunctionScoreQueryWithWeights, 1);
        float score = topDocsWithWeights.scoreDocs[0].score;
        assertThat(score, equalTo(2.0f));
    }

    public void testMinScoreExplain() throws IOException {
        Query query = new MatchAllDocsQuery();
        Explanation queryExpl = searcher.explain(query, 0);

        FunctionScoreQuery fsq = new FunctionScoreQuery(query, 0f, Float.POSITIVE_INFINITY);
        Explanation fsqExpl = searcher.explain(fsq, 0);
        assertTrue(fsqExpl.isMatch());
        assertEquals(queryExpl.getValue(), fsqExpl.getValue());
        assertEquals(queryExpl.getDescription(), fsqExpl.getDescription());

        fsq = new FunctionScoreQuery(query, 10f, Float.POSITIVE_INFINITY);
        fsqExpl = searcher.explain(fsq, 0);
        assertFalse(fsqExpl.isMatch());
        assertEquals("Score value is too low, expected at least 10.0 but got 1.0", fsqExpl.getDescription());

        FunctionScoreQuery ffsq = new FunctionScoreQuery(query, 0f, Float.POSITIVE_INFINITY);
        Explanation ffsqExpl = searcher.explain(ffsq, 0);
        assertTrue(ffsqExpl.isMatch());
        assertEquals(queryExpl.getValue(), ffsqExpl.getValue());
        assertEquals(queryExpl.getDescription(), ffsqExpl.getDescription());

        ffsq = new FunctionScoreQuery(query, 10f, Float.POSITIVE_INFINITY);
        ffsqExpl = searcher.explain(ffsq, 0);
        assertFalse(ffsqExpl.isMatch());
        assertEquals("Score value is too low, expected at least 10.0 but got 1.0", ffsqExpl.getDescription());
    }

    public void testPropagatesApproximations() throws IOException {
        Query query = new RandomApproximationQuery(new MatchAllDocsQuery(), random());
        IndexSearcher searcher = newSearcher(reader);
        searcher.setQueryCache(null); // otherwise we could get a cached entry that does not have approximations

        FunctionScoreQuery fsq = new FunctionScoreQuery(query, null, Float.POSITIVE_INFINITY);
        for (org.apache.lucene.search.ScoreMode scoreMode : org.apache.lucene.search.ScoreMode.values()) {
            Weight weight = searcher.createWeight(fsq, scoreMode, 1f);
            Scorer scorer = weight.scorer(reader.leaves().get(0));
            assertNotNull(scorer.twoPhaseIterator());
        }
    }

    public void testFunctionScoreHashCodeAndEquals() {
        Float minScore = randomBoolean() ? null : 1.0f;
        CombineFunction combineFunction = randomFrom(CombineFunction.values());
        float maxBoost = randomBoolean() ? Float.POSITIVE_INFINITY : randomFloat();
        ScoreFunction function = new DummyScoreFunction(combineFunction);

        FunctionScoreQuery q = new FunctionScoreQuery(new TermQuery(new Term("foo", "bar")), function, combineFunction, minScore, maxBoost);
        FunctionScoreQuery q1 = new FunctionScoreQuery(
            new TermQuery(new Term("foo", "bar")),
            function,
            combineFunction,
            minScore,
            maxBoost
        );
        assertEquals(q, q);
        assertEquals(q.hashCode(), q.hashCode());
        assertEquals(q, q1);
        assertEquals(q.hashCode(), q1.hashCode());

        FunctionScoreQuery diffQuery = new FunctionScoreQuery(
            new TermQuery(new Term("foo", "baz")),
            function,
            combineFunction,
            minScore,
            maxBoost
        );
        FunctionScoreQuery diffMinScore = new FunctionScoreQuery(
            q.getSubQuery(),
            function,
            combineFunction,
            minScore == null ? 1.0f : null,
            maxBoost
        );
        ScoreFunction otherFunction = new DummyScoreFunction(combineFunction);
        FunctionScoreQuery diffFunction = new FunctionScoreQuery(q.getSubQuery(), otherFunction, combineFunction, minScore, maxBoost);
        FunctionScoreQuery diffMaxBoost = new FunctionScoreQuery(
            new TermQuery(new Term("foo", "bar")),
            function,
            combineFunction,
            minScore,
            maxBoost == 1.0f ? 0.9f : 1.0f
        );
        FunctionScoreQuery[] queries = new FunctionScoreQuery[] { diffFunction, diffMinScore, diffQuery, q, diffMaxBoost };
        final int numIters = randomIntBetween(20, 100);
        for (int i = 0; i < numIters; i++) {
            FunctionScoreQuery left = randomFrom(queries);
            FunctionScoreQuery right = randomFrom(queries);
            if (left == right) {
                assertEquals(left, right);
                assertEquals(left.hashCode(), right.hashCode());
            } else {
                assertNotEquals(left + " == " + right, left, right);
            }
        }

    }

    public void testFilterFunctionScoreHashCodeAndEquals() {
        CombineFunction combineFunction = randomFrom(CombineFunction.values());
        ScoreFunction scoreFunction = new DummyScoreFunction(combineFunction);
        Float minScore = randomBoolean() ? null : 1.0f;
        Float maxBoost = randomBoolean() ? Float.POSITIVE_INFINITY : randomFloat();

        FilterScoreFunction function = new FilterScoreFunction(new TermQuery(new Term("filter", "query")), scoreFunction);
        FunctionScoreQuery q = new FunctionScoreQuery(new TermQuery(new Term("foo", "bar")), function, combineFunction, minScore, maxBoost);
        FunctionScoreQuery q1 = new FunctionScoreQuery(
            new TermQuery(new Term("foo", "bar")),
            function,
            combineFunction,
            minScore,
            maxBoost
        );
        assertEquals(q, q);
        assertEquals(q.hashCode(), q.hashCode());
        assertEquals(q, q1);
        assertEquals(q.hashCode(), q1.hashCode());
        FunctionScoreQuery diffCombineFunc = new FunctionScoreQuery(
            new TermQuery(new Term("foo", "bar")),
            function,
            combineFunction == CombineFunction.AVG ? CombineFunction.MAX : CombineFunction.AVG,
            minScore,
            maxBoost
        );
        FunctionScoreQuery diffQuery = new FunctionScoreQuery(
            new TermQuery(new Term("foo", "baz")),
            function,
            combineFunction,
            minScore,
            maxBoost
        );
        FunctionScoreQuery diffMaxBoost = new FunctionScoreQuery(
            new TermQuery(new Term("foo", "bar")),
            function,
            combineFunction,
            minScore,
            maxBoost == 1.0f ? 0.9f : 1.0f
        );
        FunctionScoreQuery diffMinScore = new FunctionScoreQuery(
            new TermQuery(new Term("foo", "bar")),
            function,
            combineFunction,
            minScore == null ? 0.9f : null,
            maxBoost
        );
        FilterScoreFunction otherFunc = new FilterScoreFunction(new TermQuery(new Term("filter", "other_query")), scoreFunction);
        FunctionScoreQuery diffFunc = new FunctionScoreQuery(
            new TermQuery(new Term("foo", "bar")),
            randomFrom(ScoreMode.values()),
            randomBoolean() ? new ScoreFunction[] { function, otherFunc } : new ScoreFunction[] { otherFunc },
            combineFunction,
            minScore,
            maxBoost
        );

        FunctionScoreQuery[] queries = new FunctionScoreQuery[] { diffQuery, diffMaxBoost, diffMinScore, diffFunc, q, diffCombineFunc };
        final int numIters = randomIntBetween(20, 100);
        for (int i = 0; i < numIters; i++) {
            FunctionScoreQuery left = randomFrom(queries);
            FunctionScoreQuery right = randomFrom(queries);
            if (left == right) {
                assertEquals(left, right);
                assertEquals(left.hashCode(), right.hashCode());
            } else {
                assertNotEquals(left + " == " + right, left, right);
            }
        }
    }

    public void testExplanationAndScoreEqualsEvenIfNoFunctionMatches() throws IOException {
        IndexSearcher localSearcher = newSearcher(reader);
        CombineFunction combineFunction = randomFrom(
            new CombineFunction[] {
                CombineFunction.SUM,
                CombineFunction.AVG,
                CombineFunction.MIN,
                CombineFunction.MAX,
                CombineFunction.MULTIPLY,
                CombineFunction.REPLACE }
        );

        // check for document that has no matching function
        FunctionScoreQuery query = new FunctionScoreQuery(
            new TermQuery(new Term(FIELD, "out")),
            new FilterScoreFunction(new TermQuery(new Term("_uid", "2")), new WeightFactorFunction(10)),
            combineFunction,
            Float.NEGATIVE_INFINITY,
            Float.MAX_VALUE
        );
        TopDocs searchResult = localSearcher.search(query, 1);
        Explanation explanation = localSearcher.explain(query, searchResult.scoreDocs[0].doc);
        assertThat(searchResult.scoreDocs[0].score, equalTo(explanation.getValue()));

        // check for document that has a matching function
        query = new FunctionScoreQuery(
            new TermQuery(new Term(FIELD, "out")),
            new FilterScoreFunction(new TermQuery(new Term("_uid", "1")), new WeightFactorFunction(10)),
            combineFunction,
            Float.NEGATIVE_INFINITY,
            Float.MAX_VALUE
        );
        searchResult = localSearcher.search(query, 1);
        explanation = localSearcher.explain(query, searchResult.scoreDocs[0].doc);
        assertThat(searchResult.scoreDocs[0].score, equalTo(explanation.getValue()));
    }

    public void testWeightFactorNeedsScore() {
        for (boolean needsScore : new boolean[] { true, false }) {
            WeightFactorFunction function = new WeightFactorFunction(10.0f, new ScoreFunction(CombineFunction.REPLACE) {
                @Override
                public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
                    return null;
                }

                @Override
                public boolean needsScores() {
                    return needsScore;
                }

                @Override
                protected boolean doEquals(ScoreFunction other) {
                    return false;
                }

                @Override
                protected int doHashCode() {
                    return 0;
                }
            });
            assertEquals(needsScore, function.needsScores());
        }
    }

    private static class ConstantScoreFunction extends ScoreFunction {
        final double value;

        protected ConstantScoreFunction(double value) {
            super(CombineFunction.REPLACE);
            this.value = value;
        }

        @Override
        public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
            return new LeafScoreFunction() {
                @Override
                public double score(int docId, float subQueryScore) throws IOException {
                    return value;
                }

                @Override
                public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                    return null;
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

        @Override
        protected int doHashCode() {
            return 0;
        }
    }

    public void testWithInvalidScores() {
        IndexSearcher localSearcher = new IndexSearcher(reader);
        FunctionScoreQuery query1 = new FunctionScoreQuery(
            new TermQuery(new Term(FIELD, "out")),
            new ConstantScoreFunction(Float.NaN),
            CombineFunction.REPLACE,
            null,
            Float.POSITIVE_INFINITY
        );
        ElasticsearchException exc = expectThrows(ElasticsearchException.class, () -> localSearcher.search(query1, 1));
        assertThat(exc.getMessage(), containsString("function score query returned an invalid score: " + Float.NaN));
        FunctionScoreQuery query2 = new FunctionScoreQuery(
            new TermQuery(new Term(FIELD, "out")),
            new ConstantScoreFunction(Float.NEGATIVE_INFINITY),
            CombineFunction.REPLACE,
            null,
            Float.POSITIVE_INFINITY
        );
        exc = expectThrows(ElasticsearchException.class, () -> localSearcher.search(query2, 1));
        assertThat(exc.getMessage(), containsString("function score query returned an invalid score: " + Float.NEGATIVE_INFINITY));
    }

    public void testExceptionOnNegativeScores() {
        IndexSearcher localSearcher = new IndexSearcher(reader);
        TermQuery termQuery = new TermQuery(new Term(FIELD, "out"));

        // test that field_value_factor function throws an exception on negative scores
        FieldValueFactorFunction.Modifier modifier = FieldValueFactorFunction.Modifier.NONE;
        final ScoreFunction fvfFunction = new FieldValueFactorFunction(FIELD, -10, modifier, 1.0, new IndexNumericFieldDataStub());
        FunctionScoreQuery fsQuery1 = new FunctionScoreQuery(
            termQuery,
            fvfFunction,
            CombineFunction.REPLACE,
            null,
            Float.POSITIVE_INFINITY
        );
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> localSearcher.search(fsQuery1, 1));
        assertThat(exc.getMessage(), containsString("field value function must not produce negative scores"));
        assertThat(exc.getMessage(), not(containsString("consider using ln1p or ln2p instead of ln to avoid negative scores")));
        assertThat(exc.getMessage(), not(containsString("consider using log1p or log2p instead of log to avoid negative scores")));
    }

    public void testExceptionOnLnNegativeScores() {
        IndexSearcher localSearcher = new IndexSearcher(reader);
        TermQuery termQuery = new TermQuery(new Term(FIELD, "out"));

        // test that field_value_factor function using modifier ln throws an exception on negative scores
        FieldValueFactorFunction.Modifier modifier = FieldValueFactorFunction.Modifier.LN;
        final ScoreFunction fvfFunction = new FieldValueFactorFunction(FIELD, 0.5f, modifier, 1.0, new IndexNumericFieldDataStub());
        FunctionScoreQuery fsQuery1 = new FunctionScoreQuery(
            termQuery,
            fvfFunction,
            CombineFunction.REPLACE,
            null,
            Float.POSITIVE_INFINITY
        );
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> localSearcher.search(fsQuery1, 1));
        assertThat(exc.getMessage(), containsString("consider using ln1p or ln2p instead of ln to avoid negative scores"));
    }

    public void testExceptionOnLogNegativeScores() {
        IndexSearcher localSearcher = new IndexSearcher(reader);
        TermQuery termQuery = new TermQuery(new Term(FIELD, "out"));

        // test that field_value_factor function using modifier log throws an exception on negative scores
        FieldValueFactorFunction.Modifier modifier = FieldValueFactorFunction.Modifier.LOG;
        final ScoreFunction fvfFunction = new FieldValueFactorFunction(FIELD, 0.5f, modifier, 1.0, new IndexNumericFieldDataStub());
        FunctionScoreQuery fsQuery1 = new FunctionScoreQuery(
            termQuery,
            fvfFunction,
            CombineFunction.REPLACE,
            null,
            Float.POSITIVE_INFINITY
        );
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> localSearcher.search(fsQuery1, 1));
        assertThat(exc.getMessage(), containsString("consider using log1p or log2p instead of log to avoid negative scores"));
    }

    private static class DummyScoreFunction extends ScoreFunction {
        protected DummyScoreFunction(CombineFunction scoreCombiner) {
            super(scoreCombiner);
        }

        @Override
        public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
            return null;
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        protected boolean doEquals(ScoreFunction other) {
            return other == this;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }
    }
}
