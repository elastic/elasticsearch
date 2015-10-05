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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.search.function.*;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionBuilder;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.core.IsEqual.equalTo;

public class FunctionScoreTests extends ESTestCase {

    private static final String UNSUPPORTED = "Method not implemented. This is just a stub for testing.";

    class IndexFieldDataStub implements IndexFieldData<AtomicFieldData> {
        @Override
        public MappedFieldType.Names getFieldNames() {
            return new MappedFieldType.Names("test");
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
        public void clear(IndexReader reader) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public Index index() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }
    }

    class IndexNumericFieldDataStub implements IndexNumericFieldData {

        @Override
        public NumericType getNumericType() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public MappedFieldType.Names getFieldNames() {
            return new MappedFieldType.Names("test");
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
        public void clear(IndexReader reader) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }

        @Override
        public Index index() {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }
    }

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
        FieldType ft = new FieldType(TextField.TYPE_STORED);
        ft.freeze();
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

    @Test
    public void testExplainFunctionScoreQuery() throws IOException {

        Explanation functionExplanation = getFunctionScoreExplanation(searcher, new RandomScoreFunction(0, 0, new IndexFieldDataStub()));
        checkFunctionScoreExplanation(functionExplanation, "random score function (seed: 0)");
        assertThat(functionExplanation.getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, new FieldValueFactorFunction("test", 1, FieldValueFactorFunction.Modifier.LN, new Double(1), null));
        checkFunctionScoreExplanation(functionExplanation, "field value function: ln(doc['test'].value?:1.0 * factor=1.0)");
        assertThat(functionExplanation.getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, GaussDecayFunctionBuilder.GAUSS_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX));
        checkFunctionScoreExplanation(functionExplanation, "Function for field test:");
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].toString(), equalTo("0.1 = exp(-0.5*pow(MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)],2.0)/0.21714724095162594)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, ExponentialDecayFunctionBuilder.EXP_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX));
        checkFunctionScoreExplanation(functionExplanation, "Function for field test:");
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].toString(), equalTo("0.1 = exp(- MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)] * 2.3025850929940455)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));

        functionExplanation = getFunctionScoreExplanation(searcher, new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, LinearDecayFunctionBuilder.LINEAR_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX));
        checkFunctionScoreExplanation(functionExplanation, "Function for field test:");
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].toString(), equalTo("0.1 = max(0.0, ((1.1111111111111112 - MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)])/1.1111111111111112)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails().length, equalTo(0));
    }

    public Explanation getFunctionScoreExplanation(IndexSearcher searcher, ScoreFunction scoreFunction) throws IOException {
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(new TermQuery(TERM), scoreFunction, 0.0f, CombineFunction.AVG, 100);
        Weight weight = functionScoreQuery.createWeight(searcher, true);
        Explanation explanation = weight.explain(searcher.getIndexReader().leaves().get(0), 0);
        return explanation.getDetails()[1];
    }

    public void checkFunctionScoreExplanation(Explanation randomExplanation, String functionExpl) {
        assertThat(randomExplanation.getDescription(), equalTo("min of:"));
        assertThat(randomExplanation.getDetails()[0].getDescription(), equalTo(functionExpl));
    }

    @Test
    public void testExplainFiltersFunctionScoreQuery() throws IOException {
        Explanation functionExplanation = getFiltersFunctionScoreExplanation(searcher, new RandomScoreFunction(0, 0, new IndexFieldDataStub()));
        checkFiltersFunctionScoreExplanation(functionExplanation, "random score function (seed: 0)", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, new FieldValueFactorFunction("test", 1, FieldValueFactorFunction.Modifier.LN, new Double(1), null));
        checkFiltersFunctionScoreExplanation(functionExplanation, "field value function: ln(doc['test'].value?:1.0 * factor=1.0)", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails().length, equalTo(0));

        functionExplanation = getFiltersFunctionScoreExplanation(searcher, new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, GaussDecayFunctionBuilder.GAUSS_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX));
        checkFiltersFunctionScoreExplanation(functionExplanation, "Function for field test:", 0);
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].toString(), equalTo("0.1 = exp(-0.5*pow(MAX[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)],2.0)/0.21714724095162594)\n"));
        assertThat(functionExplanation.getDetails()[0].getDetails()[0].getDetails()[1].getDetails()[0].getDetails().length, equalTo(0));

        // now test all together
        functionExplanation = getFiltersFunctionScoreExplanation(searcher
                , new RandomScoreFunction(0, 0, new IndexFieldDataStub())
                , new FieldValueFactorFunction("test", 1, FieldValueFactorFunction.Modifier.LN, new Double(1), null)
                , new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, GaussDecayFunctionBuilder.GAUSS_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX)
                , new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, ExponentialDecayFunctionBuilder.EXP_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX)
                , new DecayFunctionBuilder.NumericFieldDataScoreFunction(0, 1, 0.1, 0, LinearDecayFunctionBuilder.LINEAR_DECAY_FUNCTION, new IndexNumericFieldDataStub(), MultiValueMode.MAX)
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
        FiltersFunctionScoreQuery.FilterFunction[] filterFunctions = new FiltersFunctionScoreQuery.FilterFunction[scoreFunctions.length];
        for (int i = 0; i < scoreFunctions.length; i++) {
            filterFunctions[i] = new FiltersFunctionScoreQuery.FilterFunction(
                    new TermQuery(TERM), scoreFunctions[i]);
        }
        FiltersFunctionScoreQuery filtersFunctionScoreQuery = new FiltersFunctionScoreQuery(new TermQuery(TERM), FiltersFunctionScoreQuery.ScoreMode.AVG, filterFunctions, 100, new Float(0.0), CombineFunction.AVG);
        Weight weight = filtersFunctionScoreQuery.createWeight(searcher, true);
        Explanation explanation = weight.explain(searcher.getIndexReader().leaves().get(0), 0);
        return explanation.getDetails()[1];
    }

    public void checkFiltersFunctionScoreExplanation(Explanation randomExplanation, String functionExpl, int whichFunction) {
        assertThat(randomExplanation.getDescription(), equalTo("min of:"));
        assertThat(randomExplanation.getDetails()[0].getDescription(), equalTo("function score, score mode [avg]"));
        assertThat(randomExplanation.getDetails()[0].getDetails()[whichFunction].getDescription(), equalTo("function score, product of:"));
        assertThat(randomExplanation.getDetails()[0].getDetails()[whichFunction].getDetails()[0].getDescription(), equalTo("match filter: " + FIELD + ":" + TERM.text()));
        assertThat(randomExplanation.getDetails()[0].getDetails()[whichFunction].getDetails()[1].getDescription(), equalTo(functionExpl));
    }
}