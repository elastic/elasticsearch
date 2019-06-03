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
package org.elasticsearch.search.aggregations.matrix;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregator;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsResults;
import org.elasticsearch.search.aggregations.matrix.stats.RunningStats;
import org.elasticsearch.search.aggregations.support.ArrayValuesSourceAggregationBuilder;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public abstract class AbstractMatrixStatsTestCase extends AggregatorTestCase {

    private boolean initialized = false;
    protected static int numDocs;
    protected static List<String> fieldNames;
    protected static List<MappedFieldType> fieldTypes;

    public static final String DOUBLE_FIELD_NAME = "mapped_double";
    public static final String INT_FIELD_NAME = "mapped_int";

    protected static RandomIndexWriter indexWriter;
    protected static Directory directory;

    protected static RunningStats baseTruthStats = new RunningStats();
    protected static MatrixStatsResults results;

    public void initializeData() {
        if (initialized == false) {
            fieldNames = randomNumericFields(randomIntBetween(2, 7));
            fieldTypes = new ArrayList<>(fieldNames.size());
            NumberFieldMapper.NumberFieldType fieldType;
            for (String fieldName : fieldNames) {
                if (fieldName.contains(DOUBLE_FIELD_NAME)) {
                    fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
                } else {
                    fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
                }
                fieldType.setName(fieldName);
                fieldTypes.add(fieldType);
            }
            numDocs = randomIntBetween(10000, 100000);

            try {
                directory = newDirectory();
                indexWriter = new RandomIndexWriter(random(), directory);
                // create and index "documents"
                for (int i = 0; i < numDocs; ++i) {
                    Document doc = new Document();
                    double[] fieldValues = new double[fieldNames.size()];
                    int v = 0;
                    for (String fieldName : fieldNames) {
                        Number value;
                        if (fieldName.contains(DOUBLE_FIELD_NAME)) {
                            value = randomDoubleBetween(-1000, 1000, true);
                            doc.add(new SortedNumericDocValuesField(fieldName, NumericUtils.doubleToSortableLong(value.doubleValue())));
                        } else {
                            int randomI = randomInt();
                            value = randomI;
                            doc.add(new SortedNumericDocValuesField(fieldName, (long)randomI));
                        }
                        fieldValues[v++] = value.doubleValue();
                    }
                    indexWriter.addDocument(doc);
                    baseTruthStats.add(fieldNames.stream().toArray(n -> new String[n]), fieldValues);
                }
                indexWriter.close();
            } catch (IOException e) {
                throw new ElasticsearchException("exception in test suite", e);
            }
            initialized = true;
        }
    }

    public abstract void testAggregationAccuracy() throws IOException;

    public abstract <A extends ArrayValuesSourceAggregationBuilder.LeafOnly> A getAggregatorBuilder(String name);

    public <A extends MatrixStatsAggregator> A getAggregator(String name,
            IndexSearcher indexSearcher) throws IOException {
        ArrayValuesSourceAggregationBuilder.LeafOnly aggBuilder = getAggregatorBuilder(name);
        return createAggregator(aggBuilder, indexSearcher, fieldTypes.stream().toArray(n -> new NumberFieldMapper.NumberFieldType[n]));
    }

    public abstract <R extends MatrixStatsResults> R computeResults();
    public <R extends MatrixStatsResults> R getResults() {
        if (results == null) {
            return computeResults();
        }
        return (R) results;
    }

    protected void testCase(Query query,
                            Consumer<InternalAggregation> verify) throws IOException {
        initializeData();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
        AggregationBuilder aggBuilder = getAggregatorBuilder("_name");
        InternalAggregation agg = searchAndReduce(indexSearcher, query, aggBuilder, fieldTypes.stream().toArray(n -> new NumberFieldMapper.NumberFieldType[n]));
        verify.accept(agg);
        indexReader.close();
    }


    @AfterClass
    public static void closeIndex() throws IOException {
        directory.close();
    }

    private static String randomNumericField() {
        int randomInt = randomInt(2);
        switch (randomInt) {
            case 0:
                return DOUBLE_FIELD_NAME;
            case 1:
            default:
                return INT_FIELD_NAME;
        }
    }

    public static ArrayList randomNumericFields(int numFields) {
        ArrayList<String> fields = new ArrayList<>(numFields);
        for (int i = 0; i < numFields; ++i) {
            fields.add(randomNumericField() + i);
        }
        return fields;
    }


    protected void assertNearlyEqual(MatrixStatsResults a, MatrixStatsResults b) {
        assertEquals(a.getDocCount(), b.getDocCount());
        for (String field : fieldNames) {
            // field count
            assertEquals(a.getFieldCount(field), b.getFieldCount(field));
            // means
            assertTrue(nearlyEqual(a.getMean(field), b.getMean(field), 1e-7));
            // variances
            assertTrue(nearlyEqual(a.getVariance(field), b.getVariance(field), 1e-7));
            // skewness
            assertTrue(nearlyEqual(a.getSkewness(field), b.getSkewness(field), 1e-4));
            // kurtosis
            assertTrue(nearlyEqual(a.getKurtosis(field), b.getKurtosis(field), 1e-4));
            for (String fieldB : fieldNames) {
                // covariances
                assertTrue(nearlyEqual(a.getCovariance(field, fieldB), b.getCovariance(field, fieldB), 1e-7));
                // correlation
                assertTrue(nearlyEqual(a.getCorrelation(field, fieldB), b.getCorrelation(field, fieldB), 1e-7));
            }
        }
    }

    protected static boolean nearlyEqual(double a, double b, double epsilon) {
        final double absA = Math.abs(a);
        final double absB = Math.abs(b);
        final double diff = Math.abs(a - b);

        if (a == b) { // shortcut, handles infinities
            return true;
        } else if (a == 0 || b == 0 || diff < Double.MIN_NORMAL) {
            // a or b is zero or both are extremely close to it
            // relative error is less meaningful here
            return diff < (epsilon * Double.MIN_NORMAL);
        } else { // use relative error
            return diff / Math.min((absA + absB), Double.MAX_VALUE) < epsilon;
        }
    }
}
