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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.InternalWeightedAvg;
import org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregator;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class WeightedAvgAggregatorTests extends AggregatorTestCase {

    public void testNoDocs() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            // Intentionally not writing any docs
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testNoMatchingField() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 3)));
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testSomeMatchesSortedNumericDocValuesNoWeight() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 7),
                new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 2),
                new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 3),
                new SortedNumericDocValuesField("weight_field", 1)));
        }, avg -> {
            assertEquals(4, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testSomeMatchesSortedNumericDocValuesWeights() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 7),
                new SortedNumericDocValuesField("weight_field", 2)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 2),
                new SortedNumericDocValuesField("weight_field", 3)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 3),
                new SortedNumericDocValuesField("weight_field", 3)));

        }, avg -> {
            // (7*2 + 2*3 + 3*3) / (2+3+3) == 3.625
            assertEquals(3.625, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(new DocValuesFieldExistsQuery("value_field"), aggregationBuilder, iw -> {
            iw.addDocument(Arrays.asList(new NumericDocValuesField("value_field", 7),
                new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new NumericDocValuesField("value_field", 2),
                new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new NumericDocValuesField("value_field", 3),
                new SortedNumericDocValuesField("weight_field", 1)));
        }, avg -> {
            assertEquals(4, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testQueryFiltering() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(IntPoint.newRangeQuery("value_field", 0, 3), aggregationBuilder, iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("value_field", 7), new SortedNumericDocValuesField("value_field", 7),
                new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new IntPoint("value_field", 1), new SortedNumericDocValuesField("value_field", 2),
                new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new IntPoint("value_field", 3), new SortedNumericDocValuesField("value_field", 3),
                new SortedNumericDocValuesField("weight_field", 1)));
        }, avg -> {
            assertEquals(2.5, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testQueryFilteringWeights() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(IntPoint.newRangeQuery("filter_field", 0, 3), aggregationBuilder, iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("filter_field", 7), new SortedNumericDocValuesField("value_field", 7),
                new SortedNumericDocValuesField("weight_field", 2)));
            iw.addDocument(Arrays.asList(new IntPoint("filter_field", 2), new SortedNumericDocValuesField("value_field", 2),
                new SortedNumericDocValuesField("weight_field", 3)));
            iw.addDocument(Arrays.asList(new IntPoint("filter_field", 3), new SortedNumericDocValuesField("value_field", 3),
                new SortedNumericDocValuesField("weight_field", 4)));
        }, avg -> {
            double value = (2.0*3.0 + 3.0*4.0) / (3.0+4.0);
            assertEquals(value, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(IntPoint.newRangeQuery("value_field", -1, 0), aggregationBuilder, iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("value_field", 7), new SortedNumericDocValuesField("value_field", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("value_field", 1), new SortedNumericDocValuesField("value_field", 2)));
            iw.addDocument(Arrays.asList(new IntPoint("value_field", 3), new SortedNumericDocValuesField("value_field", 7)));
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testQueryFiltersAllWeights() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(IntPoint.newRangeQuery("value_field", -1, 0), aggregationBuilder, iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("filter_field", 7), new SortedNumericDocValuesField("value_field", 7),
                new SortedNumericDocValuesField("weight_field", 2)));
            iw.addDocument(Arrays.asList(new IntPoint("filter_field", 2), new SortedNumericDocValuesField("value_field", 2),
                new SortedNumericDocValuesField("weight_field", 3)));
            iw.addDocument(Arrays.asList(new IntPoint("filter_field", 3), new SortedNumericDocValuesField("value_field", 3),
                new SortedNumericDocValuesField("weight_field", 4)));
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testValueSetMissing() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder()
            .setFieldName("value_field")
            .setMissing(2)
            .build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            iw.addDocument(Collections.singletonList(new SortedNumericDocValuesField("weight_field", 2)));
            iw.addDocument(Collections.singletonList(new SortedNumericDocValuesField("weight_field", 3)));
            iw.addDocument(Collections.singletonList(new SortedNumericDocValuesField("weight_field", 4)));
        }, avg -> {
            double value = (2.0*2.0 + 2.0*3.0 + 2.0*4.0) / (2.0+3.0+4.0);
            assertEquals(value, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testWeightSetMissing() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig =  new MultiValuesSourceFieldConfig.Builder()
            .setFieldName("weight_field")
            .setMissing(2)
            .build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            iw.addDocument(Collections.singletonList(new SortedNumericDocValuesField("value_field", 2)));
            iw.addDocument(Collections.singletonList(new SortedNumericDocValuesField("value_field", 3)));
            iw.addDocument(Collections.singletonList(new SortedNumericDocValuesField("value_field", 4)));
        }, avg -> {
            double value = (2.0*2.0 + 3.0*2.0 + 4.0*2.0) / (2.0+2.0+2.0);
            assertEquals(value, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testWeightSetTimezone() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder()
            .setFieldName("weight_field")
            .setTimeZone(ZoneOffset.UTC)
            .build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
                iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 2),
                    new SortedNumericDocValuesField("weight_field", 1)));
                iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 3),
                    new SortedNumericDocValuesField("weight_field", 1)));
                iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 4),
                    new SortedNumericDocValuesField("weight_field", 1)));
            }, avg -> {
               fail("Should not have executed test case");
            }));
        assertThat(e.getMessage(), equalTo("Field [weight_field] of type [long] does not support custom time zones"));
    }

    public void testValueSetTimezone() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder()
            .setFieldName("value_field")
            .setTimeZone(ZoneOffset.UTC)
            .build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
                iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 2),
                    new SortedNumericDocValuesField("weight_field", 1)));
                iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 3),
                    new SortedNumericDocValuesField("weight_field", 1)));
                iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 4),
                    new SortedNumericDocValuesField("weight_field", 1)));
            }, avg -> {
                fail("Should not have executed test case");
            }));
        assertThat(e.getMessage(), equalTo("Field [value_field] of type [long] does not support custom time zones"));
    }

    public void testMultiValues() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder()
            .setFieldName("value_field")
            .build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);

        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 2),
                new SortedNumericDocValuesField("value_field", 3), new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 3),
                new SortedNumericDocValuesField("value_field", 4), new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 4),
                new SortedNumericDocValuesField("value_field", 5), new SortedNumericDocValuesField("weight_field", 1)));
        }, avg -> {
            double value = (((2.0+3.0)/2.0) + ((3.0+4.0)/2.0) + ((4.0+5.0)/2.0)) / (1.0+1.0+1.0);
            assertEquals(value, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testMultiWeight() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder()
            .setFieldName("weight_field")
            .build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);

        AggregationExecutionException e = expectThrows(AggregationExecutionException.class,
            () -> testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
                iw.addDocument(Arrays.asList(
                    new SortedNumericDocValuesField("value_field", 2),
                    new SortedNumericDocValuesField("weight_field", 2), new SortedNumericDocValuesField("weight_field", 3)));
                iw.addDocument(Arrays.asList(
                    new SortedNumericDocValuesField("value_field", 3),
                    new SortedNumericDocValuesField("weight_field", 3), new SortedNumericDocValuesField("weight_field", 4)));
                iw.addDocument(Arrays.asList(
                    new SortedNumericDocValuesField("value_field", 4),
                    new SortedNumericDocValuesField("weight_field", 4), new SortedNumericDocValuesField("weight_field", 5)));
            }, avg -> {
                fail("Should have thrown exception");
            }));
        assertThat(e.getMessage(), containsString("Encountered more than one weight for a single document. " +
            "Use a script to combine multiple weights-per-doc into a single value."));
    }

    public void testFormatter() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig)
            .format("0.00%");
        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 7),
                new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 2),
                new SortedNumericDocValuesField("weight_field", 1)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("value_field", 3),
                new SortedNumericDocValuesField("weight_field", 1)));
        }, avg -> {
            assertEquals(4, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
            assertEquals("400.00%", avg.getValueAsString());
        });
    }

    public void testSummationAccuracy() throws IOException {
        // Summing up a normal array and expect an accurate value
        double[] values = new double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7};
        verifyAvgOfDoubles(values, 0.9, 0d);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifyAvgOfDoubles(values, sum / n, 1e-10);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifyAvgOfDoubles(largeValues, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifyAvgOfDoubles(largeValues, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifyAvgOfDoubles(double[] values, double expected, double delta) throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder("_name")
            .value(valueConfig)
            .weight(weightConfig);
        testCase(new MatchAllDocsQuery(), aggregationBuilder,
            iw -> {
                for (double value : values) {
                    iw.addDocument(Arrays.asList(new NumericDocValuesField("value_field", NumericUtils.doubleToSortableLong(value)),
                        new SortedNumericDocValuesField("weight_field", NumericUtils.doubleToSortableLong(1.0))));
                }
            },
            avg -> assertEquals(expected, avg.getValue(), delta),
            NumberFieldMapper.NumberType.DOUBLE
        );
    }

    private void testCase(Query query, WeightedAvgAggregationBuilder aggregationBuilder,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalWeightedAvg> verify) throws IOException {
        testCase(query, aggregationBuilder, buildIndex, verify, NumberFieldMapper.NumberType.LONG);
    }

    private void testCase(Query query, WeightedAvgAggregationBuilder aggregationBuilder,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalWeightedAvg> verify,
                          NumberFieldMapper.NumberType fieldNumberType) throws IOException {

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        try {
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(fieldNumberType);
            fieldType.setName("value_field");
            fieldType.setHasDocValues(true);

            MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType(fieldNumberType);
            fieldType2.setName("weight_field");
            fieldType2.setHasDocValues(true);

            WeightedAvgAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType, fieldType2);
            aggregator.preCollection();
            indexSearcher.search(query, aggregator);
            aggregator.postCollection();
            verify.accept((InternalWeightedAvg) aggregator.buildAggregation(0L));
        } finally {
            indexReader.close();
            directory.close();
        }
    }
}
