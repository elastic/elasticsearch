/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Strings;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.InternalMultiValueAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.transform.transforms.pivot.AggregationResultUtils.BucketKeyExtractor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationResultUtilsTests extends ESTestCase {

    class TestMultiValueAggregation extends InternalMultiValueAggregation {

        private final Map<String, String> values;

        TestMultiValueAggregation(String name, Map<String, String> values) {
            super(name, Map.of());
            this.values = values;
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getValuesAsStrings(String name) {
            return List.of(values.get(name).toString());
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<String> valueNames() {
            return values.keySet();
        }

        @Override
        protected boolean mustReduceOnSingleInternalAgg() {
            return false;
        }

        @Override
        public Object getProperty(List<String> path) {
            return null;
        }
    }

    class TestNumericMultiValueAggregation extends InternalNumericMetricsAggregation.MultiValue {

        private final Map<String, Double> values;

        TestNumericMultiValueAggregation(String name, Map<String, Double> values) {
            super(name, null, Map.of());
            this.values = values;
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double value(String name) {
            return values.get(name);
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<String> valueNames() {
            return values.keySet();
        }
    }

    public void testExtractCompositeAggregationResults() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);

        GroupConfig groupBy = parseGroupConfig(Strings.format("""
            { "%s" : {"terms" : {   "field" : "doesn't_matter_for_this_test"} } }
            """, targetField));

        String aggName = randomAlphaOfLengthBetween(5, 10);
        List<AggregationBuilder> aggregationBuilders = List.of(AggregationBuilders.avg(aggName));

        InternalComposite input = createComposite(
            List.of(
                createInternalCompositeBucket(
                    asMap(targetField, "ID1"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 42.33))),
                    8L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 28.99))),
                    3L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID3"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, Double.NaN))),
                    0L
                )
            )
        );

        List<Map<String, Object>> expected = List.of(
            asMap(targetField, "ID1", aggName, 42.33),
            asMap(targetField, "ID2", aggName, 28.99),
            asMap(targetField, "ID3", aggName, null)
        );
        Map<String, String> fieldTypeMap = asStringMap(targetField, "keyword", aggName, "double");
        executeTest(groupBy, aggregationBuilders, List.of(), input, fieldTypeMap, expected, 11);
    }

    public void testExtractCompositeAggregationResultsMultipleGroups() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);
        String targetField2 = randomAlphaOfLengthBetween(5, 10) + "_2";

        GroupConfig groupBy = parseGroupConfig(Strings.format("""
            {
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              },
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              }
            }""", targetField, targetField2));

        String aggName = randomAlphaOfLengthBetween(5, 10);
        String aggTypedName = "avg#" + aggName;
        List<AggregationBuilder> aggregationBuilders = List.of(AggregationBuilders.avg(aggName));

        InternalComposite input = createComposite(
            List.of(
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID1_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 42.33))),
                    1L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID2_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 8.4))),
                    2L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID2", targetField2, "ID1_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 28.99))),
                    3L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID3", targetField2, "ID2_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, Double.NaN))),
                    0L
                )
            )
        );

        List<Map<String, Object>> expected = List.of(
            asMap(targetField, "ID1", targetField2, "ID1_2", aggName, 42.33),
            asMap(targetField, "ID1", targetField2, "ID2_2", aggName, 8.4),
            asMap(targetField, "ID2", targetField2, "ID1_2", aggName, 28.99),
            asMap(targetField, "ID3", targetField2, "ID2_2", aggName, null)
        );
        Map<String, String> fieldTypeMap = asStringMap(aggName, "double", targetField, "keyword", targetField2, "keyword");
        executeTest(groupBy, aggregationBuilders, List.of(), input, fieldTypeMap, expected, 6);
    }

    public void testExtractCompositeAggregationResultsMultiAggregations() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);

        GroupConfig groupBy = parseGroupConfig(Strings.format("""
            {
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              }
            }""", targetField));

        String aggName = randomAlphaOfLengthBetween(5, 10);

        String aggName2 = randomAlphaOfLengthBetween(5, 10) + "_2";

        List<AggregationBuilder> aggregationBuilders = List.of(AggregationBuilders.avg(aggName), AggregationBuilders.max(aggName2));

        InternalComposite input = createComposite(
            List.of(
                createInternalCompositeBucket(
                    asMap(targetField, "ID1"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 42.33), createSingleMetricAgg(aggName2, 9.9))),
                    111L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 28.99), createSingleMetricAgg(aggName2, 222.33))),
                    88L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID3"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 12.55), createSingleMetricAgg(aggName2, Double.NaN))),
                    1L
                )
            )
        );

        List<Map<String, Object>> expected = List.of(
            asMap(targetField, "ID1", aggName, 42.33, aggName2, 9.9),
            asMap(targetField, "ID2", aggName, 28.99, aggName2, 222.33),
            asMap(targetField, "ID3", aggName, 12.55, aggName2, null)
        );
        Map<String, String> fieldTypeMap = asStringMap(targetField, "keyword", aggName, "double", aggName2, "double");
        executeTest(groupBy, aggregationBuilders, List.of(), input, fieldTypeMap, expected, 200);
    }

    public void testExtractCompositeAggregationResultsMultiAggregationsAndTypes() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);
        String targetField2 = randomAlphaOfLengthBetween(5, 10) + "_2";

        GroupConfig groupBy = parseGroupConfig(Strings.format("""
            {
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              },
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              }
            }""", targetField, targetField2));

        String aggName = randomAlphaOfLengthBetween(5, 10);

        String aggName2 = randomAlphaOfLengthBetween(5, 10) + "_2";

        List<AggregationBuilder> aggregationBuilders = List.of(AggregationBuilders.avg(aggName), AggregationBuilders.max(aggName2));

        InternalComposite input = createComposite(
            List.of(
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID1_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 42.33), createSingleMetricAgg(aggName2, 9.9, "9.9F"))),
                    1L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID2_2"),
                    InternalAggregations.from(
                        List.of(createSingleMetricAgg(aggName, 8.4), createSingleMetricAgg(aggName2, 222.33, "222.33F"))
                    ),
                    2L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID2", targetField2, "ID1_2"),
                    InternalAggregations.from(
                        List.of(createSingleMetricAgg(aggName, 28.99), createSingleMetricAgg(aggName2, -2.44, "-2.44F"))
                    ),
                    3L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID3", targetField2, "ID2_2"),
                    InternalAggregations.from(
                        List.of(createSingleMetricAgg(aggName, 12.55), createSingleMetricAgg(aggName2, Double.NaN, "NaN"))
                    ),
                    4L
                )
            )
        );

        List<Map<String, Object>> expected = List.of(
            asMap(targetField, "ID1", targetField2, "ID1_2", aggName, 42.33, aggName2, "9.9F"),
            asMap(targetField, "ID1", targetField2, "ID2_2", aggName, 8.4, aggName2, "222.33F"),
            asMap(targetField, "ID2", targetField2, "ID1_2", aggName, 28.99, aggName2, "-2.44F"),
            asMap(targetField, "ID3", targetField2, "ID2_2", aggName, 12.55, aggName2, null)
        );
        Map<String, String> fieldTypeMap = asStringMap(
            aggName,
            "double",
            aggName2,
            "keyword", // If the second aggregation was some non-numeric mapped field
            targetField,
            "keyword",
            targetField2,
            "keyword"
        );
        executeTest(groupBy, aggregationBuilders, List.of(), input, fieldTypeMap, expected, 10);
    }

    public void testExtractCompositeAggregationResultsWithDynamicType() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);
        String targetField2 = randomAlphaOfLengthBetween(5, 10) + "_2";

        GroupConfig groupBy = parseGroupConfig(Strings.format("""
            {
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              },
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              }
            }""", targetField, targetField2));

        String aggName = randomAlphaOfLengthBetween(5, 10);

        List<AggregationBuilder> aggregationBuilders = List.of(AggregationBuilders.scriptedMetric(aggName));

        InternalComposite input = createComposite(
            List.of(
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID1_2"),
                    InternalAggregations.from(List.of(createScriptedMetric(aggName, asMap("field", 123.0)))),
                    1L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID2_2"),
                    InternalAggregations.from(List.of(createScriptedMetric(aggName, asMap("field", 1.0)))),
                    2L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID2", targetField2, "ID1_2"),
                    InternalAggregations.from(List.of(createScriptedMetric(aggName, asMap("field", 2.13)))),
                    3L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID3", targetField2, "ID2_2"),
                    InternalAggregations.from(List.of(createScriptedMetric(aggName, null))),
                    0L
                )
            )
        );

        List<Map<String, Object>> expected = List.of(
            asMap(targetField, "ID1", targetField2, "ID1_2", aggName, asMap("field", 123.0)),
            asMap(targetField, "ID1", targetField2, "ID2_2", aggName, asMap("field", 1.0)),
            asMap(targetField, "ID2", targetField2, "ID1_2", aggName, asMap("field", 2.13)),
            asMap(targetField, "ID3", targetField2, "ID2_2", aggName, null)
        );
        Map<String, String> fieldTypeMap = asStringMap(targetField, "keyword", targetField2, "keyword");
        executeTest(groupBy, aggregationBuilders, List.of(), input, fieldTypeMap, expected, 6);
    }

    public void testExtractCompositeAggregationResultsWithPipelineAggregation() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);
        String targetField2 = randomAlphaOfLengthBetween(5, 10) + "_2";

        GroupConfig groupBy = parseGroupConfig(Strings.format("""
            {
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              },
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              }
            }""", targetField, targetField2));

        String aggName = randomAlphaOfLengthBetween(5, 10);
        String pipelineAggName = randomAlphaOfLengthBetween(5, 10) + "_2";

        List<AggregationBuilder> aggregationBuilders = List.of(AggregationBuilders.scriptedMetric(aggName));
        List<PipelineAggregationBuilder> pipelineAggregationBuilders = List.of(
            PipelineAggregatorBuilders.bucketScript(pipelineAggName, Map.of("param_1", aggName), new Script("return params.param_1"))
        );

        InternalComposite input = createComposite(
            List.of(
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID1_2"),
                    InternalAggregations.from(
                        List.of(createSingleMetricAgg(aggName, 123.0), createSingleMetricAgg(pipelineAggName, 123.0, "123.0"))
                    ),
                    1L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID2_2"),
                    InternalAggregations.from(
                        List.of(createSingleMetricAgg(aggName, 1.0), createSingleMetricAgg(pipelineAggName, 1.0, "1.0"))
                    ),
                    2L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID2", targetField2, "ID1_2"),
                    InternalAggregations.from(
                        List.of(createSingleMetricAgg(aggName, 2.13), createSingleMetricAgg(pipelineAggName, 2.13, "2.13"))
                    ),
                    3L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID3", targetField2, "ID2_2"),
                    InternalAggregations.from(
                        List.of(createSingleMetricAgg(aggName, 12.0), createSingleMetricAgg(pipelineAggName, Double.NaN, "NaN"))
                    ),
                    4L
                )
            )
        );

        List<Map<String, Object>> expected = List.of(
            asMap(targetField, "ID1", targetField2, "ID1_2", aggName, 123.0, pipelineAggName, 123.0),
            asMap(targetField, "ID1", targetField2, "ID2_2", aggName, 1.0, pipelineAggName, 1.0),
            asMap(targetField, "ID2", targetField2, "ID1_2", aggName, 2.13, pipelineAggName, 2.13),
            asMap(targetField, "ID3", targetField2, "ID2_2", aggName, 12.0, pipelineAggName, null)
        );
        Map<String, String> fieldTypeMap = asStringMap(targetField, "keyword", targetField2, "keyword", aggName, "double");
        executeTest(groupBy, aggregationBuilders, pipelineAggregationBuilders, input, fieldTypeMap, expected, 10);
    }

    public void testExtractCompositeAggregationResultsDocIDs() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);
        String targetField2 = randomAlphaOfLengthBetween(5, 10) + "_2";

        GroupConfig groupBy = parseGroupConfig(Strings.format("""
            {
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              },
              "%s": {
                "terms": {
                  "field": "doesn't_matter_for_this_test"
                }
              }
            }""", targetField, targetField2));

        String aggName = randomAlphaOfLengthBetween(5, 10);
        List<AggregationBuilder> aggregationBuilders = List.of(AggregationBuilders.avg(aggName));

        InternalComposite inputFirstRun = createComposite(
            List.of(
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID1_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 42.33))),
                    1L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID2_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 8.4))),
                    2L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID2", targetField2, "ID1_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 28.99))),
                    3L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID3", targetField2, "ID2_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 12.55))),
                    4L
                )
            )
        );

        InternalComposite inputSecondRun = createComposite(
            List.of(
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID1_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 433.33))),
                    12L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID1", targetField2, "ID2_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 83.4))),
                    32L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID2", targetField2, "ID1_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 21.99))),
                    2L
                ),
                createInternalCompositeBucket(
                    asMap(targetField, "ID3", targetField2, "ID2_2"),
                    InternalAggregations.from(List.of(createSingleMetricAgg(aggName, 122.55))),
                    44L
                )
            )
        );

        TransformIndexerStats stats = new TransformIndexerStats();
        TransformProgress progress = new TransformProgress();

        Map<String, String> fieldTypeMap = asStringMap(aggName, "double", targetField, "keyword", targetField2, "keyword");

        List<Map<String, Object>> resultFirstRun = runExtraction(
            groupBy,
            aggregationBuilders,
            List.of(),
            inputFirstRun,
            fieldTypeMap,
            stats,
            progress
        );
        List<Map<String, Object>> resultSecondRun = runExtraction(
            groupBy,
            aggregationBuilders,
            List.of(),
            inputSecondRun,
            fieldTypeMap,
            stats,
            progress
        );

        assertNotEquals(resultFirstRun, resultSecondRun);

        Set<String> documentIdsFirstRun = new HashSet<>();
        resultFirstRun.forEach(m -> { documentIdsFirstRun.add((String) m.get(TransformField.DOCUMENT_ID_FIELD)); });

        assertEquals(4, documentIdsFirstRun.size());

        Set<String> documentIdsSecondRun = new HashSet<>();
        resultSecondRun.forEach(m -> { documentIdsSecondRun.add((String) m.get(TransformField.DOCUMENT_ID_FIELD)); });

        assertEquals(4, documentIdsSecondRun.size());
        assertEquals(documentIdsFirstRun, documentIdsSecondRun);
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDocument() {
        Map<String, Object> document = new HashMap<>();

        AggregationResultUtils.updateDocument(document, "foo.bar.baz", 1000L);
        AggregationResultUtils.updateDocument(document, "foo.bar.baz2", 2000L);
        AggregationResultUtils.updateDocument(document, "bar.field1", 1L);
        AggregationResultUtils.updateDocument(document, "metric", 10L);

        assertThat(document.get("metric"), equalTo(10L));

        Map<String, Object> bar = (Map<String, Object>) document.get("bar");

        assertThat(bar.get("field1"), equalTo(1L));

        Map<String, Object> foo = (Map<String, Object>) document.get("foo");
        Map<String, Object> foobar = (Map<String, Object>) foo.get("bar");

        assertThat(foobar.get("baz"), equalTo(1000L));
        assertThat(foobar.get("baz2"), equalTo(2000L));
    }

    public void testUpdateDocumentWithDuplicate() {
        Map<String, Object> document = new HashMap<>();

        AggregationResultUtils.updateDocument(document, "foo.bar.baz", 1000L);
        AggregationResultUtils.AggregationExtractionException exception = expectThrows(
            AggregationResultUtils.AggregationExtractionException.class,
            () -> AggregationResultUtils.updateDocument(document, "foo.bar.baz", 2000L)
        );
        assertThat(exception.getMessage(), equalTo("duplicate key value pairs key [foo.bar.baz] old value [1000] duplicate value [2000]"));
    }

    public void testUpdateDocumentWithObjectAndNotObject() {
        Map<String, Object> document = new HashMap<>();

        AggregationResultUtils.updateDocument(document, "foo.bar.baz", 1000L);
        AggregationResultUtils.AggregationExtractionException exception = expectThrows(
            AggregationResultUtils.AggregationExtractionException.class,
            () -> AggregationResultUtils.updateDocument(document, "foo.bar", 2000L)
        );
        assertThat(exception.getMessage(), equalTo("mixed object types of nested and non-nested fields [foo.bar]"));
    }

    public static InternalNumericMetricsAggregation.SingleValue createSingleMetricAgg(String name, Double value) {
        InternalNumericMetricsAggregation.SingleValue agg = mock(InternalNumericMetricsAggregation.SingleValue.class);
        when(agg.value()).thenReturn(value);
        when(agg.getName()).thenReturn(name);
        return agg;
    }

    public static InternalNumericMetricsAggregation.SingleValue createSingleMetricAgg(String name, Double value, String valueAsString) {
        InternalNumericMetricsAggregation.SingleValue agg = createSingleMetricAgg(name, value);
        when(agg.getValueAsString()).thenReturn(valueAsString);
        return agg;
    }

    public void testSingleValueAggExtractor() {
        Aggregation agg = createSingleMetricAgg("metric", Double.NaN, "NaN");
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of("metric", "double"), ""), is(nullValue()));

        agg = createSingleMetricAgg("metric", Double.POSITIVE_INFINITY, "NaN");
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of("metric", "double"), ""), is(nullValue()));

        agg = createSingleMetricAgg("metric", 100.0, "100.0");
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of("metric", "double"), ""), equalTo(100.0));

        agg = createSingleMetricAgg("metric", 100.0, "one_hundred");
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of("metric", "double"), ""), equalTo(100.0));

        agg = createSingleMetricAgg("metric", 100.0, "one_hundred");
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of("metric", "string"), ""), equalTo("one_hundred"));

        agg = createSingleMetricAgg("metric", 100.0, "one_hundred");
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of("metric", "unsigned_long"), ""), equalTo(100L));
    }

    public void testMultiValueAggExtractor() {
        Aggregation agg = new TestMultiValueAggregation("mv_metric", Map.of("ip", "192.168.1.1"));

        assertThat(
            AggregationResultUtils.getExtractor(agg).value(agg, Map.of("mv_metric.ip", "ip"), ""),
            equalTo(Map.of("ip", "192.168.1.1"))
        );

        agg = new TestMultiValueAggregation("mv_metric", Map.of("top_answer", "fortytwo"));

        assertThat(
            AggregationResultUtils.getExtractor(agg).value(agg, Map.of("mv_metric.written_answer", "written_answer"), ""),
            equalTo(Map.of("top_answer", "fortytwo"))
        );

        agg = new TestMultiValueAggregation("mv_metric", Map.of("ip", "192.168.1.1", "top_answer", "fortytwo"));

        assertThat(
            AggregationResultUtils.getExtractor(agg).value(agg, Map.of("mv_metric.top_answer", "keyword", "mv_metric.ip", "ip"), ""),
            equalTo(Map.of("top_answer", "fortytwo", "ip", "192.168.1.1"))
        );
    }

    public void testNumericMultiValueAggExtractor() {
        Aggregation agg = new TestNumericMultiValueAggregation("mv_metric", Map.of("approx_answer", 42.2));

        assertThat(
            AggregationResultUtils.getExtractor(agg).value(agg, Map.of("mv_metric.approx_answer", "double"), ""),
            equalTo(Map.of("approx_answer", Double.valueOf(42.2)))
        );

        agg = new TestNumericMultiValueAggregation("mv_metric", Map.of("exact_answer", 42.0));

        assertThat(
            AggregationResultUtils.getExtractor(agg).value(agg, Map.of("mv_metric.exact_answer", "long"), ""),
            equalTo(Map.of("exact_answer", 42L))
        );

        agg = new TestNumericMultiValueAggregation(
            "mv_metric",
            Map.of("approx_answer", Double.valueOf(42.2), "exact_answer", Double.valueOf(42.0))
        );

        assertThat(
            AggregationResultUtils.getExtractor(agg)
                .value(agg, Map.of("mv_metric.approx_answer", "double", "mv_metric.exact_answer", "long"), ""),
            equalTo(Map.of("approx_answer", Double.valueOf(42.2), "exact_answer", Long.valueOf(42)))
        );

        assertThat(
            AggregationResultUtils.getExtractor(agg)
                .value(agg, Map.of("filter.mv_metric.approx_answer", "double", "filter.mv_metric.exact_answer", "long"), "filter"),
            equalTo(Map.of("approx_answer", 42.2, "exact_answer", Long.valueOf(42)))
        );
    }

    private InternalScriptedMetric createScriptedMetric(String name, Object returnValue) {
        InternalScriptedMetric agg = mock(InternalScriptedMetric.class);
        when(agg.getName()).thenReturn(name);
        when(agg.aggregation()).thenReturn(returnValue);
        return agg;
    }

    @SuppressWarnings("unchecked")
    public void testScriptedMetricAggExtractor() {
        Aggregation agg = createScriptedMetric("name", null);
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), is(nullValue()));

        agg = createScriptedMetric("name", List.of("values"));
        Object val = AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), "");
        assertThat((List<String>) val, hasItem("values"));

        agg = createScriptedMetric("name", Map.of("key", 100));
        val = AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), "");
        assertThat(((Map<String, Object>) val).get("key"), equalTo(100));
    }

    private GeoCentroid createGeoCentroid(GeoPoint point, long count) {
        GeoCentroid agg = mock(GeoCentroid.class);
        when(agg.centroid()).thenReturn(point);
        when(agg.count()).thenReturn(count);
        return agg;
    }

    public void testGeoCentroidAggExtractor() {
        Aggregation agg = createGeoCentroid(null, 0);
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), is(nullValue()));

        agg = createGeoCentroid(new GeoPoint(100.0, 101.0), 0);
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), is(nullValue()));

        agg = createGeoCentroid(new GeoPoint(100.0, 101.0), randomIntBetween(1, 100));
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), equalTo("100.0, 101.0"));
    }

    private GeoBounds createGeoBounds(GeoPoint tl, GeoPoint br) {
        GeoBounds agg = mock(GeoBounds.class);
        when(agg.bottomRight()).thenReturn(br);
        when(agg.topLeft()).thenReturn(tl);
        return agg;
    }

    @SuppressWarnings("unchecked")
    public void testGeoBoundsAggExtractor() {
        final int numberOfRuns = 25;
        Aggregation agg = createGeoBounds(null, new GeoPoint(100.0, 101.0));
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), is(nullValue()));

        agg = createGeoBounds(new GeoPoint(100.0, 101.0), null);
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), is(nullValue()));

        String type = "point";
        for (int i = 0; i < numberOfRuns; i++) {
            Map<String, Object> expectedObject = new HashMap<>();
            expectedObject.put("type", type);
            double lat = randomDoubleBetween(-90.0, 90.0, false);
            double lon = randomDoubleBetween(-180.0, 180.0, false);
            expectedObject.put("coordinates", List.of(lon, lat));
            agg = createGeoBounds(new GeoPoint(lat, lon), new GeoPoint(lat, lon));
            assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), equalTo(expectedObject));
        }

        type = "linestring";
        for (int i = 0; i < numberOfRuns; i++) {
            double lat = randomDoubleBetween(-90.0, 90.0, false);
            double lon = randomDoubleBetween(-180.0, 180.0, false);
            double lat2 = lat;
            double lon2 = lon;
            if (randomBoolean()) {
                lat2 = randomDoubleBetween(-90.0, 90.0, false);
            } else {
                lon2 = randomDoubleBetween(-180.0, 180.0, false);
            }
            agg = createGeoBounds(new GeoPoint(lat, lon), new GeoPoint(lat2, lon2));
            Object val = AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), "");
            Map<String, Object> geoJson = (Map<String, Object>) val;
            assertThat(geoJson.get("type"), equalTo(type));
            List<Double[]> coordinates = (List<Double[]>) geoJson.get("coordinates");
            for (Double[] coor : coordinates) {
                assertThat(coor.length, equalTo(2));
            }
            assertThat(coordinates.get(0)[0], equalTo(lon));
            assertThat(coordinates.get(0)[1], equalTo(lat));
            assertThat(coordinates.get(1)[0], equalTo(lon2));
            assertThat(coordinates.get(1)[1], equalTo(lat2));
        }

        type = "polygon";
        for (int i = 0; i < numberOfRuns; i++) {
            double lat = randomDoubleBetween(-90.0, 90.0, false);
            double lon = randomDoubleBetween(-180.0, 180.0, false);
            double lat2 = randomDoubleBetween(-90.0, 90.0, false);
            double lon2 = randomDoubleBetween(-180.0, 180.0, false);
            while (Double.compare(lat, lat2) == 0 || Double.compare(lon, lon2) == 0) {
                lat2 = randomDoubleBetween(-90.0, 90.0, false);
                lon2 = randomDoubleBetween(-180.0, 180.0, false);
            }
            agg = createGeoBounds(new GeoPoint(lat, lon), new GeoPoint(lat2, lon2));
            Object val = AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), "");
            Map<String, Object> geoJson = (Map<String, Object>) val;
            assertThat(geoJson.get("type"), equalTo(type));
            List<List<Double[]>> coordinates = (List<List<Double[]>>) geoJson.get("coordinates");
            assertThat(coordinates.size(), equalTo(1));
            assertThat(coordinates.get(0).size(), equalTo(5));
            List<List<Double>> expected = List.of(
                List.of(lon, lat),
                List.of(lon2, lat),
                List.of(lon2, lat2),
                List.of(lon, lat2),
                List.of(lon, lat)
            );
            for (int j = 0; j < 5; j++) {
                Double[] coordinate = coordinates.get(0).get(j);
                assertThat(coordinate.length, equalTo(2));
                assertThat(coordinate[0], equalTo(expected.get(j).get(0)));
                assertThat(coordinate[1], equalTo(expected.get(j).get(1)));
            }
        }
    }

    private static InternalComposite createComposite(List<InternalComposite.InternalBucket> buckets) {
        InternalComposite composite = mock(InternalComposite.class);

        when(composite.getBuckets()).thenReturn(buckets);
        when(composite.getName()).thenReturn("my_feature");
        Map<String, Object> afterKey = buckets.get(buckets.size() - 1).getKey();
        when(composite.afterKey()).thenReturn(afterKey);
        return composite;
    }

    private static InternalComposite.InternalBucket createInternalCompositeBucket(
        Map<String, Object> key,
        InternalAggregations aggregations,
        long docCount
    ) {
        InternalComposite.InternalBucket bucket = mock(InternalComposite.InternalBucket.class);
        when(bucket.getDocCount()).thenReturn(docCount);
        when(bucket.getAggregations()).thenReturn(aggregations);
        when(bucket.getKey()).thenReturn(key);
        return bucket;
    }

    public static Percentiles createPercentilesAgg(String name, List<Percentile> percentiles) {
        Percentiles agg = mock(Percentiles.class);

        when(agg.iterator()).thenReturn(percentiles.iterator());
        when(agg.getName()).thenReturn(name);
        return agg;
    }

    public void testPercentilesAggExtractor() {
        Aggregation agg = createPercentilesAgg(
            "p_agg",
            List.of(new Percentile(1, 0), new Percentile(50, 22.2), new Percentile(99, 43.3), new Percentile(99.5, 100.3))
        );
        assertThat(
            AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""),
            equalTo(asMap("1", 0.0, "50", 22.2, "99", 43.3, "99_5", 100.3))
        );
    }

    public void testPercentilesAggExtractorNaN() {
        Aggregation agg = createPercentilesAgg("p_agg", List.of(new Percentile(1, Double.NaN), new Percentile(50, Double.NaN)));
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), equalTo(asMap("1", null, "50", null)));
    }

    @SuppressWarnings("unchecked")
    public static Range createRangeAgg(String name, List<InternalRange.Bucket> buckets) {
        Range agg = mock(Range.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getBuckets()).thenReturn((List) buckets);
        return agg;
    }

    public void testRangeAggExtractor() {
        Aggregation agg = createRangeAgg(
            "p_agg",
            List.of(
                new InternalRange.Bucket(null, Double.NEGATIVE_INFINITY, 10.5, 10, InternalAggregations.EMPTY, false, DocValueFormat.RAW),
                new InternalRange.Bucket(null, 10.5, 19.5, 30, InternalAggregations.EMPTY, false, DocValueFormat.RAW),
                new InternalRange.Bucket(null, 19.5, 200, 30, InternalAggregations.EMPTY, false, DocValueFormat.RAW),
                new InternalRange.Bucket(null, 20, Double.POSITIVE_INFINITY, 0, InternalAggregations.EMPTY, false, DocValueFormat.RAW),
                new InternalRange.Bucket(null, -10, -5, 0, InternalAggregations.EMPTY, false, DocValueFormat.RAW),
                new InternalRange.Bucket(null, -11.0, -6.0, 0, InternalAggregations.EMPTY, false, DocValueFormat.RAW),
                new InternalRange.Bucket(null, -11.0, 0, 0, InternalAggregations.EMPTY, false, DocValueFormat.RAW),
                new InternalRange.Bucket("custom-0", 0, 10, 777, InternalAggregations.EMPTY, false, DocValueFormat.RAW)
            )
        );
        assertThat(
            AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""),
            equalTo(
                asMap(
                    "*-10_5",
                    10L,
                    "10_5-19_5",
                    30L,
                    "19_5-200",
                    30L,
                    "20-*",
                    0L,
                    "-10--5",
                    0L,
                    "-11--6",
                    0L,
                    "-11-0",
                    0L,
                    "custom-0",
                    777L
                )
            )
        );
    }

    public static SingleBucketAggregation createSingleBucketAgg(String name, long docCount, Aggregation... subAggregations) {
        SingleBucketAggregation agg = mock(SingleBucketAggregation.class);
        when(agg.getDocCount()).thenReturn(docCount);
        when(agg.getName()).thenReturn(name);
        if (subAggregations != null) {
            org.elasticsearch.search.aggregations.Aggregations subAggs = new org.elasticsearch.search.aggregations.Aggregations(
                List.of(subAggregations)
            );
            when(agg.getAggregations()).thenReturn(subAggs);
        } else {
            when(agg.getAggregations()).thenReturn(null);
        }
        return agg;
    }

    public void testSingleBucketAggExtractor() {
        Aggregation agg = createSingleBucketAgg("sba", 42L);
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), equalTo(42L));

        agg = createSingleBucketAgg("sba1", 42L, createSingleMetricAgg("sub1", 100.0, "100.0"));
        assertThat(AggregationResultUtils.getExtractor(agg).value(agg, Map.of(), ""), equalTo(Map.of("sub1", 100.0)));

        agg = createSingleBucketAgg(
            "sba2",
            42L,
            createSingleMetricAgg("sub1", 100.0, "hundred"),
            createSingleMetricAgg("sub2", 33.33, "thirty_three")
        );
        assertThat(
            AggregationResultUtils.getExtractor(agg).value(agg, asStringMap("sba2.sub1", "long", "sba2.sub2", "float"), ""),
            equalTo(asMap("sub1", 100L, "sub2", 33.33))
        );

        agg = createSingleBucketAgg(
            "sba3",
            42L,
            createSingleMetricAgg("sub1", 100.0, "hundred"),
            createSingleMetricAgg("sub2", 33.33, "thirty_three"),
            createSingleBucketAgg("sub3", 42L)
        );
        assertThat(
            AggregationResultUtils.getExtractor(agg).value(agg, asStringMap("sba3.sub1", "long", "sba3.sub2", "double"), ""),
            equalTo(asMap("sub1", 100L, "sub2", 33.33, "sub3", 42L))
        );

        agg = createSingleBucketAgg(
            "sba4",
            42L,
            createSingleMetricAgg("sub1", 100.0, "hundred"),
            createSingleMetricAgg("sub2", 33.33, "thirty_three"),
            createSingleBucketAgg("sub3", 42L, createSingleMetricAgg("subsub1", 11.1, "eleven_dot_eleven"))
        );
        assertThat(
            AggregationResultUtils.getExtractor(agg)
                .value(agg, asStringMap("sba4.sub3.subsub1", "double", "sba4.sub2", "float", "sba4.sub1", "long"), ""),
            equalTo(asMap("sub1", 100L, "sub2", 33.33, "sub3", asMap("subsub1", 11.1)))
        );
    }

    public void testDefaultBucketKeyExtractor() {
        BucketKeyExtractor extractor = new AggregationResultUtils.DefaultBucketKeyExtractor();

        assertThat(extractor.value(42.0, "long"), equalTo(42L));
        assertThat(extractor.value(42.2, "double"), equalTo(42.2));
        assertThat(extractor.value(1577836800000L, "date"), equalTo("2020-01-01T00:00:00.000Z"));
        assertThat(extractor.value(1577836800000L, "date_nanos"), equalTo("2020-01-01T00:00:00.000Z"));
        assertThat(extractor.value(1577836800000L, "long"), equalTo(1577836800000L));
        assertThat(extractor.value(1577836800000L, null), equalTo(1577836800000L));
    }

    public void testDatesAsEpochBucketKeyExtractor() {
        BucketKeyExtractor extractor = new AggregationResultUtils.DatesAsEpochBucketKeyExtractor();

        assertThat(extractor.value(42.0, "long"), equalTo(42L));
        assertThat(extractor.value(42.2, "double"), equalTo(42.2));
        assertThat(extractor.value(1577836800000L, "date"), equalTo(1577836800000L));
        assertThat(extractor.value(1577836800000L, "date_nanos"), equalTo(1577836800000L));
        assertThat(extractor.value(1577836800000L, "long"), equalTo(1577836800000L));
        assertThat(extractor.value(1577836800000L, null), equalTo(1577836800000L));
    }

    private void executeTest(
        GroupConfig groups,
        List<AggregationBuilder> aggregationBuilders,
        List<PipelineAggregationBuilder> pipelineAggregationBuilders,
        InternalComposite input,
        Map<String, String> fieldTypeMap,
        List<Map<String, Object>> expected,
        long expectedDocCounts
    ) {
        TransformIndexerStats stats = new TransformIndexerStats();
        TransformProgress progress = new TransformProgress();

        List<Map<String, Object>> result = runExtraction(
            groups,
            aggregationBuilders,
            pipelineAggregationBuilders,
            input,
            fieldTypeMap,
            stats,
            progress
        );

        // remove the document ids and test uniqueness
        Set<String> documentIds = new HashSet<>();
        result.forEach(m -> { documentIds.add((String) m.remove(TransformField.DOCUMENT_ID_FIELD)); });

        assertEquals(result.size(), documentIds.size());
        assertEquals(expected, result);
        assertEquals(expectedDocCounts, stats.getNumDocuments());

    }

    private List<Map<String, Object>> runExtraction(
        GroupConfig groups,
        List<AggregationBuilder> aggregationBuilders,
        List<PipelineAggregationBuilder> pipelineAggregationBuilders,
        InternalComposite input,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats stats,
        TransformProgress progress
    ) {
        return AggregationResultUtils.extractCompositeAggregationResults(
            input,
            groups,
            aggregationBuilders,
            pipelineAggregationBuilders,
            fieldTypeMap,
            stats,
            progress,
            true
        ).collect(Collectors.toList());
    }

    private GroupConfig parseGroupConfig(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), json);
        return GroupConfig.fromXContent(parser, false);
    }

    static Map<String, Object> asMap(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            String field = (String) fields[i];
            map.put(field, fields[i + 1]);
        }
        return map;
    }

    static Map<String, String> asStringMap(String... strings) {
        assert strings.length % 2 == 0;
        final Map<String, String> map = new HashMap<>();
        for (int i = 0; i < strings.length; i += 2) {
            String field = strings[i];
            map.put(field, strings[i + 1]);
        }
        return map;
    }
}
