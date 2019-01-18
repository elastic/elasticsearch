/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.ParsedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.dataframe.transforms.pivot.TermsGroupSource;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.dataframe.transforms.pivot.SingleGroupSource.Type.TERMS;

public class AggregationResultUtilsTests extends ESTestCase {

    private final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(namedXContents);

    private final String KEY = Aggregation.CommonFields.KEY.getPreferredName();
    private final String DOC_COUNT = Aggregation.CommonFields.DOC_COUNT.getPreferredName();

    // aggregations potentially useful for writing tests, to be expanded as necessary
    private static final List<NamedXContentRegistry.Entry> namedXContents;
    static {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        map.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        map.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        map.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        map.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        map.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        map.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        map.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        map.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        map.put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c));
        map.put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c));

        namedXContents = map.entrySet().stream()
                .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    public void testExtractCompositeAggregationResults() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);

        List<GroupConfig> sources = Collections.singletonList(
                new GroupConfig(targetField, TERMS, new TermsGroupSource("doesn't_matter_for_this_test"))
                );

        String aggName = randomAlphaOfLengthBetween(5, 10);
        String aggTypedName = "avg#" + aggName;
        Collection<AggregationBuilder> aggregationBuilders = Collections.singletonList(AggregationBuilders.avg(aggName));

        Map<String, Object> input = asMap(
                "buckets",
                    asList(
                            asMap(
                                  KEY, asMap(
                                          targetField, "ID1"),
                                  aggTypedName, asMap(
                                          "value", 42.33),
                                  DOC_COUNT, 8),
                            asMap(
                                  KEY, asMap(
                                          targetField, "ID2"),
                                  aggTypedName, asMap(
                                          "value", 28.99),
                                  DOC_COUNT, 3),
                            asMap(
                                  KEY, asMap(
                                          targetField, "ID3"),
                                  aggTypedName, asMap(
                                          "value", 12.55),
                                  DOC_COUNT, 9)
                    ));

        List<Map<String, Object>> expected = asList(
                asMap(
                        targetField, "ID1",
                        aggName, 42.33
                        ),
                asMap(
                        targetField, "ID2",
                        aggName, 28.99
                        ),
                asMap(
                        targetField, "ID3",
                        aggName, 12.55
                        )
                );

        executeTest(sources, aggregationBuilders, input, expected, 20);
    }

    public void testExtractCompositeAggregationResultsMultiSources() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);
        String targetField2 = randomAlphaOfLengthBetween(5, 10) + "_2";

        List<GroupConfig> sources = asList(
                new GroupConfig(targetField, TERMS, new TermsGroupSource("doesn't_matter_for_this_test")),
                new GroupConfig(targetField2, TERMS, new TermsGroupSource("doesn't_matter_for_this_test"))
                );

        String aggName = randomAlphaOfLengthBetween(5, 10);
        String aggTypedName = "avg#" + aggName;
        Collection<AggregationBuilder> aggregationBuilders = Collections.singletonList(AggregationBuilders.avg(aggName));

        Map<String, Object> input = asMap(
                "buckets",
                    asList(
                            asMap(
                                  KEY, asMap(
                                          targetField, "ID1",
                                          targetField2, "ID1_2"
                                          ),
                                  aggTypedName, asMap(
                                          "value", 42.33),
                                  DOC_COUNT, 1),
                            asMap(
                                    KEY, asMap(
                                            targetField, "ID1",
                                            targetField2, "ID2_2"
                                            ),
                                    aggTypedName, asMap(
                                            "value", 8.4),
                                    DOC_COUNT, 2),
                            asMap(
                                  KEY, asMap(
                                          targetField, "ID2",
                                          targetField2, "ID1_2"
                                          ),
                                  aggTypedName, asMap(
                                          "value", 28.99),
                                  DOC_COUNT, 3),
                            asMap(
                                  KEY, asMap(
                                          targetField, "ID3",
                                          targetField2, "ID2_2"
                                          ),
                                  aggTypedName, asMap(
                                          "value", 12.55),
                                  DOC_COUNT, 4)
                    ));

        List<Map<String, Object>> expected = asList(
                asMap(
                        targetField, "ID1",
                        targetField2, "ID1_2",
                        aggName, 42.33
                        ),
                asMap(
                        targetField, "ID1",
                        targetField2, "ID2_2",
                        aggName, 8.4
                        ),
                asMap(
                        targetField, "ID2",
                        targetField2, "ID1_2",
                        aggName, 28.99
                        ),
                asMap(
                        targetField, "ID3",
                        targetField2, "ID2_2",
                        aggName, 12.55
                        )
                );
        executeTest(sources, aggregationBuilders, input, expected, 10);
    }

    public void testExtractCompositeAggregationResultsMultiAggregations() throws IOException {
        String targetField = randomAlphaOfLengthBetween(5, 10);

        List<GroupConfig> sources = Collections.singletonList(
                new GroupConfig(targetField, TERMS, new TermsGroupSource("doesn't_matter_for_this_test"))
                );

        String aggName = randomAlphaOfLengthBetween(5, 10);
        String aggTypedName = "avg#" + aggName;

        String aggName2 = randomAlphaOfLengthBetween(5, 10) + "_2";
        String aggTypedName2 = "max#" + aggName2;

        Collection<AggregationBuilder> aggregationBuilders = asList(AggregationBuilders.avg(aggName), AggregationBuilders.max(aggName2));

        Map<String, Object> input = asMap(
                "buckets",
                    asList(
                            asMap(
                                  KEY, asMap(
                                          targetField, "ID1"),
                                  aggTypedName, asMap(
                                          "value", 42.33),
                                  aggTypedName2, asMap(
                                          "value", 9.9),
                                  DOC_COUNT, 111),
                            asMap(
                                  KEY, asMap(
                                          targetField, "ID2"),
                                  aggTypedName, asMap(
                                          "value", 28.99),
                                  aggTypedName2, asMap(
                                          "value", 222.33),
                                  DOC_COUNT, 88),
                            asMap(
                                  KEY, asMap(
                                          targetField, "ID3"),
                                  aggTypedName, asMap(
                                          "value", 12.55),
                                  aggTypedName2, asMap(
                                          "value", -2.44),
                                  DOC_COUNT, 1)
                    ));

        List<Map<String, Object>> expected = asList(
                asMap(
                        targetField, "ID1",
                        aggName, 42.33,
                        aggName2, 9.9
                        ),
                asMap(
                        targetField, "ID2",
                        aggName, 28.99,
                        aggName2, 222.33
                        ),
                asMap(
                        targetField, "ID3",
                        aggName, 12.55,
                        aggName2, -2.44
                        )
                );
        executeTest(sources, aggregationBuilders, input, expected, 200);
    }

    private void executeTest(Iterable<GroupConfig> sources, Collection<AggregationBuilder> aggregationBuilders, Map<String, Object> input,
            List<Map<String, Object>> expected, long expectedDocCounts) throws IOException {
        DataFrameIndexerTransformStats stats = new DataFrameIndexerTransformStats();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        builder.map(input);

        try (XContentParser parser = createParser(builder)) {
            CompositeAggregation agg = ParsedComposite.fromXContent(parser, "my_feature");
            List<Map<String, Object>> result = AggregationResultUtils
                    .extractCompositeAggregationResults(agg, sources, aggregationBuilders, stats).collect(Collectors.toList());

            assertEquals(expected, result);
            assertEquals(expectedDocCounts, stats.getNumDocuments());
        }
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
}
