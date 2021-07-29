/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRange;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class MlAggsHelperTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(InternalAggregationTestCase.getDefaultNamedXContents());
    }

    public void testExtractSimpleCountPath() {
        InternalDateRange agg = new InternalDateRange.Factory().create(
            "daily",
            Arrays.asList(
                new InternalDateRange.Bucket(
                    "*-2021-07-19T00:00:00.000Z",
                    Double.NaN,
                    1.6266528E12,
                    0,
                    Collections.singletonList(new InternalAvg("ticketPrice", 0L, 0L, DocValueFormat.RAW, Collections.emptyMap())),
                    true,
                    DocValueFormat.RAW
                ),
                new InternalDateRange.Bucket(
                    "2021-07-19T00:00:00.000Z-2021-07-19T01:00:00.000Z",
                    1.6266528E12,
                    1.6266564E12,
                    1,
                    Collections.singletonList(new InternalAvg("ticketPrice", 841.265625, 1L, DocValueFormat.RAW, Collections.emptyMap())),
                    true,
                    DocValueFormat.RAW
                )
            ),
            DocValueFormat.RAW,
            true,
            Collections.emptyMap()
        );

        MlAggsHelper.DoubleBucketValues deValues = MlAggsHelper.extractDoubleBucketedValues(
            "daily>_count",
            new Aggregations(Collections.singletonList(agg))
        ).orElseThrow(() -> new AssertionError("extracted bucket values were null"));

        assertThat(deValues.getValues()[0], equalTo(0.0));
        assertThat(deValues.getDocCounts()[0], equalTo(0L));
        assertThat(deValues.getValues()[1], equalTo(1.0));
        assertThat(deValues.getDocCounts()[1], equalTo(1L));
    }

    public void testExtractDoubleBucketedValuesFromKeyedPath() {
        InternalFilters internalFilters = new InternalFilters(
            "airline",
            Arrays.asList(
                new InternalFilters.InternalBucket(
                    "DE",
                    14,
                    InternalAggregations.from(
                        Arrays.asList(
                            new InternalDateRange.Factory().create(
                                "daily",
                                Arrays.asList(
                                    new InternalDateRange.Bucket(
                                        "*-2021-07-19T00:00:00.000Z",
                                        Double.NaN,
                                        1.6266528E12,
                                        0,
                                        Collections.singletonList(
                                            new InternalAvg("ticketPrice", 0L, 0L, DocValueFormat.RAW, Collections.emptyMap())
                                        ),
                                        true,
                                        DocValueFormat.RAW
                                    ),
                                    new InternalDateRange.Bucket(
                                        "2021-07-19T00:00:00.000Z-2021-07-19T01:00:00.000Z",
                                        1.6266528E12,
                                        1.6266564E12,
                                        1,
                                        Collections.singletonList(
                                            new InternalAvg("ticketPrice", 841.265625, 1L, DocValueFormat.RAW, Collections.emptyMap())
                                        ),
                                        true,
                                        DocValueFormat.RAW
                                    )
                                ),
                                DocValueFormat.RAW,
                                true,
                                Collections.emptyMap()
                            )
                        )
                    ),
                    true
                ),
                new InternalFilters.InternalBucket(
                    "US",
                    49,
                    InternalAggregations.from(
                        Arrays.asList(
                            new InternalDateRange.Factory().create(
                                "daily",
                                Arrays.asList(
                                    new InternalDateRange.Bucket(
                                        "*-2021-07-19T00:00:00.000Z",
                                        Double.NaN,
                                        1.6266528E12,
                                        0,
                                        Collections.singletonList(
                                            new InternalAvg("ticketPrice", 0L, 0L, DocValueFormat.RAW, Collections.emptyMap())
                                        ),
                                        true,
                                        DocValueFormat.RAW
                                    ),
                                    new InternalDateRange.Bucket(
                                        "2021-07-19T00:00:00.000Z-2021-07-19T01:00:00.000Z",
                                        1.6266528E12,
                                        1.6266564E12,
                                        2,
                                        Collections.singletonList(
                                            new InternalAvg("ticketPrice", 932.091796875, 2L, DocValueFormat.RAW, Collections.emptyMap())
                                        ),
                                        true,
                                        DocValueFormat.RAW
                                    )
                                ),
                                DocValueFormat.RAW,
                                true,
                                Collections.emptyMap()
                            )
                        )
                    ),
                    true
                )
            ),
            true,
            Collections.emptyMap()
        );
        MlAggsHelper.DoubleBucketValues deValues = MlAggsHelper.extractDoubleBucketedValues(
            "airline['DE']>daily>ticketPrice",
            new Aggregations(Collections.singletonList(internalFilters))
        ).orElseThrow(() -> new AssertionError("extracted bucket values were null"));

        assertThat(deValues.getValues()[0], equalTo(0.0));
        assertThat(deValues.getDocCounts()[0], equalTo(0L));
        assertThat(deValues.getValues()[1], equalTo(841.265625));
        assertThat(deValues.getDocCounts()[1], equalTo(1L));

        MlAggsHelper.DoubleBucketValues deValuesCount = MlAggsHelper.extractDoubleBucketedValues(
            "airline['DE']>daily._count",
            new Aggregations(Collections.singletonList(internalFilters))
        ).orElseThrow(() -> new AssertionError("extracted bucket values were null"));

        assertThat(deValuesCount.getValues()[0], equalTo(0.0));
        assertThat(deValuesCount.getDocCounts()[0], equalTo(0L));
        assertThat(deValuesCount.getValues()[1], equalTo(1.0));
        assertThat(deValuesCount.getDocCounts()[1], equalTo(1L));

        MlAggsHelper.DoubleBucketValues usValues = MlAggsHelper.extractDoubleBucketedValues(
            "airline['US']>daily>ticketPrice",
            new Aggregations(Collections.singletonList(internalFilters))
        ).orElseThrow(() -> new AssertionError("extracted bucket values were null"));
        assertThat(usValues.getValues()[0], equalTo(0.0));
        assertThat(usValues.getDocCounts()[0], equalTo(0L));
        assertThat(usValues.getValues()[1], equalTo(466.0458984375));
        assertThat(usValues.getDocCounts()[1], equalTo(2L));

        MlAggsHelper.DoubleBucketValues usValuesCount = MlAggsHelper.extractDoubleBucketedValues(
            "airline['US']>daily._count",
            new Aggregations(Collections.singletonList(internalFilters))
        ).orElseThrow(() -> new AssertionError("extracted bucket values were null"));
        assertThat(usValuesCount.getValues()[0], equalTo(0.0));
        assertThat(usValuesCount.getDocCounts()[0], equalTo(0L));
        assertThat(usValuesCount.getValues()[1], equalTo(2.0));
        assertThat(usValuesCount.getDocCounts()[1], equalTo(2L));
    }

    public void testExtractDoubleBucketedValuesFromKeyedAndSingleBucketAggPath() {
        InternalFilters internalFilters = new InternalFilters(
            "airline",
            Arrays.asList(
                new InternalFilters.InternalBucket(
                    "DE",
                    14,
                    InternalAggregations.from(
                        Arrays.asList(
                            new InternalFilter(
                                "foo",
                                1,
                                InternalAggregations.from(
                                    Arrays.asList(
                                        new InternalDateRange.Factory().create(
                                            "daily",
                                            Arrays.asList(
                                                new InternalDateRange.Bucket(
                                                    "*-2021-07-19T00:00:00.000Z",
                                                    Double.NaN,
                                                    1.6266528E12,
                                                    0,
                                                    Collections.singletonList(
                                                        new InternalAvg("ticketPrice", 0L, 0L, DocValueFormat.RAW, Collections.emptyMap())
                                                    ),
                                                    true,
                                                    DocValueFormat.RAW
                                                ),
                                                new InternalDateRange.Bucket(
                                                    "2021-07-19T00:00:00.000Z-2021-07-19T01:00:00.000Z",
                                                    1.6266528E12,
                                                    1.6266564E12,
                                                    1,
                                                    Collections.singletonList(
                                                        new InternalAvg(
                                                            "ticketPrice",
                                                            841.265625,
                                                            1L,
                                                            DocValueFormat.RAW,
                                                            Collections.emptyMap()
                                                        )
                                                    ),
                                                    true,
                                                    DocValueFormat.RAW
                                                )
                                            ),
                                            DocValueFormat.RAW,
                                            true,
                                            Collections.emptyMap()
                                        )
                                    )
                                ),
                                Collections.emptyMap()
                            )
                        )
                    ),
                    true
                ),
                new InternalFilters.InternalBucket(
                    "US",
                    49,
                    InternalAggregations.from(
                        Arrays.asList(
                            new InternalFilter(
                                "foo",
                                2,
                                InternalAggregations.from(
                                    Arrays.asList(
                                        new InternalDateRange.Factory().create(
                                            "daily",
                                            Arrays.asList(
                                                new InternalDateRange.Bucket(
                                                    "*-2021-07-19T00:00:00.000Z",
                                                    Double.NaN,
                                                    1.6266528E12,
                                                    0,
                                                    Collections.singletonList(
                                                        new InternalAvg("ticketPrice", 0L, 0L, DocValueFormat.RAW, Collections.emptyMap())
                                                    ),
                                                    true,
                                                    DocValueFormat.RAW
                                                ),
                                                new InternalDateRange.Bucket(
                                                    "2021-07-19T00:00:00.000Z-2021-07-19T01:00:00.000Z",
                                                    1.6266528E12,
                                                    1.6266564E12,
                                                    2,
                                                    Collections.singletonList(
                                                        new InternalAvg(
                                                            "ticketPrice",
                                                            932.091796875,
                                                            2L,
                                                            DocValueFormat.RAW,
                                                            Collections.emptyMap()
                                                        )
                                                    ),
                                                    true,
                                                    DocValueFormat.RAW
                                                )
                                            ),
                                            DocValueFormat.RAW,
                                            true,
                                            Collections.emptyMap()
                                        )
                                    )
                                ),
                                Collections.emptyMap()
                            )

                        )
                    ),
                    true
                )
            ),
            true,
            Collections.emptyMap()
        );

        InternalFilter barFilter = new InternalFilter(
            "bar",
            3,
            InternalAggregations.from(Collections.singletonList(internalFilters)),
            Collections.emptyMap()
        );

        MlAggsHelper.DoubleBucketValues deValues = MlAggsHelper.extractDoubleBucketedValues(
            "bar>airline['DE']>foo>daily>ticketPrice",
            new Aggregations(Collections.singletonList(barFilter))
        ).orElseThrow(() -> new AssertionError("extracted bucket values were null"));

        assertThat(deValues.getValues()[0], equalTo(0.0));
        assertThat(deValues.getDocCounts()[0], equalTo(0L));
        assertThat(deValues.getValues()[1], equalTo(841.265625));
        assertThat(deValues.getDocCounts()[1], equalTo(1L));

        MlAggsHelper.DoubleBucketValues deValuesCount = MlAggsHelper.extractDoubleBucketedValues(
            "bar>airline['DE']>foo>daily._count",
            new Aggregations(Collections.singletonList(barFilter))
        ).orElseThrow(() -> new AssertionError("extracted bucket values were null"));

        assertThat(deValuesCount.getValues()[0], equalTo(0.0));
        assertThat(deValuesCount.getDocCounts()[0], equalTo(0L));
        assertThat(deValuesCount.getValues()[1], equalTo(1.0));
        assertThat(deValuesCount.getDocCounts()[1], equalTo(1L));

        MlAggsHelper.DoubleBucketValues usValues = MlAggsHelper.extractDoubleBucketedValues(
            "bar>airline['US']>foo>daily>ticketPrice",
            new Aggregations(Collections.singletonList(barFilter))
        ).orElseThrow(() -> new AssertionError("extracted bucket values were null"));
        assertThat(usValues.getValues()[0], equalTo(0.0));
        assertThat(usValues.getDocCounts()[0], equalTo(0L));
        assertThat(usValues.getValues()[1], equalTo(466.0458984375));
        assertThat(usValues.getDocCounts()[1], equalTo(2L));

        MlAggsHelper.DoubleBucketValues usValuesCount = MlAggsHelper.extractDoubleBucketedValues(
            "bar>airline['US']>foo>daily._count",
            new Aggregations(Collections.singletonList(barFilter))
        ).orElseThrow(() -> new AssertionError("extracted bucket values were null"));
        assertThat(usValuesCount.getValues()[0], equalTo(0.0));
        assertThat(usValuesCount.getDocCounts()[0], equalTo(0L));
        assertThat(usValuesCount.getValues()[1], equalTo(2.0));
        assertThat(usValuesCount.getDocCounts()[1], equalTo(2L));
    }

}
