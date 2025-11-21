/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram.aggregations;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramTestUtils;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xpack.analytics.mapper.IndexWithCount;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramFieldMapper;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramMapperPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.analytics.mapper.ExponentialHistogramParser.EXPONENTIAL_HISTOGRAM_FEATURE;

public abstract class ExponentialHistogramAggregatorTestCase extends AggregatorTestCase {

    @Before
    public void setup() {
        assumeTrue("Only when exponential_histogram feature flag is enabled", EXPONENTIAL_HISTOGRAM_FEATURE.isEnabled());
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new ExponentialHistogramMapperPlugin());
    }

    protected static List<ExponentialHistogram> createRandomHistograms(int count) {
        return IntStream.range(0, count).mapToObj(i -> ExponentialHistogramTestUtils.randomHistogram()).toList();
    }

    public static void addHistogramDoc(
        RandomIndexWriter iw,
        String fieldName,
        @Nullable ExponentialHistogram histogram,
        IndexableField... additionalFields
    ) {
        try {
            if (histogram == null) {
                iw.addDocument(Collections.emptyList());
            } else {
                ExponentialHistogramFieldMapper.HistogramDocValueFields docValues = ExponentialHistogramFieldMapper.buildDocValueFields(
                    fieldName,
                    histogram.scale(),
                    IndexWithCount.fromIterator(histogram.negativeBuckets().iterator()),
                    IndexWithCount.fromIterator(histogram.positiveBuckets().iterator()),
                    histogram.zeroBucket().zeroThreshold(),
                    histogram.valueCount(),
                    histogram.sum(),
                    histogram.min(),
                    histogram.max()
                );
                iw.addDocument(Stream.concat(docValues.fieldsAsList().stream(), Arrays.stream(additionalFields)).toList());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
