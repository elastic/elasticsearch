/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramFieldMapper;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramMapperPlugin;
import org.elasticsearch.xpack.exponentialhistogram.IndexWithCount;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ExponentialHistogramAggregatorTestCase extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new ExponentialHistogramMapperPlugin());
    }

    protected static List<ExponentialHistogram> createRandomHistograms(int count) {
        return IntStream.range(0, count).mapToObj(i -> {
            boolean hasNegativeValues = randomBoolean();
            boolean hasPositiveValues = randomBoolean();
            boolean hasZeroValues = randomBoolean();
            double[] rawValues = IntStream.concat(
                IntStream.concat(
                    hasNegativeValues ? IntStream.range(0, randomIntBetween(1, 1000)).map(i1 -> -1) : IntStream.empty(),
                    hasPositiveValues ? IntStream.range(0, randomIntBetween(1, 1000)).map(i1 -> 1) : IntStream.empty()
                ),
                hasZeroValues ? IntStream.range(0, randomIntBetween(1, 100)).map(i1 -> 0) : IntStream.empty()
            ).mapToDouble(sign -> sign * (Math.pow(1_000_000, randomDouble()))).toArray();

            int numBuckets = randomIntBetween(4, 300);
            ExponentialHistogram histo = ExponentialHistogram.create(numBuckets, ExponentialHistogramCircuitBreaker.noop(), rawValues);
            // Randomize sum, min and max a little
            if (histo.valueCount() > 0) {
                ExponentialHistogramBuilder builder = ExponentialHistogram.builder(histo, ExponentialHistogramCircuitBreaker.noop());
                builder.sum(histo.sum() + (randomDouble() - 0.5) * 10_000);
                builder.max(histo.max() - randomDouble());
                builder.min(histo.min() + randomDouble());
                histo = builder.build();
            }
            return histo;
        }).toList();
    }

    protected static void addHistogramDoc(
        RandomIndexWriter iw,
        String fieldName,
        ExponentialHistogram histogram,
        IndexableField... additionalFields
    ) {
        try {
            ExponentialHistogramFieldMapper.HistogramDocValueFields docValues = ExponentialHistogramFieldMapper.buildDocValueFields(
                fieldName,
                histogram.scale(),
                IndexWithCount.fromIterator(histogram.negativeBuckets().iterator()),
                IndexWithCount.fromIterator(histogram.positiveBuckets().iterator()),
                histogram.zeroBucket().zeroThreshold(),
                histogram.valueCount(),
                histogram.sum(),
                nanToNull(histogram.min()),
                nanToNull(histogram.max())
            );
            iw.addDocument(Stream.concat(docValues.fieldsAsList().stream(), Arrays.stream(additionalFields)).toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Double nanToNull(double value) {
        return Double.isNaN(value) ? null : value;
    }

}
