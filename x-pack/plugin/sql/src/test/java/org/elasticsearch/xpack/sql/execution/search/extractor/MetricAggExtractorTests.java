/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlException;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class MetricAggExtractorTests extends AbstractWireSerializingTestCase<MetricAggExtractor> {

    public static MetricAggExtractor randomMetricAggExtractor() {
        return new MetricAggExtractor(randomAlphaOfLength(16), randomAlphaOfLength(16), randomAlphaOfLength(16));
    }

    @Override
    protected MetricAggExtractor createTestInstance() {
        return randomMetricAggExtractor();
    }

    @Override
    protected Reader<MetricAggExtractor> instanceReader() {
        return MetricAggExtractor::new;
    }

    @Override
    protected MetricAggExtractor mutateInstance(MetricAggExtractor instance) throws IOException {
        return new MetricAggExtractor(instance.name() + "mutated", instance.property(), instance.innerKey());
    }

    public void testNoAggs() {
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(emptyList()));
        MetricAggExtractor extractor = randomMetricAggExtractor();
        SqlException exception = expectThrows(SqlException.class, () -> extractor.extract(bucket));
        assertEquals("Cannot find an aggregation named " + extractor.name(), exception.getMessage());
    }

    public void testSingleValueProperty() {
        MetricAggExtractor extractor = randomMetricAggExtractor();

        double value = randomDouble();
        Aggregation agg = new TestSingleValueAggregation(extractor.name(), singletonList(extractor.property()), value);
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(value, extractor.extract(bucket));
    }

    public void testSingleValueInnerKey() {
        MetricAggExtractor extractor = randomMetricAggExtractor();
        double innerValue = randomDouble();
        Aggregation agg = new TestSingleValueAggregation(extractor.name(), singletonList(extractor.property()),
                singletonMap(extractor.innerKey(), innerValue));
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(innerValue, extractor.extract(bucket));
    }

    public void testMultiValueProperty() {
        MetricAggExtractor extractor = randomMetricAggExtractor();

        double value = randomDouble();
        Aggregation agg = new TestMultiValueAggregation(extractor.name(), singletonMap(extractor.property(), value));
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(value, extractor.extract(bucket));
    }
}
