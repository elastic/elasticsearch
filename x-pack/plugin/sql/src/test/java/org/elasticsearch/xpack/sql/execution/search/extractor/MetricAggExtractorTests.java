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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.ZoneId;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class MetricAggExtractorTests extends AbstractSqlWireSerializingTestCase<MetricAggExtractor> {

    public static MetricAggExtractor randomMetricAggExtractor() {
        return new MetricAggExtractor(randomAlphaOfLength(16), randomAlphaOfLength(16), randomAlphaOfLength(16),
            randomZone(), randomBoolean());
    }

    public static MetricAggExtractor randomMetricAggExtractor(ZoneId zoneId) {
        return new MetricAggExtractor(randomAlphaOfLength(16), randomAlphaOfLength(16), randomAlphaOfLength(16), zoneId, randomBoolean());
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
    protected ZoneId instanceZoneId(MetricAggExtractor instance) {
        return instance.zoneId();
    }

    @Override
    protected MetricAggExtractor mutateInstance(MetricAggExtractor instance) throws IOException {
        return new MetricAggExtractor(
            instance.name() + "mutated",
            instance.property() + "mutated",
            instance.innerKey() + "mutated",
            randomValueOtherThan(instance.zoneId(),
                ESTestCase::randomZone), randomBoolean());
    }

    public void testNoAggs() {
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(emptyList()));
        MetricAggExtractor extractor = randomMetricAggExtractor();
        SqlIllegalArgumentException exception = expectThrows(SqlIllegalArgumentException.class, () -> extractor.extract(bucket));
        assertEquals("Cannot find an aggregation named " + extractor.name(), exception.getMessage());
    }

    public void testSingleValueProperty() {
        MetricAggExtractor extractor = new MetricAggExtractor("field", "property", "innerKey", null, false);

        double value = randomDouble();
        Aggregation agg = new TestSingleValueAggregation(extractor.name(), singletonList(extractor.property()), value);
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(value, extractor.extract(bucket));
    }

    public void testSingleValuePropertyDate() {
        ZoneId zoneId = randomZone();
        MetricAggExtractor extractor = new MetricAggExtractor("my_date_field", "property", "innerKey", zoneId, true);

        double value = randomDouble();
        Aggregation agg = new TestSingleValueAggregation(extractor.name(), singletonList(extractor.property()), value);
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(DateUtils.asDateTime((long) value , zoneId), extractor.extract(bucket));
    }

    public void testSingleValueInnerKey() {
        MetricAggExtractor extractor = new MetricAggExtractor("field", "property", "innerKey", null, false);
        double innerValue = randomDouble();
        Aggregation agg = new TestSingleValueAggregation(extractor.name(), singletonList(extractor.property()),
                singletonMap(extractor.innerKey(), innerValue));
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(innerValue, extractor.extract(bucket));
    }

    public void testSingleValueInnerKeyDate() {
        ZoneId zoneId = randomZone();
        MetricAggExtractor extractor = new MetricAggExtractor("field", "property", "innerKey", zoneId, true);

        double innerValue = randomDouble();
        Aggregation agg = new TestSingleValueAggregation(extractor.name(), singletonList(extractor.property()),
            singletonMap(extractor.innerKey(), innerValue));
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(DateUtils.asDateTime((long) innerValue , zoneId), extractor.extract(bucket));
    }

    public void testMultiValueProperty() {
        MetricAggExtractor extractor = new MetricAggExtractor("field", "property", "innerKey", null, false);

        double value = randomDouble();
        Aggregation agg = new TestMultiValueAggregation(extractor.name(), singletonMap(extractor.property(), value));
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(value, extractor.extract(bucket));
    }

    public void testMultiValuePropertyDate() {
        ZoneId zoneId = randomZone();
        MetricAggExtractor extractor = new MetricAggExtractor("field", "property", "innerKey", zoneId, true);

        double value = randomDouble();
        Aggregation agg = new TestMultiValueAggregation(extractor.name(), singletonMap(extractor.property(), value));
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(DateUtils.asDateTime((long) value , zoneId), extractor.extract(bucket));
    }

    public static ZoneId extractZoneId(BucketExtractor extractor) {
        return extractor instanceof MetricAggExtractor ? ((MetricAggExtractor) extractor).zoneId() : null;
    }
}
