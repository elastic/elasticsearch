/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.ZoneId;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class TopHitsAggExtractorTests extends AbstractSqlWireSerializingTestCase<TopHitsAggExtractor> {

    public static TopHitsAggExtractor randomTopHitsAggExtractor() {
        return new TopHitsAggExtractor(randomAlphaOfLength(16), randomFrom(DataType.values()), randomZone());
    }

    @Override
    protected TopHitsAggExtractor createTestInstance() {
        return randomTopHitsAggExtractor();
    }

    @Override
    protected Reader<TopHitsAggExtractor> instanceReader() {
        return TopHitsAggExtractor::new;
    }

    @Override
    protected ZoneId instanceZoneId(TopHitsAggExtractor instance) {
        return instance.zoneId();
    }

    @Override
    protected TopHitsAggExtractor mutateInstance(TopHitsAggExtractor instance) {
        return new TopHitsAggExtractor(
            instance.name() + "mutated",
            randomValueOtherThan(instance.fieldDataType(), () -> randomFrom(DataType.values())),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone));
    }

    public void testNoAggs() {
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(emptyList()));
        TopHitsAggExtractor extractor = randomTopHitsAggExtractor();
        SqlException exception = expectThrows(SqlException.class, () -> extractor.extract(bucket));
        assertEquals("Cannot find an aggregation named " + extractor.name(), exception.getMessage());
    }

    public void testZeroNullValue() {
        TopHitsAggExtractor extractor = randomTopHitsAggExtractor();

        TotalHits totalHits = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
        Aggregation agg = new InternalTopHits(extractor.name(), 0, 0, null, new SearchHits(null, totalHits, 0.0f), null, null);
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertNull(extractor.extract(bucket));
    }

    public void testExtractValue() {
        TopHitsAggExtractor extractor = new TopHitsAggExtractor("topHitsAgg", DataType.KEYWORD, UTC);

        String value = "Str_Value";
        Aggregation agg = new InternalTopHits(extractor.name(), 0, 1, null, searchHitsOf(value), null, null);
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(value, extractor.extract(bucket));
    }

    public void testExtractDateValue() {
        ZoneId zoneId = randomZone();
        TopHitsAggExtractor extractor = new TopHitsAggExtractor("topHitsAgg", DataType.DATETIME, zoneId);

        long value = 123456789L;
        Aggregation agg = new InternalTopHits(extractor.name(), 0, 1, null, searchHitsOf(value), null, null);
        Bucket bucket = new TestBucket(emptyMap(), 0, new Aggregations(singletonList(agg)));
        assertEquals(DateUtils.asDateTime(value, zoneId), extractor.extract(bucket));
    }

    private SearchHits searchHitsOf(Object value) {
        TotalHits totalHits = new TotalHits(10, TotalHits.Relation.EQUAL_TO);
        return new SearchHits(new SearchHit[] {new SearchHit(1, "docId", null,
                Collections.singletonMap("topHitsAgg", new DocumentField("field", Collections.singletonList(value))))},
            totalHits, 0.0f);
    }
}
