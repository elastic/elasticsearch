/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.math.BigInteger;
import java.time.ZoneId;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class TopHitsAggExtractorTests extends AbstractSqlWireSerializingTestCase<TopHitsAggExtractor> {

    public static TopHitsAggExtractor randomTopHitsAggExtractor() {
        return new TopHitsAggExtractor(randomAlphaOfLength(16), randomFrom(SqlDataTypes.types()), randomZone());
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
            randomValueOtherThan(instance.fieldDataType(), () -> randomFrom(SqlDataTypes.types())),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone)
        );
    }

    public void testNoAggs() {
        Bucket bucket = new TestBucket(emptyMap(), 0, InternalAggregations.from(emptyList()));
        TopHitsAggExtractor extractor = randomTopHitsAggExtractor();
        SqlIllegalArgumentException exception = expectThrows(SqlIllegalArgumentException.class, () -> extractor.extract(bucket));
        assertEquals("Cannot find an aggregation named " + extractor.name(), exception.getMessage());
    }

    public void testZeroNullValue() {
        TopHitsAggExtractor extractor = randomTopHitsAggExtractor();

        InternalAggregation agg = new InternalTopHits(extractor.name(), 0, 0, null, SearchHits.EMPTY_WITH_TOTAL_HITS, null);
        Bucket bucket = new TestBucket(emptyMap(), 0, InternalAggregations.from(singletonList(agg)));
        assertNull(extractor.extract(bucket));
    }

    public void testExtractValue() {
        TopHitsAggExtractor extractor = new TopHitsAggExtractor("topHitsAgg", DataTypes.KEYWORD, UTC);

        String value = "Str_Value";
        InternalAggregation agg = new InternalTopHits(extractor.name(), 0, 1, null, searchHitsOf(value), null);
        Bucket bucket = new TestBucket(emptyMap(), 0, InternalAggregations.from(singletonList(agg)));
        assertEquals(value, extractor.extract(bucket));
    }

    public void testExtractDateValue() {
        ZoneId zoneId = randomZone();
        TopHitsAggExtractor extractor = new TopHitsAggExtractor("topHitsAgg", DataTypes.DATETIME, zoneId);

        long value = 123456789L;
        InternalAggregation agg = new InternalTopHits(
            extractor.name(),
            0,
            1,
            null,
            searchHitsOf(StringUtils.toString(DateUtils.asDateTimeWithMillis(value, zoneId))),
            null
        );
        Bucket bucket = new TestBucket(emptyMap(), 0, InternalAggregations.from(singletonList(agg)));
        assertEquals(DateUtils.asDateTimeWithMillis(value, zoneId), extractor.extract(bucket));
    }

    public void testExtractUnsignedLong() {
        BigInteger bi = randomBigInteger();
        Object value = bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0 ? bi.longValue() : bi;

        TopHitsAggExtractor extractor = new TopHitsAggExtractor(randomAlphaOfLength(10), DataTypes.UNSIGNED_LONG, randomZone());
        InternalAggregation agg = new InternalTopHits(extractor.name(), 0, 1, null, searchHitsOf(value), null);
        Bucket bucket = new TestBucket(emptyMap(), 0, InternalAggregations.from(singletonList(agg)));
        assertEquals(bi, extractor.extract(bucket));
    }

    private SearchHits searchHitsOf(Object value) {
        TotalHits totalHits = new TotalHits(10, TotalHits.Relation.EQUAL_TO);
        SearchHit searchHit = SearchHit.unpooled(1, "docId");
        searchHit.addDocumentFields(
            Collections.singletonMap("topHitsAgg", new DocumentField("field", Collections.singletonList(value))),
            Collections.singletonMap(
                "topHitsAgg",
                new DocumentField("_ignored", Collections.singletonList(randomValueOtherThan(value, () -> randomAlphaOfLength(5))))
            )
        );
        return SearchHits.unpooled(new SearchHit[] { searchHit }, totalHits, 0.0f);
    }
}
