/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.querydsl.container.GroupByRef.Property;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.ZoneId;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class CompositeKeyExtractorTests extends AbstractWireSerializingTestCase<CompositeKeyExtractor> {

    public static CompositeKeyExtractor randomCompositeKeyExtractor() {
        return new CompositeKeyExtractor(randomAlphaOfLength(16), randomFrom(asList(Property.values())), randomSafeZone(), randomBoolean());
    }

    @Override
    protected CompositeKeyExtractor createTestInstance() {
        return randomCompositeKeyExtractor();
    }

    @Override
    protected Reader<CompositeKeyExtractor> instanceReader() {
        return CompositeKeyExtractor::new;
    }

    @Override
    protected CompositeKeyExtractor mutateInstance(CompositeKeyExtractor instance) {
        return new CompositeKeyExtractor(
            instance.key() + "mutated",
            randomValueOtherThan(instance.property(), () -> randomFrom(Property.values())),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone),
            !instance.isDateTimeBased());
    }

    public void testExtractBucketCount() {
        Bucket bucket = new TestBucket(emptyMap(), randomLong(), new Aggregations(emptyList()));
        CompositeKeyExtractor extractor = new CompositeKeyExtractor(randomAlphaOfLength(16), Property.COUNT,
                randomZone(), false);
        assertEquals(bucket.getDocCount(), extractor.extract(bucket));
    }

    public void testExtractKey() {
        CompositeKeyExtractor extractor = new CompositeKeyExtractor(randomAlphaOfLength(16), Property.VALUE, UTC, false);

        Object value = new Object();
        Bucket bucket = new TestBucket(singletonMap(extractor.key(), value), randomLong(), new Aggregations(emptyList()));
        assertEquals(value, extractor.extract(bucket));
    }

    public void testExtractDate() {
        CompositeKeyExtractor extractor = new CompositeKeyExtractor(randomAlphaOfLength(16), Property.VALUE, randomSafeZone(), true);

        long millis = System.currentTimeMillis();
        Bucket bucket = new TestBucket(singletonMap(extractor.key(), millis), randomLong(), new Aggregations(emptyList()));
        assertEquals(DateUtils.asDateTime(millis, extractor.zoneId()), extractor.extract(bucket));
    }

    public void testExtractIncorrectDateKey() {
        CompositeKeyExtractor extractor = new CompositeKeyExtractor(randomAlphaOfLength(16), Property.VALUE, randomZone(), true);

        Object value = new Object();
        Bucket bucket = new TestBucket(singletonMap(extractor.key(), value), randomLong(), new Aggregations(emptyList()));
        SqlIllegalArgumentException exception = expectThrows(SqlIllegalArgumentException.class, () -> extractor.extract(bucket));
        assertEquals("Invalid date key returned: " + value, exception.getMessage());
    }

    /**
     * We need to exclude SystemV/* time zones because they cannot be converted
     * back to DateTimeZone which we currently still need to do internally,
     * e.g. in bwc serialization and in the extract() method
     */
    private static ZoneId randomSafeZone() {
        return randomValueOtherThanMany(zi -> zi.getId().startsWith("SystemV"), () -> randomZone());
    }
}
