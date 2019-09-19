/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.CompositeKeyExtractorTests;
import org.elasticsearch.xpack.sql.execution.search.extractor.ConstantExtractorTests;
import org.elasticsearch.xpack.sql.execution.search.extractor.MetricAggExtractorTests;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;

public class CompositeAggregationCursorTests extends AbstractSqlWireSerializingTestCase<CompositeAggregationCursor> {
    public static CompositeAggregationCursor randomCompositeCursor() {
        int extractorsSize = between(1, 20);
        ZoneId id = randomSafeZone();
        List<BucketExtractor> extractors = new ArrayList<>(extractorsSize);
        for (int i = 0; i < extractorsSize; i++) {
            extractors.add(randomBucketExtractor(id));
        }

        return new CompositeAggregationCursor(new byte[randomInt(256)], extractors, randomBitSet(extractorsSize),
                randomIntBetween(10, 1024), randomBoolean(), randomAlphaOfLength(5));
    }

    static BucketExtractor randomBucketExtractor(ZoneId zoneId) {
        List<Supplier<BucketExtractor>> options = new ArrayList<>();
        options.add(ConstantExtractorTests::randomConstantExtractor);
        options.add(() -> MetricAggExtractorTests.randomMetricAggExtractor(zoneId));
        options.add(() -> CompositeKeyExtractorTests.randomCompositeKeyExtractor(zoneId));
        return randomFrom(options).get();
    }

    @Override
    protected CompositeAggregationCursor mutateInstance(CompositeAggregationCursor instance) throws IOException {
        return new CompositeAggregationCursor(instance.next(), instance.extractors(),
                randomValueOtherThan(instance.mask(), () -> randomBitSet(instance.extractors().size())),
                randomValueOtherThan(instance.limit(), () -> randomIntBetween(1, 512)),
                !instance.includeFrozen(),
                instance.indices());
    }

    @Override
    protected CompositeAggregationCursor createTestInstance() {
        return randomCompositeCursor();
    }

    @Override
    protected Reader<CompositeAggregationCursor> instanceReader() {
        return CompositeAggregationCursor::new;
    }

    @Override
    protected ZoneId instanceZoneId(CompositeAggregationCursor instance) {
        List<BucketExtractor> extractors = instance.extractors();
        for (BucketExtractor bucketExtractor : extractors) {
            ZoneId zoneId = MetricAggExtractorTests.extractZoneId(bucketExtractor);
            zoneId = zoneId == null ? CompositeKeyExtractorTests.extractZoneId(bucketExtractor) : zoneId;

            if (zoneId != null) {
                return zoneId;
            }
        }
        return randomSafeZone();
    }

    static BitSet randomBitSet(int size) {
        BitSet mask = new BitSet(size);
        for (int i = 0; i < size; i++) {
            mask.set(i, randomBoolean());
        }
        return mask;
    }
}
