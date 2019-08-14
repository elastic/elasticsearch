/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.CompositeKeyExtractorTests;
import org.elasticsearch.xpack.sql.execution.search.extractor.ConstantExtractorTests;
import org.elasticsearch.xpack.sql.execution.search.extractor.MetricAggExtractorTests;
import org.elasticsearch.xpack.sql.session.Cursors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;

public class CompositeAggregationCursorTests extends AbstractWireSerializingTestCase<CompositeAggregationCursor> {
    public static CompositeAggregationCursor randomCompositeCursor() {
        int extractorsSize = between(1, 20);
        List<BucketExtractor> extractors = new ArrayList<>(extractorsSize);
        for (int i = 0; i < extractorsSize; i++) {
            extractors.add(randomBucketExtractor());
        }

        return new CompositeAggregationCursor(new byte[randomInt(256)], extractors, randomBitSet(extractorsSize),
                randomIntBetween(10, 1024), randomBoolean(), randomAlphaOfLength(5));
    }

    static BucketExtractor randomBucketExtractor() {
        List<Supplier<BucketExtractor>> options = new ArrayList<>();
        options.add(ConstantExtractorTests::randomConstantExtractor);
        options.add(MetricAggExtractorTests::randomMetricAggExtractor);
        options.add(CompositeKeyExtractorTests::randomCompositeKeyExtractor);
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
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Cursors.getNamedWriteables());
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
    protected CompositeAggregationCursor copyInstance(CompositeAggregationCursor instance, Version version) throws IOException {
        /* Randomly choose between internal protocol round trip and String based
         * round trips used to toXContent. */
        if (randomBoolean()) {
            return super.copyInstance(instance, version);
        }
        return (CompositeAggregationCursor) Cursors.decodeFromString(Cursors.encodeToString(version, instance));
    }

    static BitSet randomBitSet(int size) {
        BitSet mask = new BitSet(size);
        for (int i = 0; i < size; i++) {
            mask.set(i, randomBoolean());
        }
        return mask;
    }
}
