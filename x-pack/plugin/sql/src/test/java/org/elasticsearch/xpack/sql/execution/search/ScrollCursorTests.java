/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.ql.execution.search.extractor.ConstantExtractorTests;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.execution.search.extractor.ComputingExtractorTests;
import org.elasticsearch.xpack.sql.plugin.CursorTests;
import org.elasticsearch.xpack.sql.session.Cursors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ScrollCursorTests extends AbstractSqlWireSerializingTestCase<ScrollCursor> {
    public static ScrollCursor randomScrollCursor() {
        int extractorsSize = between(1, 20);
        List<HitExtractor> extractors = new ArrayList<>(extractorsSize);
        for (int i = 0; i < extractorsSize; i++) {
            extractors.add(randomHitExtractor(0));
        }
        return new ScrollCursor(randomAlphaOfLength(5), extractors, CompositeAggregationCursorTests.randomBitSet(extractorsSize),
                randomIntBetween(10, 1024));
    }

    static HitExtractor randomHitExtractor(int depth) {
        List<Supplier<HitExtractor>> options = new ArrayList<>();
        if (depth < 5) {
            options.add(() -> ComputingExtractorTests.randomComputingExtractor());
        }
        options.add(ConstantExtractorTests::randomConstantExtractor);
        return randomFrom(options).get();
    }

    @Override
    protected ScrollCursor mutateInstance(ScrollCursor instance) throws IOException {
        return new ScrollCursor(instance.scrollId(), instance.extractors(),
                randomValueOtherThan(instance.mask(), () -> CompositeAggregationCursorTests.randomBitSet(instance.extractors().size())),
                randomValueOtherThan(instance.limit(), () -> randomIntBetween(1, 1024)));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Cursors.getNamedWriteables());
    }

    @Override
    protected ScrollCursor createTestInstance() {
        return randomScrollCursor();
    }

    @Override
    protected Reader<ScrollCursor> instanceReader() {
        return ScrollCursor::new;
    }

    @Override
    protected ScrollCursor copyInstance(ScrollCursor instance, Version version) throws IOException {
        /* Randomly choose between internal protocol round trip and String based
         * round trips used to toXContent. */
        if (randomBoolean()) {
            return super.copyInstance(instance, version);
        }
        return (ScrollCursor) CursorTests.decodeFromString(Cursors.encodeToString(instance, randomZone()));
    }
}
