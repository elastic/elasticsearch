/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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

public class SearchHitCursorTests extends AbstractSqlWireSerializingTestCase<SearchHitCursor> {
    public static SearchHitCursor randomSearchHitCursor() {
        int extractorsSize = between(1, 20);
        List<HitExtractor> extractors = new ArrayList<>(extractorsSize);
        for (int i = 0; i < extractorsSize; i++) {
            extractors.add(randomHitExtractor(0));
        }
        return new SearchHitCursor(
            new SearchSourceBuilder().size(randomInt(1000)),
            extractors,
            CompositeAggregationCursorTests.randomBitSet(extractorsSize),
            randomIntBetween(10, 1024),
            randomBoolean()
        );
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
    protected SearchHitCursor mutateInstance(SearchHitCursor instance) throws IOException {
        return new SearchHitCursor(
            instance.next(),
            instance.extractors(),
            randomValueOtherThan(instance.mask(), () -> CompositeAggregationCursorTests.randomBitSet(instance.extractors().size())),
            randomValueOtherThan(instance.limit(), () -> randomIntBetween(1, 1024)),
            instance.includeFrozen() == false
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Cursors.getNamedWriteables());
    }

    @Override
    protected SearchHitCursor createTestInstance() {
        return randomSearchHitCursor();
    }

    @Override
    protected Reader<SearchHitCursor> instanceReader() {
        return SearchHitCursor::new;
    }

    @Override
    protected SearchHitCursor copyInstance(SearchHitCursor instance, Version version) throws IOException {
        /* Randomly choose between internal protocol round trip and String based
         * round trips used to toXContent. */
        if (randomBoolean()) {
            return super.copyInstance(instance, version);
        }
        return (SearchHitCursor) CursorTests.decodeFromString(Cursors.encodeToString(instance, randomZone()));
    }
}
