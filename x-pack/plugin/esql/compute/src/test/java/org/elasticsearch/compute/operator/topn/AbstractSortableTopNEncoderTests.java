/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytes;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.util.Comparator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractSortableTopNEncoderTests extends ESTestCase {
    protected record TestCase<T>(
        String name,
        Supplier<T> randomValue,
        Comparator<T> comparator,
        TriConsumer<TopNEncoder, T, PagedBytesBuilder> encode,
        BiFunction<TopNEncoder, PagedBytesCursor, T> decode
    ) {
        public void testCompare(
            TopNEncoder encoder,
            BiConsumer<PagedBytesBuilder, PagedBytesBuilder> assertMinMax,
            PageCacheRecycler recycler
        ) {
            var breaker = new NoopCircuitBreaker("test");
            T min = randomValue.get();
            T max = randomValueOtherThan(min, randomValue);
            if (comparator.compare(min, max) > 0) {
                T tmp = min;
                min = max;
                max = tmp;
            }
            try (
                PagedBytesBuilder minBytes = new PagedBytesBuilder(recycler, breaker, "min", 0);
                PagedBytesBuilder maxBytes = new PagedBytesBuilder(recycler, breaker, "max", 0)
            ) {
                encode.apply(encoder, min, minBytes);
                encode.apply(encoder, max, maxBytes);
                assertMinMax.accept(minBytes, maxBytes);
            }
        }

        public void testEncodeDecode(TopNEncoder encoder, PageCacheRecycler recycler) {
            var breaker = new NoopCircuitBreaker("test");
            T v = randomValue.get();
            try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "bytes", 0)) {
                encode.apply(encoder, v, builder);
                try (PagedBytes ref = builder.build()) {
                    PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                    assertThat(decode.apply(encoder, cursor), equalTo(v));
                    assertThat(cursor.remaining(), equalTo(0));
                }
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }

    protected final TestCase<?> testCase;
    private final MockPageCacheRecycler recycler = new MockPageCacheRecycler(Settings.EMPTY);

    protected PageCacheRecycler recycler() {
        return recycler;
    }

    protected AbstractSortableTopNEncoderTests(TestCase<?> testCase) {
        this.testCase = testCase;
    }

    protected abstract TopNEncoder encoder();

    protected abstract void assertMinMax(PagedBytesBuilder min, PagedBytesBuilder max);

    public final void testCompare() {
        testCase.testCompare(encoder(), this::assertMinMax, recycler);
    }

    public final void testEncodeDecode() {
        testCase.testEncodeDecode(encoder(), recycler);
    }
}
