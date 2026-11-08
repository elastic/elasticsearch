/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/** The shared pages of a segment: a lookup by document, a bound in bytes, and the oldest pages leave first. */
public class DecodedPageCacheTests extends ESTestCase {

    private static NumericWindow.NumericPage page(int start, int count) {
        return new NumericWindow.NumericPage(start, count, null, new long[count]);
    }

    public void testFindsThePageThatHoldsTheDocument() {
        DecodedPageCache cache = new DecodedPageCache(1L << 20);
        DecodedPageCache.ColumnPage first = page(0, 100);
        DecodedPageCache.ColumnPage second = page(100, 50);
        cache.put("a", first);
        cache.put("a", second);
        assertThat(cache.find("a", 0), sameInstance(first));
        assertThat(cache.find("a", 99), sameInstance(first));
        assertThat(cache.find("a", 100), sameInstance(second));
        assertThat(cache.find("a", 149), sameInstance(second));
        assertThat("after the last page", cache.find("a", 150), nullValue());
        assertThat("another field", cache.find("b", 10), nullValue());
        assertThat(cache.hits(), equalTo(4L));
    }

    public void testAGapIsNotAHit() {
        DecodedPageCache cache = new DecodedPageCache(1L << 20);
        cache.put("a", page(0, 10));
        cache.put("a", page(100, 10));
        // the floor of 50 is the page at 0, which does not hold it
        assertThat(cache.find("a", 50), nullValue());
    }

    public void testASecondPageAtTheSameStartIsIgnored() {
        DecodedPageCache cache = new DecodedPageCache(1L << 20);
        DecodedPageCache.ColumnPage first = page(0, 10);
        cache.put("a", first);
        cache.put("a", page(0, 10));
        assertThat(cache.find("a", 5), sameInstance(first));
        assertThat(cache.published(), equalTo(1L));
    }

    public void testTheOldestPagesLeaveWhenTheBoundIsPassed() {
        DecodedPageCache.ColumnPage sample = page(0, 1_000);
        // room for three pages of this size
        DecodedPageCache cache = new DecodedPageCache(3 * sample.weight() + 10);
        for (int i = 0; i < 6; i++) {
            cache.put("a", page(i * 1_000, 1_000));
        }
        assertThat("the first pages are gone", cache.find("a", 0), nullValue());
        assertThat(cache.find("a", 1_000), nullValue());
        assertThat(cache.find("a", 2_000), nullValue());
        assertThat("the last pages are there", cache.find("a", 3_000).start(), equalTo(3_000));
        assertThat(cache.find("a", 5_999).start(), equalTo(5_000));
    }

    public void testClearEmptiesTheCache() {
        DecodedPageCache cache = new DecodedPageCache(1L << 20);
        cache.put("a", page(0, 10));
        cache.clear();
        assertThat(cache.find("a", 5), nullValue());
        // and it takes pages again
        cache.put("a", page(0, 10));
        assertThat(cache.find("a", 5).start(), equalTo(0));
    }
}
