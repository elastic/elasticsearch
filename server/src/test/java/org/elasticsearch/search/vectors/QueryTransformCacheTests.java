/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

/**
 * Computing each transformation once is {@link org.elasticsearch.common.util.CachedSupplier}'s job, tested with it; this
 * tests what {@link QueryTransformCache} adds on top.
 */
public class QueryTransformCacheTests extends ESTestCase {

    private record Key(String name) {}

    public void testPerQuerySharesByKey() {
        QueryTransformCache transforms = QueryTransformCache.perQuery();
        assertThat(transforms.getSharedQuery(new Key("a"), () -> "a"), equalTo("a"));
        assertThat("computed once for an equal key", transforms.getSharedQuery(new Key("a"), () -> "other"), equalTo("a"));
        assertThat(transforms.getSharedQuery(new Key("b"), () -> "b"), equalTo("b"));
    }

    public void testNoneSharesNothing() {
        AtomicInteger computed = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            QueryTransformCache.NONE.getSharedQuery(new Key("a"), () -> "a" + computed.incrementAndGet());
        }
        assertThat(computed.get(), equalTo(3));
    }
}
