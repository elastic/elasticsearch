/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class EmptyTDigestStateTests extends ESTestCase {

    private static final TDigestState singleton = new EmptyTDigestState();

    public void testAddValue() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.add(randomDouble()));
    }

    public void testTestAddTDigest() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.add(new EmptyTDigestState()));
    }

    public void testTestAddWithWeight() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.add(randomDouble(), randomInt(10)));
    }

    public void testTestAddList() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.add(randomDouble(), randomInt(10)));
    }

    public void testTestAddListTDigest() {
        expectThrows(UnsupportedOperationException.class, () -> singleton.add(List.of(new EmptyTDigestState(), new EmptyTDigestState())));
    }
}
