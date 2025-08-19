/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.MapMatcher;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

public abstract class AsyncOperatorTestCase extends OperatorTestCase {
    @Override
    protected final void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        var mapMatcher = matchesMap().entry("pages_received", input.size())
            .entry("pages_completed", input.size())
            .entry("process_nanos", matchNumberGreaterThanOrEqualTo(0));

        mapMatcher = extendStatusMatcher(mapMatcher, input, output);

        assertMap(map, mapMatcher);
    }

    protected MapMatcher extendStatusMatcher(MapMatcher mapMatcher, List<Page> input, List<Page> output) {
        return mapMatcher;
    }
}
