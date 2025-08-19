/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

public abstract class SourceOperatorTestCase extends AnyOperatorTestCase {
    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertMap(
            map,
            matchesMap().extraOk()
                .entry("pages_emitted", output.size())
                .entry("rows_emitted", output.stream().mapToInt(Page::getPositionCount).sum())
        );
    }
}
