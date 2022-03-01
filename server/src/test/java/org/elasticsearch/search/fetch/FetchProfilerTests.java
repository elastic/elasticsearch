/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;

public class FetchProfilerTests extends ESTestCase {
    public void testTime() {
        long startTime = randomLongBetween(0, Long.MAX_VALUE / 2);
        FetchProfiler profiler = new FetchProfiler(startTime);
        long elapsed = randomLongBetween(0, Long.MAX_VALUE / 2);
        ProfileResult result = profiler.finish(startTime + elapsed);
        assertThat(result.getTime(), equalTo(elapsed));
    }

    public void testStoredFieldsIsOrdered() throws IOException {
        FetchProfiler profiler = new FetchProfiler();
        profiler.visitor(new CustomFieldsVisitor(Set.of(), true));
        ProfileResult result = profiler.finish();
        assertMap(result.getDebugInfo(), matchesMap().entry("stored_fields", List.of("_id", "_routing", "_source")));
        // Make sure that serialization preserves the order
        ProfileResult copy = copyWriteable(result, new NamedWriteableRegistry(List.of()), ProfileResult::new);
        assertMap(copy.getDebugInfo(), matchesMap().entry("stored_fields", List.of("_id", "_routing", "_source")));
    }
}
