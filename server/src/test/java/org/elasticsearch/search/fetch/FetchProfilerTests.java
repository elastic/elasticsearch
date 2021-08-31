package org.elasticsearch.search.fetch;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
import static io.github.nik9000.mapmatcher.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;

public class FetchProfilerTests extends ESTestCase {
    public void testTime() {
        FixedTimeProfiler profiler = new FixedTimeProfiler();
        profiler.start();
        long elapsed = randomLongBetween(0, Long.MAX_VALUE / 2);
        profiler.nanoTime += elapsed;
        ProfileResult result = profiler.stop();
        assertThat(result.getTime(), equalTo(elapsed));
    }

    public void testStoredFieldsIsOrdered() throws IOException {
        FetchProfiler profiler = new FetchProfiler();
        profiler.start();
        profiler.visitor(new CustomFieldsVisitor(Set.of(), true));
        ProfileResult result = profiler.stop();
        assertMap(result.getDebugInfo(), matchesMap().entry("stored_fields", List.of("_id", "_routing", "_source")));
        // Make sure that serialization preserves the order
        ProfileResult copy = copyWriteable(result, new NamedWriteableRegistry(List.of()), ProfileResult::new);
        assertMap(copy.getDebugInfo(), matchesMap().entry("stored_fields", List.of("_id", "_routing", "_source")));
    }

    static class FixedTimeProfiler extends FetchProfiler {
        long nanoTime = randomLongBetween(0, Long.MAX_VALUE / 2);

        @Override
        long nanoTime() {
            return nanoTime;
        }
    }
}
