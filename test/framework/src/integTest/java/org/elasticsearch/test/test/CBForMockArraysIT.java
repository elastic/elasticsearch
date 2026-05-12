/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.test;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.NodeMocksPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/// Verifies that [MockBigArrays] and [org.elasticsearch.common.util.MockPageCacheRecycler] detect
/// unreleased allocations at teardown, using a plugin that simulates allocation without release.
@ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CBForMockArraysIT extends ESIntegTestCase {

    public static class LeakyPlugin extends Plugin {
        @Override
        public Collection<?> createComponents(PluginServices services) {
            return List.of(new LeakyService(services.bigArrays()));
        }
    }

    public static class LeakyService {
        private final BigArrays bigArrays;

        LeakyService(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
        }

        ByteArray leakByteArray(int size) {
            return bigArrays.newByteArray(size);
        }

        ByteArray leakRecyclerByteArray() {
            return bigArrays.newByteArray(PageCacheRecycler.BYTE_PAGE_SIZE / 2);
        }

        Recycler.V<BytesRef> leakPage() {
            return bigArrays.bytesRefRecycler().obtain();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LeakyPlugin.class, NodeMocksPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    @Override
    protected boolean enableArraysReleasedCheck() {
        // These tests verify the detection mechanism themselves.
        return false;
    }

    public void testLeakingBigArrayIsDetected() {
        final var faultyService = internalCluster().getInstance(LeakyService.class);
        try (var leaked = faultyService.leakByteArray(between(1, 1024))) {
            final var e = expectThrows(RuntimeException.class, MockBigArrays::ensureAllArraysAreReleased);
            assertThat(e.getMessage(), containsString("arrays have not been released"));
        }
    }

    public void testLeakingRecyclerBackedByteArrayIsDetected() {
        final var faultyService = internalCluster().getInstance(LeakyService.class);
        try (var leaked = faultyService.leakRecyclerByteArray()) {
            final var e1 = expectThrows(RuntimeException.class, MockBigArrays::ensureAllArraysAreReleased);
            assertThat(e1.getMessage(), containsString("arrays have not been released"));
            final var e2 = expectThrows(RuntimeException.class, MockPageCacheRecycler::ensureAllPagesAreReleased);
            assertThat(e2.getMessage(), containsString("pages have not been released"));
        }
    }

    public void testLeakingBytesRefPageIsDetected() {
        final var faultyService = internalCluster().getInstance(LeakyService.class);
        try (var leaked = faultyService.leakPage()) {
            final var e = expectThrows(RuntimeException.class, MockPageCacheRecycler::ensureAllPagesAreReleased);
            assertThat(e.getMessage(), containsString("pages have not been released"));
        }
    }

    public void testLeakingBytesRefPageIsDetectedByEstimatedStats() {
        final var faultyService = internalCluster().getInstance(LeakyService.class);
        try (var leaked = faultyService.leakPage()) {
            expectThrows(AssertionError.class, () -> internalCluster().ensureEstimatedStats());
            final var e = expectThrows(RuntimeException.class, MockPageCacheRecycler::ensureAllPagesAreReleased);
            assertThat(e.getMessage(), containsString("pages have not been released"));
        }
    }
}
