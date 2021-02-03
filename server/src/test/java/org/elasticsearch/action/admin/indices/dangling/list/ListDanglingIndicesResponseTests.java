/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.list;

import org.elasticsearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesResponse.AggregatedDanglingIndexInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesResponse.resultsByIndexUUID;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListDanglingIndicesResponseTests extends ESTestCase {

    public static final String UUID_1 = UUID.randomUUID().toString();
    public static final String UUID_2 = UUID.randomUUID().toString();

    /**
     * Checks that {@link ListDanglingIndicesResponse#resultsByIndexUUID(List)} handles the
     * basic base of empty input.
     */
    public void testResultsByIndexUUIDWithEmptyListReturnsEmptyMap() {
        assertThat(resultsByIndexUUID(emptyList()), empty());
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate a single dangling index
     * on a single node.
     */
    public void testResultsByIndexUUIDCanAggregateASingleResponse() {
        final DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("some-node-id");

        final var danglingIndexInfo = List.of(new DanglingIndexInfo("some-node-id", "some-index", UUID_1, 123456L));
        final var nodes = List.of(new NodeListDanglingIndicesResponse(node, danglingIndexInfo));

        final var aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(1));

        final var expected = new AggregatedDanglingIndexInfo(UUID_1, "some-index", 123456L);
        expected.getNodeIds().add("some-node-id");
        assertThat(aggregated.get(0), equalTo(expected));
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate a single dangling index
     * across multiple nodes.
     */
    public void testResultsByIndexUUIDCanAggregateAcrossMultipleNodes() {
        final DiscoveryNode node1 = mock(DiscoveryNode.class);
        final DiscoveryNode node2 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node-id-1");
        when(node2.getId()).thenReturn("node-id-2");

        final var danglingIndexInfo1 = List.of(new DanglingIndexInfo("node-id-1", "some-index", UUID_1, 123456L));
        final var danglingIndexInfo2 = List.of(new DanglingIndexInfo("node-id-2", "some-index", UUID_1, 123456L));
        final var nodes = List.of(
            new NodeListDanglingIndicesResponse(node1, danglingIndexInfo1),
            new NodeListDanglingIndicesResponse(node2, danglingIndexInfo2)
        );

        final var aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(1));

        final var expected = new AggregatedDanglingIndexInfo(UUID_1, "some-index", 123456L);
        expected.getNodeIds().add("node-id-1");
        expected.getNodeIds().add("node-id-2");
        assertThat(aggregated.get(0), equalTo(expected));
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate multiple dangling indices
     * on a single node.
     */
    public void testResultsByIndexUUIDCanAggregateMultipleIndicesOnOneNode() {
        final DiscoveryNode node1 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node-id-1");

        final var danglingIndexInfo = List.of(
            new DanglingIndexInfo("node-id-1", "some-index", UUID_1, 123456L),
            new DanglingIndexInfo("node-id-1", "some-other-index", UUID_2, 7891011L)
        );

        final var nodes = List.of(new NodeListDanglingIndicesResponse(node1, danglingIndexInfo));

        final var aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(2));

        var info1 = new AggregatedDanglingIndexInfo(UUID_1, "some-index", 123456L);
        var info2 = new AggregatedDanglingIndexInfo(UUID_2, "some-other-index", 7891011L);
        info1.getNodeIds().add("node-id-1");
        info2.getNodeIds().add("node-id-1");

        assertThat(aggregated, containsInAnyOrder(info1, info2));
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate multiple dangling indices
     * across multiple nodes.
     */
    public void testResultsByIndexUUIDCanAggregateMultipleIndicesAcrossMultipleNodes() {
        final DiscoveryNode node1 = mock(DiscoveryNode.class);
        final DiscoveryNode node2 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node-id-1");
        when(node2.getId()).thenReturn("node-id-2");

        final var danglingIndexInfo1 = List.of(new DanglingIndexInfo("node-id-1", "some-index", UUID_1, 123456L));
        final var danglingIndexInfo2 = List.of(new DanglingIndexInfo("node-id-2", "some-other-index", UUID_2, 7891011L));
        final var nodes = List.of(
            new NodeListDanglingIndicesResponse(node1, danglingIndexInfo1),
            new NodeListDanglingIndicesResponse(node2, danglingIndexInfo2)
        );

        final var aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(2));

        var info1 = new AggregatedDanglingIndexInfo(UUID_1, "some-index", 123456L);
        var info2 = new AggregatedDanglingIndexInfo(UUID_2, "some-other-index", 7891011L);
        info1.getNodeIds().add("node-id-1");
        info2.getNodeIds().add("node-id-2");

        assertThat(aggregated, containsInAnyOrder(info1, info2));
    }
}
