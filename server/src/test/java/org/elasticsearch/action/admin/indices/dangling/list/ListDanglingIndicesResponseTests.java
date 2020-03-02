package org.elasticsearch.action.admin.indices.dangling.list;

import org.elasticsearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesResponse.AggregatedDanglingIndexInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesResponse.resultsByIndexUUID;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListDanglingIndicesResponseTests {

    /**
     * Checks that {@link ListDanglingIndicesResponse#resultsByIndexUUID(List)} handles the
     * basic base of empty input.
     */
    @Test
    public void resultsByIndexUUIDWithEmptyListReturnsEmptyMap() {
        assertThat(resultsByIndexUUID(emptyList()), empty());
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate a single dangling index
     * on a single node.
     */
    @Test
    public void resultsByIndexUUIDCanAggregateASingleResponse() {
        final DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("some-node-id");

        final var danglingIndexInfo = List.of(new DanglingIndexInfo("some-node-id", "some-index", "deadb33f", 123456L));
        final var nodes = List.of(new NodeListDanglingIndicesResponse(node, danglingIndexInfo));

        final var aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(1));

        final var expected = new AggregatedDanglingIndexInfo("deadb33f", "some-index", 123456L);
        expected.getNodeIds().add("some-node-id");
        assertThat(aggregated.get(0), equalTo(expected));
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate a single dangling index
     * across multiple nodes.
     */
    @Test
    public void resultsByIndexUUIDCanAggregateAcrossMultipleNodes() {
        final DiscoveryNode node1 = mock(DiscoveryNode.class);
        final DiscoveryNode node2 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node-id-1");
        when(node2.getId()).thenReturn("node-id-2");

        final var danglingIndexInfo1 = List.of(new DanglingIndexInfo("node-id-1", "some-index", "deadb33f", 123456L));
        final var danglingIndexInfo2 = List.of(new DanglingIndexInfo("node-id-2", "some-index", "deadb33f", 123456L));
        final var nodes = List.of(
            new NodeListDanglingIndicesResponse(node1, danglingIndexInfo1),
            new NodeListDanglingIndicesResponse(node2, danglingIndexInfo2)
        );

        final var aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(1));

        final var expected = new AggregatedDanglingIndexInfo("deadb33f", "some-index", 123456L);
        expected.getNodeIds().add("node-id-1");
        expected.getNodeIds().add("node-id-2");
        assertThat(aggregated.get(0), equalTo(expected));
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate multiple dangling indices
     * on a single node.
     */
    @Test
    public void resultsByIndexUUIDCanAggregateMultipleIndicesOnOneNode() {
        final DiscoveryNode node1 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node-id-1");

        final var danglingIndexInfo = List.of(
            new DanglingIndexInfo("node-id-1", "some-index", "deadb33f", 123456L),
            new DanglingIndexInfo("node-id-1", "some-other-index", "cafebabe", 7891011L)
        );

        final var nodes = List.of(new NodeListDanglingIndicesResponse(node1, danglingIndexInfo));

        final var aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(2));

        var info1 = new AggregatedDanglingIndexInfo("deadb33f", "some-index", 123456L);
        var info2 = new AggregatedDanglingIndexInfo("cafebabe", "some-other-index", 7891011L);
        info1.getNodeIds().add("node-id-1");
        info2.getNodeIds().add("node-id-1");

        assertThat(aggregated, containsInAnyOrder(info1, info2));
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate multiple dangling indices
     * across multiple nodes.
     */
    @Test
    public void resultsByIndexUUIDCanAggregateMultipleIndicesAcrossMultipleNodes() {
        final DiscoveryNode node1 = mock(DiscoveryNode.class);
        final DiscoveryNode node2 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node-id-1");
        when(node2.getId()).thenReturn("node-id-2");

        final var danglingIndexInfo1 = List.of(new DanglingIndexInfo("node-id-1", "some-index", "deadb33f", 123456L));
        final var danglingIndexInfo2 = List.of(new DanglingIndexInfo("node-id-2", "some-other-index", "cafebabe", 7891011L));
        final var nodes = List.of(
            new NodeListDanglingIndicesResponse(node1, danglingIndexInfo1),
            new NodeListDanglingIndicesResponse(node2, danglingIndexInfo2)
        );

        final var aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(2));

        var info1 = new AggregatedDanglingIndexInfo("deadb33f", "some-index", 123456L);
        var info2 = new AggregatedDanglingIndexInfo("cafebabe", "some-other-index", 7891011L);
        info1.getNodeIds().add("node-id-1");
        info2.getNodeIds().add("node-id-2");

        assertThat(aggregated, containsInAnyOrder(info1, info2));
    }
}
