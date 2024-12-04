/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.metering.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheNodesResponseTests.createErrors;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class GetBlobStoreStatsNodesResponseTests extends AbstractWireSerializingTestCase<GetBlobStoreStatsNodesResponse> {
    static final int MAX_NODE_RESPONSES = 2; // keep these low so that chance of 0 is higher.
    static final int MAX_NODE_FAILURES = 2; // keep these low so that chance of 0 is higher.

    public static List<GetBlobStoreStatsNodeResponse> createTestInstances(int numInstances) {
        return IntStream.range(0, numInstances)
            .mapToObj(
                instanceNum -> GetBlobStoreStatsNodeResponseTests.createLabeledTestInstance(
                    Integer.toString(instanceNum),
                    GetBlobStoreStatsNodeResponseTests.randomRequestNames()
                )
            )
            .toList();
    }

    @Override
    protected Writeable.Reader<GetBlobStoreStatsNodesResponse> instanceReader() {
        return GetBlobStoreStatsNodesResponse::new;
    }

    @Override
    protected GetBlobStoreStatsNodesResponse createTestInstance() {
        return new GetBlobStoreStatsNodesResponse(
            new ClusterName("elasticsearch"),
            createTestInstances(MAX_NODE_RESPONSES),
            createErrors(randomInt(MAX_NODE_FAILURES))
        );
    }

    @Override
    protected GetBlobStoreStatsNodesResponse mutateInstance(GetBlobStoreStatsNodesResponse instance) throws IOException {
        return switch (between(0, 3)) {
            // Change the cluster name.
            case 0 -> new GetBlobStoreStatsNodesResponse(
                new ClusterName(randomAlphaOfLength(10)),
                instance.getNodes(),
                instance.failures()
            );
            // Change the number of node responses.
            case 1 -> new GetBlobStoreStatsNodesResponse(
                instance.getClusterName(),
                createTestInstances(randomValueOtherThan(instance.getNodes().size(), () -> randomInt(MAX_NODE_RESPONSES))),
                instance.failures()
            );
            // Mutate the node response instances (if not empty), but keep the same count to exercise equality of members.
            case 2 -> instance.getNodes().isEmpty()
                ? new GetBlobStoreStatsNodesResponse(new ClusterName(randomAlphaOfLength(6)), instance.getNodes(), instance.failures())
                : new GetBlobStoreStatsNodesResponse(
                    instance.getClusterName(),
                    instance.getNodes()
                        .stream()
                        .map(
                            getBlobStoreStatsNodeResponse -> new GetBlobStoreStatsNodeResponseTests().mutateInstance(
                                getBlobStoreStatsNodeResponse
                            )
                        )
                        .toList(),
                    instance.failures()
                );
            // Change the number of failures.
            case 3 -> instance.failures().isEmpty()
                ? new GetBlobStoreStatsNodesResponse(new ClusterName(randomAlphaOfLength(6)), instance.getNodes(), instance.failures())
                : new GetBlobStoreStatsNodesResponse(
                    instance.getClusterName(),
                    instance.getNodes(),
                    createErrors(randomValueOtherThan(instance.failures().size(), () -> randomInt(MAX_NODE_FAILURES)))
                );
            default -> throw new AssertionError();
        };
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        final GetBlobStoreStatsNodesResponse instance = createTestInstance();
        final Map<String, Object> responseMap = XContentTestUtils.convertToMap(instance);
        assertThat(responseMap.keySet(), containsInAnyOrder("nodes", "_all"));

        final var nodesMap = (Map<String, Object>) responseMap.get("nodes");
        assertThat(nodesMap, aMapWithSize(instance.getNodes().size()));

        instance.getNodes().forEach(node -> {
            final String nodeId = node.getNode().getId();
            assertThat(nodesMap, hasKey(nodeId));
            assertThat(
                nodesMap.get(nodeId),
                equalTo(
                    Map.of(
                        "object_store_stats",
                        Map.of(
                            "request_counts",
                            Maps.transformValues(
                                GetBlobStoreStatsNodeResponse.getRequestCounts(node.getRepositoryStats()),
                                Math::toIntExact
                            )
                        ),
                        "operational_backup_service_stats",
                        Map.of(
                            "request_counts",
                            Maps.transformValues(
                                GetBlobStoreStatsNodeResponse.getRequestCounts(node.getObsRepositoryStats()),
                                Math::toIntExact
                            )
                        )
                    )
                )
            );
        });

        final var allCounts = new HashMap<String, BlobStoreActionStats>();
        for (GetBlobStoreStatsNodeResponse nodeResponse : instance.getNodes()) {
            nodeResponse.getRepositoryStats().actionStats.forEach(
                (key, value) -> allCounts.compute(key, (k, v) -> v == null ? value : v.add(value))
            );
        }
        final var obsAllCounts = new HashMap<String, BlobStoreActionStats>();
        for (GetBlobStoreStatsNodeResponse nodeResponse : instance.getNodes()) {
            nodeResponse.getObsRepositoryStats().actionStats.forEach(
                (key, value) -> obsAllCounts.compute(key, (k, v) -> v == null ? value : v.add(value))
            );
        }
        assertThat(
            responseMap.get("_all"),
            equalTo(
                Map.of(
                    "object_store_stats",
                    Map.of("request_counts", Maps.transformValues(allCounts, v -> Math.toIntExact(v.requests()))),
                    "operational_backup_service_stats",
                    Map.of("request_counts", Maps.transformValues(obsAllCounts, v -> Math.toIntExact(v.requests())))
                )
            )
        );
    }
}
