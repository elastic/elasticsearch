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

package co.elastic.elasticsearch.stateless.cache.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class ClearBlobCacheNodesResponseTests extends ESTestCase {
    static final int MAX_NODE_RESPONSES = 2; // keep these low so that chance of 0 is higher.
    static final int MAX_NODE_FAILURES = 2; // keep these low so that chance of 0 is higher.

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            createTestInstance(),
            r -> new ClearBlobCacheNodesResponse(r.getClusterName(), r.getNodes(), r.failures()),
            ClearBlobCacheNodesResponseTests::mutateInstance
        );
    }

    private static ClearBlobCacheNodesResponse createTestInstance() {
        return new ClearBlobCacheNodesResponse(
            new ClusterName("elasticsearch"),
            ClearBlobCacheNodeResponseTests.createTestInstances(randomInt(MAX_NODE_RESPONSES)),
            createErrors(randomInt(MAX_NODE_FAILURES))
        );
    }

    public static List<FailedNodeException> createErrors(int numErrors) {
        return IntStream.range(0, numErrors).mapToObj(num -> {
            var numString = Integer.toString(num);
            var nodeId = "node_" + numString;
            return createError(nodeId);
        }).toList();
    }

    private static FailedNodeException createError(String nodeId) {
        return new FailedNodeException(nodeId, nodeId + " reason", new Throwable(nodeId + " caused by reason"));
    }

    private static ClearBlobCacheNodesResponse mutateInstance(ClearBlobCacheNodesResponse instance) {
        return switch (between(0, 3)) {
            // Change the cluster name.
            case 0 -> new ClearBlobCacheNodesResponse(new ClusterName("mutate"), instance.getNodes(), instance.failures());
            // Change the number of node responses.
            case 1 -> new ClearBlobCacheNodesResponse(
                instance.getClusterName(),
                ClearBlobCacheNodeResponseTests.createTestInstances(
                    randomValueOtherThan(instance.getNodes().size(), () -> randomInt(MAX_NODE_RESPONSES))
                ),
                instance.failures()
            );
            // Mutate the node response instances (if not empty), but keep the same count to exercise equality of members.
            case 2 -> instance.getNodes().isEmpty()
                ? new ClearBlobCacheNodesResponse(new ClusterName("mutate"), instance.getNodes(), instance.failures())
                : new ClearBlobCacheNodesResponse(
                    instance.getClusterName(),
                    instance.getNodes()
                        .stream()
                        .map(clearBlobCacheNodeResponse -> new ClearBlobCacheNodeResponseTests().mutateInstance(clearBlobCacheNodeResponse))
                        .toList(),
                    instance.failures()
                );
            // Change the number of failures.
            case 3 -> instance.failures().isEmpty()
                ? new ClearBlobCacheNodesResponse(new ClusterName("mutate"), instance.getNodes(), instance.failures())
                : new ClearBlobCacheNodesResponse(
                    instance.getClusterName(),
                    instance.getNodes(),
                    createErrors(randomValueOtherThan(instance.failures().size(), () -> randomInt(MAX_NODE_FAILURES)))
                );
            default -> throw new AssertionError();
        };
    }

    @SuppressWarnings("unchecked")
    public void testResponse() throws IOException {
        final int NUM_TEST_RUNS = 8;
        for (int testRun = 0; testRun < NUM_TEST_RUNS; testRun++) {
            var instance = createTestInstance();
            var wrapped = ChunkedToXContent.wrapAsToXContent(instance);
            XContentBuilder builder = XContentFactory.jsonBuilder();
            wrapped.toXContent(builder, ToXContent.EMPTY_PARAMS);

            var xContentMap = XContentHelper.convertToMap(builder.contentType().xContent(), Strings.toString(builder), true);

            var baseResponseData = (Map<?, ?>) xContentMap.get("_nodes");
            var failed = (int) baseResponseData.get("failed");
            var expectedFailed = instance.failures().size();
            assertEquals("The number of failures in the XContent should be equivalent to the original.", expectedFailed, failed);

            // If there are failures, test the xContent
            if (expectedFailed != 0) {
                var xContentFailures = (List<Map<?, ?>>) baseResponseData.get("failures");
                for (int i = 0; i < expectedFailed; i++) {
                    var expectedFailure = instance.failures().get(i);
                    var xContentFailure = xContentFailures.get(i);
                    assertEquals("The nodeID should match", expectedFailure.nodeId(), xContentFailure.get("node_id"));
                    assertEquals("The reason should match", expectedFailure.getMessage(), xContentFailure.get("reason"));
                    assertEquals(
                        "The caused by reason should match",
                        expectedFailure.getCause().getMessage(),
                        ((Map<?, ?>) xContentFailure.get("caused_by")).get("reason")
                    );
                    assertEquals(
                        "The type should match",
                        ElasticsearchException.getExceptionName(expectedFailure),
                        xContentFailure.get("type")
                    );
                }
            }

            var clusterName = (String) xContentMap.get("cluster_name");
            assertEquals(
                "The cluster name in the Xcontent should be equivalent to the original.",
                instance.getClusterName().value(),
                clusterName
            );

            var nodes = (List<Map<?, ?>>) xContentMap.get("nodes");
            var nodeResponses = nodes.stream()
                .map(
                    map -> new ClearBlobCacheNodeResponse(
                        DiscoveryNodeUtils.create((String) map.get("node_id")),
                        (long) map.get("timestamp"),
                        (int) map.get("evictions")
                    )
                )
                .toList();
            assertEquals("The list of nodes in XContent should be equivalent to the original.", instance.getNodes(), nodeResponses);
        }
    }
}
