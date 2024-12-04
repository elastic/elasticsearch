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

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class GetBlobStoreStatsNodeResponseTests extends AbstractWireSerializingTestCase<GetBlobStoreStatsNodeResponse> {

    @Override
    protected Writeable.Reader<GetBlobStoreStatsNodeResponse> instanceReader() {
        return GetBlobStoreStatsNodeResponse::new;
    }

    @Override
    protected GetBlobStoreStatsNodeResponse createTestInstance() {
        return createLabeledTestInstance("1", randomRequestNames());
    }

    public static GetBlobStoreStatsNodeResponse createLabeledTestInstance(String label, Set<String> requestNames) {
        return new GetBlobStoreStatsNodeResponse(
            DiscoveryNodeUtils.builder("node_" + label).roles(Set.copyOf(randomSubsetOf(DiscoveryNodeRole.roles()))).build(),
            randomRepositoryStats(requestNames),
            randomRepositoryStats(requestNames)
        );
    }

    @Override
    protected GetBlobStoreStatsNodeResponse mutateInstance(GetBlobStoreStatsNodeResponse instance) {
        return switch (between(0, 3)) {
            case 0 -> new GetBlobStoreStatsNodeResponse(
                DiscoveryNodeUtils.builder(randomAlphaOfLength(5)).roles(instance.getNode().getRoles()).build(),
                instance.getRepositoryStats(),
                instance.getObsRepositoryStats()
            );
            case 1 -> new GetBlobStoreStatsNodeResponse(
                instance.getNode(),
                randomRepositoryStats(
                    randomValueOtherThanMany(
                        names -> names.equals(instance.getRepositoryStats().actionStats.keySet()),
                        GetBlobStoreStatsNodeResponseTests::randomRequestNames
                    )
                ),
                instance.getObsRepositoryStats()
            );
            case 2 -> {
                if (instance.getRepositoryStats().actionStats.keySet().isEmpty()) {
                    // file
                    yield new GetBlobStoreStatsNodeResponse(
                        instance.getNode(),
                        randomRepositoryStats(randomValueOtherThan(Set.of(), GetBlobStoreStatsNodeResponseTests::randomRequestNames)),
                        instance.getObsRepositoryStats()
                    );
                } else {
                    yield new GetBlobStoreStatsNodeResponse(
                        instance.getNode(),
                        instance.getRepositoryStats().merge(randomRepositoryStats(instance.getRepositoryStats().actionStats.keySet())),
                        instance.getObsRepositoryStats()
                    );
                }
            }
            case 3 -> {
                if (instance.getObsRepositoryStats().actionStats.keySet().isEmpty()) {
                    // file
                    yield new GetBlobStoreStatsNodeResponse(
                        instance.getNode(),
                        instance.getRepositoryStats(),
                        randomRepositoryStats(randomValueOtherThan(Set.of(), GetBlobStoreStatsNodeResponseTests::randomRequestNames))
                    );
                } else {
                    yield new GetBlobStoreStatsNodeResponse(
                        instance.getNode(),
                        instance.getRepositoryStats(),
                        instance.getObsRepositoryStats().merge(randomRepositoryStats(instance.getObsRepositoryStats().actionStats.keySet()))

                    );
                }
            }
            default -> throw new AssertionError("option is out of range");
        };
    }

    public void testToXContent() throws IOException {
        final GetBlobStoreStatsNodeResponse instance = createTestInstance();
        final Map<String, Object> map = XContentTestUtils.convertToMap(instance);
        assertThat(map.keySet(), containsInAnyOrder("node_1"));
        assertThat(
            map.get("node_1"),
            equalTo(
                Map.of(
                    "object_store_stats",
                    Map.of(
                        "request_counts",
                        Maps.transformValues(
                            GetBlobStoreStatsNodeResponse.getRequestCounts(instance.getRepositoryStats()),
                            Math::toIntExact
                        )
                    ),
                    "operational_backup_service_stats",
                    Map.of(
                        "request_counts",
                        Maps.transformValues(
                            GetBlobStoreStatsNodeResponse.getRequestCounts(instance.getObsRepositoryStats()),
                            Math::toIntExact
                        )
                    )
                )
            )
        );
    }

    public static RepositoryStats randomRepositoryStats(Set<String> requestNames) {
        final Map<String, BlobStoreActionStats> requestCounts = requestNames.stream().map(name -> {
            final long operations = randomLongBetween(0, 9999);
            return Map.entry(name, new BlobStoreActionStats(operations, randomLongBetween(operations, 9999)));
        }).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        return new RepositoryStats(requestCounts);
    }

    public static Set<String> randomRequestNames() {
        return randomFrom(
            Set.of("GetObject", "ListObjects", "PutObject", "PutMultipartObject"), // s3
            Set.of("GetObject", "ListObjects", "InsertObject"), // gcs
            Set.of("GetBlob", "ListBlobs", "GetBlobProperties", "PutBlob", "PutBlock", "PutBlockList"), // azure
            Set.of() // file
        );
    }
}
