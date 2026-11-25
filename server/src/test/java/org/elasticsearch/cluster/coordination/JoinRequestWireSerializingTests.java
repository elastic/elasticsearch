/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeUtils.randomDiscoveryNode;

/**
 * Wire serialization tests for {@link JoinRequest}.
 */
public class JoinRequestWireSerializingTests extends AbstractWireSerializingTestCase<JoinRequest> {

    @Override
    protected Writeable.Reader<JoinRequest> instanceReader() {
        return JoinRequest::new;
    }

    @Override
    protected JoinRequest createTestInstance() {
        DiscoveryNode sourceNode = randomDiscoveryNode();
        return new JoinRequest(
            sourceNode,
            CompatibilityVersionsUtils.fakeSystemIndicesRandom(),
            Set.of(generateRandomStringArray(randomInt(10), 10, false)),
            randomNonNegativeLong(),
            randomBoolean()
                ? Optional.empty()
                : Optional.of(
                    new Join(sourceNode, randomDiscoveryNode(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
                ),
            randomJoinAttributes()
        );
    }

    private static Map<String, String> randomJoinAttributes() {
        return randomMap(0, 3, () -> Tuple.tuple(randomAlphaOfLength(8), randomAlphaOfLength(5)));
    }

    public void testJoinAttributesAreDroppedForOlderVersions() throws IOException {
        JoinRequest request = createTestInstance();
        JoinRequest read = copyInstance(request, TransportVersionUtils.getPreviousVersion(JoinRequest.JOIN_REQUEST_ATTRIBUTES));
        assertEquals(Map.of(), read.getJoinAttributes());
        assertEquals(request.getFeatures(), read.getFeatures());
    }

    @Override
    protected JoinRequest mutateInstance(JoinRequest joinRequest) throws IOException {
        return switch (randomInt(5)) {
            case 0 -> {
                DiscoveryNode discoveryNode = randomDiscoveryNode();
                yield new JoinRequest(
                    discoveryNode,
                    joinRequest.getCompatibilityVersions(),
                    joinRequest.getFeatures(),
                    joinRequest.getMinimumTerm(),
                    // The optional join must always include the source node
                    joinRequest.getOptionalJoin().isPresent()
                        ? Optional.of(
                            new Join(
                                discoveryNode,
                                randomDiscoveryNode(),
                                randomNonNegativeLong(),
                                randomNonNegativeLong(),
                                randomNonNegativeLong()
                            )
                        )
                        : Optional.empty(),
                    joinRequest.getJoinAttributes()
                );
            }
            case 1 -> new JoinRequest(
                joinRequest.getSourceNode(),
                new CompatibilityVersions(
                    TransportVersionUtils.randomVersion(Set.of(joinRequest.getCompatibilityVersions().transportVersion())),
                    Map.of()
                ),
                joinRequest.getFeatures(),
                joinRequest.getMinimumTerm(),
                joinRequest.getOptionalJoin(),
                joinRequest.getJoinAttributes()
            );
            case 2 -> new JoinRequest(
                joinRequest.getSourceNode(),
                joinRequest.getCompatibilityVersions(),
                randomValueOtherThan(joinRequest.getFeatures(), () -> Set.of(generateRandomStringArray(10, 10, false))),
                joinRequest.getMinimumTerm(),
                joinRequest.getOptionalJoin(),
                joinRequest.getJoinAttributes()
            );
            case 3 -> new JoinRequest(
                joinRequest.getSourceNode(),
                joinRequest.getCompatibilityVersions(),
                joinRequest.getFeatures(),
                randomValueOtherThan(joinRequest.getMinimumTerm(), ESTestCase::randomNonNegativeLong),
                joinRequest.getOptionalJoin(),
                joinRequest.getJoinAttributes()
            );
            case 4 -> {
                Join newJoin = new Join(
                    joinRequest.getSourceNode(),
                    randomDiscoveryNode(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                );
                yield new JoinRequest(
                    joinRequest.getSourceNode(),
                    joinRequest.getCompatibilityVersions(),
                    joinRequest.getFeatures(),
                    joinRequest.getMinimumTerm(),
                    joinRequest.getOptionalJoin().isPresent() && randomBoolean() ? Optional.empty() : Optional.of(newJoin),
                    joinRequest.getJoinAttributes()
                );
            }
            case 5 -> new JoinRequest(
                joinRequest.getSourceNode(),
                joinRequest.getCompatibilityVersions(),
                joinRequest.getFeatures(),
                joinRequest.getMinimumTerm(),
                joinRequest.getOptionalJoin(),
                randomValueOtherThan(joinRequest.getJoinAttributes(), JoinRequestWireSerializingTests::randomJoinAttributes)
            );
            default -> throw new AssertionError();
        };
    }
}
