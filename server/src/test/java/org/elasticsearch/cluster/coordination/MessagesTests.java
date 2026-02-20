/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class MessagesTests extends ESTestCase {

    private DiscoveryNode createNode(String id) {
        return DiscoveryNodeUtils.create(id);
    }

    public void testPublishRequestEqualsHashCode() {
        PublishRequest initialPublishRequest = new PublishRequest(randomClusterState());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialPublishRequest,
            publishRequest -> new PublishRequest(publishRequest.getAcceptedState()),
            in -> new PublishRequest(randomClusterState())
        );
    }

    public void testPublishResponseEqualsHashCodeSerialization() {
        PublishResponse initialPublishResponse = new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialPublishResponse,
            publishResponse -> copyWriteable(publishResponse, writableRegistry(), PublishResponse::new),
            publishResponse -> switch (randomInt(1)) {
                case 0 ->
                    // change term
                    new PublishResponse(
                        randomValueOtherThan(publishResponse.getTerm(), ESTestCase::randomNonNegativeLong),
                        publishResponse.getVersion()
                    );
                case 1 ->
                    // change version
                    new PublishResponse(
                        publishResponse.getTerm(),
                        randomValueOtherThan(publishResponse.getVersion(), ESTestCase::randomNonNegativeLong)
                    );
                default -> throw new AssertionError();
            }
        );
    }

    public ClusterState randomClusterState() {
        return CoordinationStateTests.clusterState(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            createNode(randomAlphaOfLength(10)),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false))),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false))),
            randomLong()
        );
    }

}
