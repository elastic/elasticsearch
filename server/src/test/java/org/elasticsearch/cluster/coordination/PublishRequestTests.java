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
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.UUID;

public class PublishRequestTests extends ESTestCase {

    /** Verifies that {@link PublishRequest#getAcceptedState()} returns the same instance passed to the constructor. */
    public void testGetAcceptedState() {
        ClusterState clusterState = randomClusterState();
        PublishRequest publishRequest = new PublishRequest(clusterState);
        assertSame(clusterState, publishRequest.getAcceptedState());
    }

    /** Verifies equals/hashCode contract: copy equals original, mutation does not, and hashCode matches equals. */
    public void testPublishRequestEqualsHashCode() {
        PublishRequest initialPublishRequest = new PublishRequest(randomClusterState());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialPublishRequest,
            publishRequest -> new PublishRequest(publishRequest.getAcceptedState()),
            in -> new PublishRequest(randomClusterState())
        );
    }

    /** Two requests built from the same cluster state are equal and have the same hashCode. */
    public void testEqualsWithSameTermAndVersion() {
        ClusterState clusterState = randomClusterState();
        PublishRequest publishRequest = new PublishRequest(clusterState);
        PublishRequest sameTermAndVersion = new PublishRequest(clusterState);
        assertEquals(publishRequest, sameTermAndVersion);
        assertEquals(publishRequest.hashCode(), sameTermAndVersion.hashCode());
    }

    /** Requests with different terms are not equal and have different hashCodes. */
    public void testEqualsWithDifferentTerm() {
        ClusterState clusterState = randomClusterState();
        PublishRequest publishRequest = new PublishRequest(clusterState);
        ClusterState differentTerm = CoordinationStateTests.clusterState(
            randomValueOtherThan(clusterState.term(), ESTestCase::randomNonNegativeLong),
            clusterState.version(),
            createNode(UUID.randomUUID().toString()),
            clusterState.getLastCommittedConfiguration(),
            clusterState.getLastAcceptedConfiguration(),
            randomLong()
        );
        PublishRequest other = new PublishRequest(differentTerm);
        assertNotEquals(publishRequest, other);
        assertNotEquals(publishRequest.hashCode(), other.hashCode());
    }

    /** Requests with different versions are not equal and have different hashCodes. */
    public void testEqualsWithDifferentVersion() {
        ClusterState clusterState = randomClusterState();
        PublishRequest publishRequest = new PublishRequest(clusterState);
        ClusterState differentVersion = CoordinationStateTests.clusterState(
            clusterState.term(),
            randomValueOtherThan(clusterState.version(), ESTestCase::randomNonNegativeLong),
            createNode(UUID.randomUUID().toString()),
            clusterState.getLastCommittedConfiguration(),
            clusterState.getLastAcceptedConfiguration(),
            randomLong()
        );
        PublishRequest other = new PublishRequest(differentVersion);
        assertNotEquals(publishRequest, other);
        assertNotEquals(publishRequest.hashCode(), other.hashCode());
    }

    /** Verifies that equals returns false for a null argument. */
    public void testEqualsNull() {
        PublishRequest publishRequest = new PublishRequest(randomClusterState());
        assertFalse(publishRequest.equals(null));
    }

    /** Verifies that equals returns false for non-PublishRequest types. */
    public void testEqualsIncompatibleType() {
        PublishRequest publishRequest = new PublishRequest(randomClusterState());
        assertNotEquals(publishRequest, randomClusterState());
        assertNotEquals("PublishRequest", publishRequest);
    }

    /** Verifies that toString includes the class name, term, and version. */
    public void testToString() {
        ClusterState clusterState = randomClusterState();
        PublishRequest publishRequest = new PublishRequest(clusterState);
        String string = publishRequest.toString();
        assertTrue(string.contains("PublishRequest"));
        assertTrue(string.contains("term=" + clusterState.term()));
        assertTrue(string.contains("version=" + clusterState.version()));
    }

    private ClusterState randomClusterState() {
        return CoordinationStateTests.clusterState(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            createNode(UUID.randomUUID().toString()),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false))),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false))),
            randomLong()
        );
    }

    private DiscoveryNode createNode(String id) {
        return DiscoveryNodeUtils.builder(id)
            .address(new TransportAddress(TransportAddress.META_ADDRESS, randomIntBetween(1, 65535)))
            .build();
    }
}
