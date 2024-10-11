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
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MessagesTests extends ESTestCase {

    private DiscoveryNode createNode(String id) {
        return DiscoveryNodeUtils.create(id);
    }

    public void testJoinEqualsHashCodeSerialization() {
        Join initialJoin = new Join(
            createNode(randomAlphaOfLength(10)),
            createNode(randomAlphaOfLength(10)),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialJoin,
            join -> copyWriteable(join, writableRegistry(), Join::new),
            join -> switch (randomInt(4)) {
                case 0 ->
                    // change sourceNode
                    new Join(
                        createNode(randomAlphaOfLength(20)),
                        join.masterCandidateNode(),
                        join.term(),
                        join.lastAcceptedTerm(),
                        join.lastAcceptedVersion()
                    );
                case 1 ->
                    // change targetNode
                    new Join(
                        join.votingNode(),
                        createNode(randomAlphaOfLength(20)),
                        join.term(),
                        join.lastAcceptedTerm(),
                        join.lastAcceptedVersion()
                    );
                case 2 ->
                    // change term
                    new Join(
                        join.votingNode(),
                        join.masterCandidateNode(),
                        randomValueOtherThan(join.term(), ESTestCase::randomNonNegativeLong),
                        join.lastAcceptedTerm(),
                        join.lastAcceptedVersion()
                    );
                case 3 ->
                    // change last accepted term
                    new Join(
                        join.votingNode(),
                        join.masterCandidateNode(),
                        join.term(),
                        randomValueOtherThan(join.lastAcceptedTerm(), ESTestCase::randomNonNegativeLong),
                        join.lastAcceptedVersion()
                    );
                case 4 ->
                    // change version
                    new Join(
                        join.votingNode(),
                        join.masterCandidateNode(),
                        join.term(),
                        join.lastAcceptedTerm(),
                        randomValueOtherThan(join.lastAcceptedVersion(), ESTestCase::randomNonNegativeLong)
                    );
                default -> throw new AssertionError();
            }
        );
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

    public void testPublishWithJoinResponseEqualsHashCodeSerialization() {
        PublishResponse initialPublishResponse = new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong());
        Join initialJoin = new Join(
            createNode(randomAlphaOfLength(10)),
            createNode(randomAlphaOfLength(10)),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        PublishWithJoinResponse initialPublishWithJoinResponse = new PublishWithJoinResponse(
            initialPublishResponse,
            randomBoolean() ? Optional.empty() : Optional.of(initialJoin)
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialPublishWithJoinResponse,
            publishWithJoinResponse -> copyWriteable(publishWithJoinResponse, writableRegistry(), PublishWithJoinResponse::new),
            publishWithJoinResponse -> switch (randomInt(1)) {
                case 0 ->
                    // change publish response
                    new PublishWithJoinResponse(
                        new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong()),
                        publishWithJoinResponse.getJoin()
                    );
                case 1 -> {
                    // change optional join
                    Join newJoin = new Join(
                        createNode(randomAlphaOfLength(10)),
                        createNode(randomAlphaOfLength(10)),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    );
                    yield new PublishWithJoinResponse(
                        publishWithJoinResponse.getPublishResponse(),
                        publishWithJoinResponse.getJoin().isPresent() && randomBoolean() ? Optional.empty() : Optional.of(newJoin)
                    );
                }
                default -> throw new AssertionError();
            }
        );
    }

    public void testStartJoinRequestEqualsHashCodeSerialization() {
        StartJoinRequest initialStartJoinRequest = new StartJoinRequest(createNode(randomAlphaOfLength(10)), randomNonNegativeLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialStartJoinRequest,
            startJoinRequest -> copyWriteable(startJoinRequest, writableRegistry(), StartJoinRequest::new),
            startJoinRequest -> switch (randomInt(1)) {
                case 0 ->
                    // change sourceNode
                    new StartJoinRequest(createNode(randomAlphaOfLength(20)), startJoinRequest.getTerm());
                case 1 ->
                    // change term
                    new StartJoinRequest(
                        startJoinRequest.getMasterCandidateNode(),
                        randomValueOtherThan(startJoinRequest.getTerm(), ESTestCase::randomNonNegativeLong)
                    );
                default -> throw new AssertionError();
            }
        );
    }

    public void testApplyCommitEqualsHashCodeSerialization() {
        ApplyCommitRequest initialApplyCommit = new ApplyCommitRequest(
            createNode(randomAlphaOfLength(10)),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialApplyCommit,
            applyCommit -> copyWriteable(applyCommit, writableRegistry(), ApplyCommitRequest::new),
            applyCommit -> switch (randomInt(2)) {
                case 0 ->
                    // change sourceNode
                    new ApplyCommitRequest(createNode(randomAlphaOfLength(20)), applyCommit.getTerm(), applyCommit.getVersion());
                case 1 ->
                    // change term
                    new ApplyCommitRequest(
                        applyCommit.getSourceNode(),
                        randomValueOtherThan(applyCommit.getTerm(), ESTestCase::randomNonNegativeLong),
                        applyCommit.getVersion()
                    );
                case 2 ->
                    // change version
                    new ApplyCommitRequest(
                        applyCommit.getSourceNode(),
                        applyCommit.getTerm(),
                        randomValueOtherThan(applyCommit.getVersion(), ESTestCase::randomNonNegativeLong)
                    );
                default -> throw new AssertionError();
            }
        );
    }

    public void testJoinRequestEqualsHashCodeSerialization() {
        Join initialJoin = new Join(
            createNode(randomAlphaOfLength(10)),
            createNode(randomAlphaOfLength(10)),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        JoinRequest initialJoinRequest = new JoinRequest(
            initialJoin.votingNode(),
            CompatibilityVersionsUtils.fakeSystemIndicesRandom(),
            Set.of(generateRandomStringArray(10, 10, false)),
            randomNonNegativeLong(),
            randomBoolean() ? Optional.empty() : Optional.of(initialJoin)
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialJoinRequest,
            joinRequest -> copyWriteable(joinRequest, writableRegistry(), JoinRequest::new),
            joinRequest -> switch (randomInt(4)) {
                case 0 -> {
                    assumeTrue("Optional join needs to be empty", joinRequest.getOptionalJoin().isEmpty());
                    yield new JoinRequest(
                        createNode(randomAlphaOfLength(10)),
                        joinRequest.getCompatibilityVersions(),
                        joinRequest.getFeatures(),
                        joinRequest.getMinimumTerm(),
                        joinRequest.getOptionalJoin()
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
                    joinRequest.getOptionalJoin()
                );
                case 2 -> new JoinRequest(
                    joinRequest.getSourceNode(),
                    joinRequest.getCompatibilityVersions(),
                    randomValueOtherThan(joinRequest.getFeatures(), () -> Set.of(generateRandomStringArray(10, 10, false))),
                    joinRequest.getMinimumTerm(),
                    joinRequest.getOptionalJoin()
                );
                case 3 -> new JoinRequest(
                    joinRequest.getSourceNode(),
                    joinRequest.getCompatibilityVersions(),
                    joinRequest.getFeatures(),
                    randomValueOtherThan(joinRequest.getMinimumTerm(), ESTestCase::randomNonNegativeLong),
                    joinRequest.getOptionalJoin()
                );
                case 4 -> {
                    // change OptionalJoin
                    Join newJoin = new Join(
                        joinRequest.getSourceNode(),
                        createNode(randomAlphaOfLength(10)),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    );
                    yield new JoinRequest(
                        joinRequest.getSourceNode(),
                        joinRequest.getCompatibilityVersions(),
                        joinRequest.getFeatures(),
                        joinRequest.getMinimumTerm(),
                        joinRequest.getOptionalJoin().isPresent() && randomBoolean() ? Optional.empty() : Optional.of(newJoin)
                    );
                }
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

    public void testPreVoteRequestEqualsHashCodeSerialization() {
        PreVoteRequest initialPreVoteRequest = new PreVoteRequest(createNode(randomAlphaOfLength(10)), randomNonNegativeLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialPreVoteRequest,
            preVoteRequest -> copyWriteable(preVoteRequest, writableRegistry(), PreVoteRequest::new),
            preVoteRequest -> switch (randomInt(1)) {
                case 0 -> new PreVoteRequest(createNode(randomAlphaOfLength(10)), preVoteRequest.getCurrentTerm());
                case 1 -> new PreVoteRequest(preVoteRequest.getSourceNode(), randomNonNegativeLong());
                default -> throw new AssertionError();
            }
        );
    }

    public void testPreVoteResponseEqualsHashCodeSerialization() {
        long currentTerm = randomNonNegativeLong();
        PreVoteResponse initialPreVoteResponse = new PreVoteResponse(
            currentTerm,
            randomLongBetween(1, currentTerm),
            randomNonNegativeLong()
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialPreVoteResponse,
            preVoteResponse -> copyWriteable(preVoteResponse, writableRegistry(), PreVoteResponse::new),
            preVoteResponse -> switch (randomInt(2)) {
                case 0 -> {
                    assumeTrue("last-accepted term is Long.MAX_VALUE", preVoteResponse.getLastAcceptedTerm() < Long.MAX_VALUE);
                    yield new PreVoteResponse(
                        randomValueOtherThan(
                            preVoteResponse.getCurrentTerm(),
                            () -> randomLongBetween(preVoteResponse.getLastAcceptedTerm(), Long.MAX_VALUE)
                        ),
                        preVoteResponse.getLastAcceptedTerm(),
                        preVoteResponse.getLastAcceptedVersion()
                    );
                }
                case 1 -> {
                    assumeTrue("current term is 1", 1 < preVoteResponse.getCurrentTerm());
                    yield new PreVoteResponse(
                        preVoteResponse.getCurrentTerm(),
                        randomValueOtherThan(
                            preVoteResponse.getLastAcceptedTerm(),
                            () -> randomLongBetween(1, preVoteResponse.getCurrentTerm())
                        ),
                        preVoteResponse.getLastAcceptedVersion()
                    );
                }
                case 2 -> new PreVoteResponse(
                    preVoteResponse.getCurrentTerm(),
                    preVoteResponse.getLastAcceptedTerm(),
                    randomValueOtherThan(preVoteResponse.getLastAcceptedVersion(), ESTestCase::randomNonNegativeLong)
                );
                default -> throw new AssertionError();
            }
        );
    }
}
