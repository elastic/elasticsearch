/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.EqualsHashCodeTestUtils.CopyFunction;

import java.util.Optional;

public class MessagesTests extends ESTestCase {

    private DiscoveryNode createNode(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Version.CURRENT);
    }

    public void testJoinEqualsHashCodeSerialization() {
        Join initialJoin = new Join(createNode(randomAlphaOfLength(10)), createNode(randomAlphaOfLength(10)), randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong());
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialJoin,
            (CopyFunction<Join>) join -> copyWriteable(join, writableRegistry(), Join::new),
            join -> {
                switch (randomInt(4)) {
                    case 0:
                        // change sourceNode
                        return new Join(createNode(randomAlphaOfLength(20)), join.getTargetNode(), join.getTerm(),
                            join.getLastAcceptedTerm(), join.getLastAcceptedVersion());
                    case 1:
                        // change targetNode
                        return new Join(join.getSourceNode(), createNode(randomAlphaOfLength(20)), join.getTerm(),
                            join.getLastAcceptedTerm(), join.getLastAcceptedVersion());
                    case 2:
                        // change term
                        return new Join(join.getSourceNode(), join.getTargetNode(),
                            randomValueOtherThan(join.getTerm(), ESTestCase::randomNonNegativeLong), join.getLastAcceptedTerm(),
                            join.getLastAcceptedVersion());
                    case 3:
                        // change last accepted term
                        return new Join(join.getSourceNode(), join.getTargetNode(), join.getTerm(),
                            randomValueOtherThan(join.getLastAcceptedTerm(), ESTestCase::randomNonNegativeLong),
                            join.getLastAcceptedVersion());
                    case 4:
                        // change version
                        return new Join(join.getSourceNode(), join.getTargetNode(),
                            join.getTerm(), join.getLastAcceptedTerm(),
                            randomValueOtherThan(join.getLastAcceptedVersion(), ESTestCase::randomNonNegativeLong));
                    default:
                        throw new AssertionError();
                }
            });
    }

    public void testPublishRequestEqualsHashCode() {
        PublishRequest initialPublishRequest = new PublishRequest(randomClusterState());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialPublishRequest,
            publishRequest -> new PublishRequest(publishRequest.getAcceptedState()),
            in -> new PublishRequest(randomClusterState()));
    }

    public void testPublishResponseEqualsHashCodeSerialization() {
        PublishResponse initialPublishResponse = new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong());
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialPublishResponse,
            (CopyFunction<PublishResponse>) publishResponse -> copyWriteable(publishResponse, writableRegistry(), PublishResponse::new),
            publishResponse -> {
                switch (randomInt(1)) {
                    case 0:
                        // change term
                        return new PublishResponse(randomValueOtherThan(publishResponse.getTerm(), ESTestCase::randomNonNegativeLong),
                            publishResponse.getVersion());
                    case 1:
                        // change version
                        return new PublishResponse(publishResponse.getTerm(),
                            randomValueOtherThan(publishResponse.getVersion(), ESTestCase::randomNonNegativeLong));
                    default:
                        throw new AssertionError();
                }
            });
    }

    public void testPublishWithJoinResponseEqualsHashCodeSerialization() {
        PublishResponse initialPublishResponse = new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong());
        Join initialJoin = new Join(createNode(randomAlphaOfLength(10)), createNode(randomAlphaOfLength(10)), randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong());
        PublishWithJoinResponse initialPublishWithJoinResponse = new PublishWithJoinResponse(initialPublishResponse,
            randomBoolean() ? Optional.empty() : Optional.of(initialJoin));
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialPublishWithJoinResponse,
                (CopyFunction<PublishWithJoinResponse>) publishWithJoinResponse -> copyWriteable(publishWithJoinResponse,
                        writableRegistry(), PublishWithJoinResponse::new),
            publishWithJoinResponse -> {
                switch (randomInt(1)) {
                    case 0:
                        // change publish response
                        return new PublishWithJoinResponse(new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong()),
                            publishWithJoinResponse.getJoin());
                    case 1:
                        // change optional join
                        Join newJoin = new Join(createNode(randomAlphaOfLength(10)), createNode(randomAlphaOfLength(10)),
                            randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
                        return new PublishWithJoinResponse(publishWithJoinResponse.getPublishResponse(),
                            publishWithJoinResponse.getJoin().isPresent() && randomBoolean() ? Optional.empty() : Optional.of(newJoin));
                    default:
                        throw new AssertionError();
                }
            });
    }

    public void testStartJoinRequestEqualsHashCodeSerialization() {
        StartJoinRequest initialStartJoinRequest = new StartJoinRequest(createNode(randomAlphaOfLength(10)), randomNonNegativeLong());
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialStartJoinRequest,
                (CopyFunction<StartJoinRequest>) startJoinRequest -> copyWriteable(startJoinRequest, writableRegistry(),
                        StartJoinRequest::new),
            startJoinRequest -> {
                switch (randomInt(1)) {
                    case 0:
                        // change sourceNode
                        return new StartJoinRequest(createNode(randomAlphaOfLength(20)), startJoinRequest.getTerm());
                    case 1:
                        // change term
                        return new StartJoinRequest(startJoinRequest.getSourceNode(),
                            randomValueOtherThan(startJoinRequest.getTerm(), ESTestCase::randomNonNegativeLong));
                    default:
                        throw new AssertionError();
                }
            });
    }

    public void testApplyCommitEqualsHashCodeSerialization() {
        ApplyCommitRequest initialApplyCommit = new ApplyCommitRequest(createNode(randomAlphaOfLength(10)), randomNonNegativeLong(),
            randomNonNegativeLong());
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialApplyCommit,
                (CopyFunction<ApplyCommitRequest>) applyCommit -> copyWriteable(applyCommit, writableRegistry(), ApplyCommitRequest::new),
            applyCommit -> {
                switch (randomInt(2)) {
                    case 0:
                        // change sourceNode
                        return new ApplyCommitRequest(createNode(randomAlphaOfLength(20)), applyCommit.getTerm(), applyCommit.getVersion());
                    case 1:
                        // change term
                        return new ApplyCommitRequest(applyCommit.getSourceNode(),
                            randomValueOtherThan(applyCommit.getTerm(), ESTestCase::randomNonNegativeLong), applyCommit.getVersion());
                    case 2:
                        // change version
                        return new ApplyCommitRequest(applyCommit.getSourceNode(), applyCommit.getTerm(),
                            randomValueOtherThan(applyCommit.getVersion(), ESTestCase::randomNonNegativeLong));
                    default:
                        throw new AssertionError();
                }
            });
    }

    public void testJoinRequestEqualsHashCodeSerialization() {
        Join initialJoin = new Join(createNode(randomAlphaOfLength(10)), createNode(randomAlphaOfLength(10)), randomNonNegativeLong(),
            randomNonNegativeLong(), randomNonNegativeLong());
        JoinRequest initialJoinRequest = new JoinRequest(initialJoin.getSourceNode(),
            randomNonNegativeLong(), randomBoolean() ? Optional.empty() : Optional.of(initialJoin));
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialJoinRequest,
                (CopyFunction<JoinRequest>) joinRequest -> copyWriteable(joinRequest, writableRegistry(), JoinRequest::new),
            joinRequest -> {
                if (randomBoolean() && joinRequest.getOptionalJoin().isPresent() == false) {
                    return new JoinRequest(createNode(randomAlphaOfLength(10)),
                        joinRequest.getMinimumTerm(), joinRequest.getOptionalJoin());
                } else if (randomBoolean()) {
                    return new JoinRequest(joinRequest.getSourceNode(),
                        randomValueOtherThan(joinRequest.getMinimumTerm(), ESTestCase::randomNonNegativeLong),
                        joinRequest.getOptionalJoin());
                } else {
                    // change OptionalJoin
                    final Optional<Join> newOptionalJoin;
                    if (joinRequest.getOptionalJoin().isPresent() && randomBoolean()) {
                        newOptionalJoin = Optional.empty();
                    } else {
                        newOptionalJoin = Optional.of(new Join(joinRequest.getSourceNode(), createNode(randomAlphaOfLength(10)),
                            randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
                    }
                    return new JoinRequest(joinRequest.getSourceNode(), joinRequest.getMinimumTerm(), newOptionalJoin);
                }
            });
    }

    public ClusterState randomClusterState() {
        return CoordinationStateTests.clusterState(randomNonNegativeLong(), randomNonNegativeLong(), createNode(randomAlphaOfLength(10)),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false))),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false))),
            randomLong());
    }

    public void testPreVoteRequestEqualsHashCodeSerialization() {
        PreVoteRequest initialPreVoteRequest = new PreVoteRequest(createNode(randomAlphaOfLength(10)), randomNonNegativeLong());
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialPreVoteRequest,
                (CopyFunction<PreVoteRequest>) preVoteRequest -> copyWriteable(preVoteRequest, writableRegistry(), PreVoteRequest::new),
            preVoteRequest -> {
                if (randomBoolean()) {
                    return new PreVoteRequest(createNode(randomAlphaOfLength(10)), preVoteRequest.getCurrentTerm());
                } else {
                    return new PreVoteRequest(preVoteRequest.getSourceNode(), randomNonNegativeLong());
                }
            });
    }

    public void testPreVoteResponseEqualsHashCodeSerialization() {
        long currentTerm = randomNonNegativeLong();
        PreVoteResponse initialPreVoteResponse
            = new PreVoteResponse(currentTerm, randomLongBetween(1, currentTerm), randomNonNegativeLong());
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialPreVoteResponse,
                (CopyFunction<PreVoteResponse>) preVoteResponse -> copyWriteable(preVoteResponse, writableRegistry(), PreVoteResponse::new),
            preVoteResponse -> {
                switch (randomInt(2)) {
                    case 0:
                        assumeTrue("last-accepted term is Long.MAX_VALUE", preVoteResponse.getLastAcceptedTerm() < Long.MAX_VALUE);
                        return new PreVoteResponse(
                            randomValueOtherThan(preVoteResponse.getCurrentTerm(),
                                () -> randomLongBetween(preVoteResponse.getLastAcceptedTerm(), Long.MAX_VALUE)),
                            preVoteResponse.getLastAcceptedTerm(),
                            preVoteResponse.getLastAcceptedVersion());
                    case 1:
                        assumeTrue("current term is 1", 1 < preVoteResponse.getCurrentTerm());
                        return new PreVoteResponse(
                            preVoteResponse.getCurrentTerm(),
                            randomValueOtherThan(preVoteResponse.getLastAcceptedTerm(),
                                () -> randomLongBetween(1, preVoteResponse.getCurrentTerm())),
                            preVoteResponse.getLastAcceptedVersion());
                    case 2:
                        return new PreVoteResponse(
                            preVoteResponse.getCurrentTerm(),
                            preVoteResponse.getLastAcceptedTerm(),
                            randomValueOtherThan(preVoteResponse.getLastAcceptedVersion(), ESTestCase::randomNonNegativeLong));
                    default:
                        throw new AssertionError();
                }
            });
    }
}
