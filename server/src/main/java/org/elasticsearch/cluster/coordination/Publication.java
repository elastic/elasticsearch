/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.Discovery.AckListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

public abstract class Publication extends AbstractComponent {

    private final List<PublicationTarget> publicationTargets;
    private final PublishRequest publishRequest;
    private final AckListener ackListener;
    private final LongSupplier currentTimeSupplier;
    private final long startTime;

    private Optional<ApplyCommitRequest> applyCommitRequest; // set when state is committed
    private boolean isCompleted; // set when publication is completed
    private boolean timedOut; // set when publication timed out

    public Publication(Settings settings, PublishRequest publishRequest, AckListener ackListener, LongSupplier currentTimeSupplier) {
        super(settings);
        this.publishRequest = publishRequest;
        this.ackListener = ackListener;
        this.currentTimeSupplier = currentTimeSupplier;
        startTime = currentTimeSupplier.getAsLong();
        applyCommitRequest = Optional.empty();
        publicationTargets = new ArrayList<>(publishRequest.getAcceptedState().getNodes().getNodes().size());
        publishRequest.getAcceptedState().getNodes().iterator().forEachRemaining(n -> publicationTargets.add(new PublicationTarget(n)));
    }

    public void start(Set<DiscoveryNode> faultyNodes) {
        logger.trace("publishing {} to {}", publishRequest, publicationTargets);

        for (final DiscoveryNode faultyNode : faultyNodes) {
            onFaultyNode(faultyNode);
        }
        onPossibleCommitFailure();
        publicationTargets.forEach(PublicationTarget::sendPublishRequest);
    }

    public void onTimeout() {
        assert timedOut == false;
        timedOut = true;
        if (applyCommitRequest.isPresent() == false) {
            logger.debug("onTimeout: [{}] timed out before committing", this);
            // fail all current publications
            final Exception e = new ElasticsearchException("publication timed out before committing");
            publicationTargets.stream().filter(PublicationTarget::isActive).forEach(pt -> pt.setFailed(e));
        }
        onPossibleCompletion();
    }

    public void onFaultyNode(DiscoveryNode faultyNode) {
        publicationTargets.forEach(t -> t.onFaultyNode(faultyNode));
        onPossibleCompletion();
    }

    private void onPossibleCompletion() {
        if (isCompleted) {
            return;
        }

        if (timedOut == false) {
            for (final PublicationTarget target : publicationTargets) {
                if (target.publicationTargetStateMachine.isActive()) {
                    return;
                }
            }
        }

        if (applyCommitRequest.isPresent() == false) {
            logger.debug("onPossibleCompletion: [{}] commit failed", this);
            assert isCompleted == false;
            isCompleted = true;
            onCompletion(false);
            return;
        }

        assert isCompleted == false;
        isCompleted = true;
        onCompletion(true);
        assert applyCommitRequest.isPresent();
        logger.trace("onPossibleCompletion: [{}] was successful, applying new state locally", this);
    }

    // For assertions only: verify that this invariant holds
    private boolean publicationCompletedIffAllTargetsInactiveOrTimedOut() {
        if (timedOut == false) {
            for (final PublicationTarget target : publicationTargets) {
                if (target.publicationTargetStateMachine.isActive()) {
                    return isCompleted == false;
                }
            }
        }
        return isCompleted;
    }

    private void onPossibleCommitFailure() {
        if (applyCommitRequest.isPresent()) {
            onPossibleCompletion();
            return;
        }

        final CoordinationState.VoteCollection possiblySuccessfulNodes = new CoordinationState.VoteCollection();
        for (PublicationTarget publicationTarget : publicationTargets) {
            if (publicationTarget.publicationTargetStateMachine.mayCommitInFuture()) {
                possiblySuccessfulNodes.addVote(publicationTarget.discoveryNode);
            } else {
                assert publicationTarget.publicationTargetStateMachine.isFailed() : publicationTarget.publicationTargetStateMachine;
            }
        }

        if (isPublishQuorum(possiblySuccessfulNodes) == false) {
            logger.debug("onPossibleCommitFailure: non-failed nodes do not form a quorum, so {} cannot succeed", this);
            Exception e = new Discovery.FailedToCommitClusterStateException("non-failed nodes do not form a quorum");
            publicationTargets.stream().filter(PublicationTarget::isActive).forEach(pt -> pt.setFailed(e));
            onPossibleCompletion();
        }
    }

    protected abstract void onCompletion(boolean committed);

    protected abstract boolean isPublishQuorum(CoordinationState.VoteCollection votes);

    protected abstract Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse);

    protected abstract void onPossibleJoin(DiscoveryNode sourceNode, PublishWithJoinResponse response);

    protected abstract void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                               ActionListener<PublishWithJoinResponse> responseActionListener);

    protected abstract void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                            ActionListener<TransportResponse.Empty> responseActionListener);

    @Override
    public String toString() {
        return "Publication{term=" + publishRequest.getAcceptedState().term() +
            ", version=" + publishRequest.getAcceptedState().version() + '}';
    }

    enum PublicationTargetState {
        NOT_STARTED,
        FAILED,
        SENT_PUBLISH_REQUEST,
        WAITING_FOR_QUORUM,
        SENT_APPLY_COMMIT,
        APPLIED_COMMIT,
    }

    static class PublicationTargetStateMachine {
        private PublicationTargetState state = PublicationTargetState.NOT_STARTED;

        public void setState(PublicationTargetState newState) {
            switch (newState) {
                case NOT_STARTED:
                    assert false : state + " -> " + newState;
                    break;
                case SENT_PUBLISH_REQUEST:
                    assert state == PublicationTargetState.NOT_STARTED : state + " -> " + newState;
                    break;
                case WAITING_FOR_QUORUM:
                    assert state == PublicationTargetState.SENT_PUBLISH_REQUEST : state + " -> " + newState;
                    break;
                case SENT_APPLY_COMMIT:
                    assert state == PublicationTargetState.WAITING_FOR_QUORUM : state + " -> " + newState;
                    break;
                case APPLIED_COMMIT:
                    assert state == PublicationTargetState.SENT_APPLY_COMMIT : state + " -> " + newState;
                    break;
                case FAILED:
                    assert state != PublicationTargetState.APPLIED_COMMIT : state + " -> " + newState;
                    break;
            }
            state = newState;
        }

        public boolean isActive() {
            return state != PublicationTargetState.FAILED
                && state != PublicationTargetState.APPLIED_COMMIT;
        }

        public boolean isWaitingForQuorum() {
            return state == PublicationTargetState.WAITING_FOR_QUORUM;
        }

        public boolean mayCommitInFuture() {
            return (state == PublicationTargetState.NOT_STARTED
                || state == PublicationTargetState.SENT_PUBLISH_REQUEST
                || state == PublicationTargetState.WAITING_FOR_QUORUM);
        }

        public boolean isFailed() {
            return state == PublicationTargetState.FAILED;
        }

        @Override
        public String toString() {
            return state.toString();
        }
    }

    private class PublicationTarget {
        private final DiscoveryNode discoveryNode;
        private final PublicationTargetStateMachine publicationTargetStateMachine = new PublicationTargetStateMachine();
        private boolean ackIsPending = true;

        private PublicationTarget(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        @Override
        public String toString() {
            return discoveryNode.getId();
        }

        public void sendPublishRequest() {
            if (publicationTargetStateMachine.isFailed()) {
                return;
            }
            publicationTargetStateMachine.setState(PublicationTargetState.SENT_PUBLISH_REQUEST);
            Publication.this.sendPublishRequest(discoveryNode, publishRequest, new PublicationTarget.PublishResponseHandler());
            // TODO Can this ^ fail with an exception? Target should be failed if so.
            assert publicationCompletedIffAllTargetsInactiveOrTimedOut();
        }

        void handlePublishResponse(PublishResponse publishResponse) {
            assert publicationTargetStateMachine.isWaitingForQuorum() : publicationTargetStateMachine;
            logger.trace("handlePublishResponse: handling [{}] from [{}])", publishResponse, discoveryNode);
            if (applyCommitRequest.isPresent()) {
                sendApplyCommit();
            } else {
                Publication.this.handlePublishResponse(discoveryNode, publishResponse).ifPresent(applyCommit -> {
                    assert applyCommitRequest.isPresent() == false;
                    applyCommitRequest = Optional.of(applyCommit);
                    ackListener.onCommit(TimeValue.timeValueMillis(currentTimeSupplier.getAsLong() - startTime));
                    publicationTargets.stream().filter(PublicationTarget::isWaitingForQuorum).forEach(PublicationTarget::sendApplyCommit);
                });
            }
        }

        public void sendApplyCommit() {
            publicationTargetStateMachine.setState(PublicationTargetState.SENT_APPLY_COMMIT);
            assert applyCommitRequest.isPresent();
            Publication.this.sendApplyCommit(discoveryNode, applyCommitRequest.get(), new PublicationTarget.ApplyCommitResponseHandler());
            assert publicationCompletedIffAllTargetsInactiveOrTimedOut();
        }

        public boolean isWaitingForQuorum() {
            return publicationTargetStateMachine.isWaitingForQuorum();
        }

        public boolean isActive() {
            return publicationTargetStateMachine.isActive();
        }

        public void setFailed(Exception e) {
            assert isActive();
            publicationTargetStateMachine.setState(PublicationTargetState.FAILED);
            ackOnce(e);
        }

        public void onFaultyNode(DiscoveryNode faultyNode) {
            if (isActive() && discoveryNode.equals(faultyNode)) {
                logger.debug("onFaultyNode: [{}] is faulty, failing target in publication of version [{}] in term [{}]", faultyNode,
                    publishRequest.getAcceptedState().version(), publishRequest.getAcceptedState().term());
                setFailed(new ElasticsearchException("faulty node"));
                onPossibleCommitFailure();
            }
        }

        private void ackOnce(Exception e) {
            if (ackIsPending) {
                ackIsPending = false;
                ackListener.onNodeAck(discoveryNode, e);
            }
        }

        private class PublishResponseHandler implements ActionListener<PublishWithJoinResponse> {

            @Override
            public void onResponse(PublishWithJoinResponse response) {
                if (publicationTargetStateMachine.isFailed()) {
                    logger.debug("PublishResponseHandler.handleResponse: already failed, ignoring response from [{}]", discoveryNode);
                    assert publicationCompletedIffAllTargetsInactiveOrTimedOut();
                    return;
                }

                onPossibleJoin(discoveryNode, response);

                publicationTargetStateMachine.setState(PublicationTargetState.WAITING_FOR_QUORUM);
                handlePublishResponse(response.getPublishResponse());

                assert publicationCompletedIffAllTargetsInactiveOrTimedOut();
            }

            @Override
            public void onFailure(Exception e) {
                assert e instanceof TransportException;
                final TransportException exp = (TransportException) e;
                if (exp.getRootCause() instanceof CoordinationStateRejectedException) {
                    logger.debug("PublishResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                } else {
                    logger.debug(() -> new ParameterizedMessage("PublishResponseHandler: [{}] failed", discoveryNode), exp);
                }
                publicationTargetStateMachine.setState(PublicationTargetState.FAILED);
                onPossibleCommitFailure();
                assert publicationCompletedIffAllTargetsInactiveOrTimedOut();
                ackOnce(exp);
            }

        }

        private class ApplyCommitResponseHandler implements ActionListener<TransportResponse.Empty> {

            @Override
            public void onResponse(TransportResponse.Empty ignored) {
                if (publicationTargetStateMachine.isFailed()) {
                    logger.debug("ApplyCommitResponseHandler.handleResponse: already failed, ignoring response from [{}]",
                        discoveryNode);
                    return;
                }
                publicationTargetStateMachine.setState(PublicationTargetState.APPLIED_COMMIT);
                onPossibleCompletion();
                assert publicationCompletedIffAllTargetsInactiveOrTimedOut();
                ackOnce(null);
            }

            @Override
            public void onFailure(Exception e) {
                assert e instanceof TransportException;
                final TransportException exp = (TransportException) e;
                if (exp.getRootCause() instanceof CoordinationStateRejectedException) {
                    logger.debug("ApplyCommitResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                } else {
                    logger.debug(() -> new ParameterizedMessage("ApplyCommitResponseHandler: [{}] failed", discoveryNode), exp);
                }
                publicationTargetStateMachine.setState(PublicationTargetState.FAILED);
                onPossibleCompletion();
                assert publicationCompletedIffAllTargetsInactiveOrTimedOut();
                ackOnce(exp);
            }
        }
    }
}
