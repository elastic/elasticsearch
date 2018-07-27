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
import org.elasticsearch.discovery.Discovery.AckListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

public abstract class Publication extends AbstractComponent {

    private final AtomicReference<ApplyCommitRequest> applyCommitReference;
    private final List<PublicationTarget> publicationTargets;
    private final PublishRequest publishRequest;
    private final AckListener ackListener;
    private final DiscoveryNode localNode;
    private final LongSupplier currentTimeSupplier;
    private final long startTime;
    private boolean isCompleted;

    public Publication(Settings settings, PublishRequest publishRequest, AckListener ackListener, LongSupplier currentTimeSupplier) {
        super(settings);
        this.publishRequest = publishRequest;
        this.ackListener = ackListener;
        this.localNode = publishRequest.getAcceptedState().nodes().getLocalNode();
        this.currentTimeSupplier = currentTimeSupplier;
        applyCommitReference = new AtomicReference<>();
        startTime = currentTimeSupplier.getAsLong();

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
        publicationTargets.stream().filter(PublicationTarget::isActive).forEach(PublicationTarget::onTimeOut);
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

        for (final PublicationTarget target : publicationTargets) {
            if (target.publicationTargetStateMachine.isActive()) {
                return;
            }
        }

        for (final PublicationTarget target : publicationTargets) {
            if (target.discoveryNode.equals(localNode) && target.publicationTargetStateMachine.isFailed()) {
                logger.debug("onPossibleCompletion: [{}] failed on master", this);
                assert isCompleted == false;
                isCompleted = true;
                onCompletion(false);
                return;
            }
        }

        assert isCompleted == false;
        isCompleted = true;
        onCompletion(true);
        assert applyCommitReference.get() != null;
        logger.trace("onPossibleCompletion: [{}] was successful, applying new state locally", this);
    }

    // For assertions only: verify that this invariant holds
    private boolean publicationCompletedIffAllTargetsInactive() {
        for (final PublicationTarget target : publicationTargets) {
            if (target.publicationTargetStateMachine.isActive()) {
                return isCompleted == false;
            }
        }
        return isCompleted;
    }

    private void onPossibleCommitFailure() {
        if (applyCommitReference.get() != null) {
            onPossibleCompletion();
            return;
        }

        CoordinationState.VoteCollection possiblySuccessfulNodes = new CoordinationState.VoteCollection();
        for (PublicationTarget publicationTarget : publicationTargets) {
            if (publicationTarget.publicationTargetStateMachine.mayCommitInFuture()) {
                possiblySuccessfulNodes.addVote(publicationTarget.discoveryNode);
            } else {
                assert publicationTarget.publicationTargetStateMachine.isFailed() : publicationTarget.publicationTargetStateMachine;
            }
        }

        if (isPublishQuorum(possiblySuccessfulNodes) == false) {
            logger.debug("onPossibleCommitFailure: non-failed nodes do not form a quorum, so {} cannot succeed", this);
            publicationTargets.stream().filter(PublicationTarget::isActive).forEach(PublicationTarget::setFailed);
            onPossibleCompletion();
        }
    }

    protected abstract void onCompletion(boolean success);

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
            // TODO DANGER non-volatile, mutable variable requires synchronisation
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
            // everything here is immutable so no synchronisation required
            return discoveryNode.getId();
        }

        public void sendPublishRequest() {
            if (publicationTargetStateMachine.isFailed()) {
                return;
            }
            publicationTargetStateMachine.setState(PublicationTargetState.SENT_PUBLISH_REQUEST);
            Publication.this.sendPublishRequest(discoveryNode, publishRequest, new PublicationTarget.PublishResponseHandler());
            // TODO Can this ^ fail with an exception? Target should be failed if so.
            assert publicationCompletedIffAllTargetsInactive();
        }

        void handlePublishResponse(PublishResponse publishResponse) {
            assert publicationTargetStateMachine.isWaitingForQuorum() : publicationTargetStateMachine;

            logger.trace("handlePublishResponse: handling [{}] from [{}])", publishResponse, discoveryNode);
            if (applyCommitReference.get() != null) {
                sendApplyCommit();
            } else {
                Publication.this.handlePublishResponse(discoveryNode, publishResponse).ifPresent(applyCommit -> {
                    assert applyCommitReference.get() == null;
                    applyCommitReference.set(applyCommit);
                    ackListener.onCommit(TimeValue.timeValueMillis(currentTimeSupplier.getAsLong() - startTime));
                    publicationTargets.stream().filter(PublicationTarget::isWaitingForQuorum).forEach(PublicationTarget::sendApplyCommit);
                });
            }
        }

        public void sendApplyCommit() {
            publicationTargetStateMachine.setState(PublicationTargetState.SENT_APPLY_COMMIT);

            ApplyCommitRequest applyCommit = applyCommitReference.get();
            assert applyCommit != null;

            Publication.this.sendApplyCommit(discoveryNode, applyCommit, new PublicationTarget.ApplyCommitResponseHandler());
            assert publicationCompletedIffAllTargetsInactive();
        }

        public boolean isWaitingForQuorum() {
            return publicationTargetStateMachine.isWaitingForQuorum();
        }

        public boolean isActive() {
            return publicationTargetStateMachine.isActive();
        }

        public void setFailed() {
            assert isActive();
            publicationTargetStateMachine.setState(PublicationTargetState.FAILED);
            ackOnce(new ElasticsearchException("publication failed"));
        }

        public void onTimeOut() {
            assert isActive();
            publicationTargetStateMachine.setState(PublicationTargetState.FAILED);
            if (applyCommitReference.get() == null) {
                ackOnce(new ElasticsearchException("publication timed out"));
            }
        }

        public void onFaultyNode(DiscoveryNode faultyNode) {
            if (isActive() && discoveryNode.equals(faultyNode)) {
                logger.debug("onFaultyNode: [{}] is faulty, failing target in publication of version [{}] in term [{}]", faultyNode,
                    publishRequest.getAcceptedState().version(), publishRequest.getAcceptedState().term());
                setFailed();
                onPossibleCommitFailure();
            }
        }

        private void ackOnce(Exception e) {
            if (ackIsPending && localNode.equals(discoveryNode) == false) {
                ackIsPending = false;
                ackListener.onNodeAck(discoveryNode, e);
            }
        }

        private class PublishResponseHandler implements ActionListener<PublishWithJoinResponse> {

            @Override
            public void onResponse(PublishWithJoinResponse response) {
                if (publicationTargetStateMachine.isFailed()) {
                    if (applyCommitReference.get() != null) {
                        logger.trace("PublishResponseHandler.handleResponse: handling [{}] from [{}] while already failed",
                            response.getPublishResponse(), discoveryNode);
                        Publication.this.sendApplyCommit(discoveryNode, applyCommitReference.get(),
                            new PublicationTarget.ApplyCommitResponseHandler());
                    } else {
                        logger.debug("PublishResponseHandler.handleResponse: already failed, ignoring response from [{}]", discoveryNode);
                    }
                    assert publicationCompletedIffAllTargetsInactive();
                    return;
                }

                onPossibleJoin(discoveryNode, response);

                publicationTargetStateMachine.setState(PublicationTargetState.WAITING_FOR_QUORUM);
                handlePublishResponse(response.getPublishResponse());

                assert publicationCompletedIffAllTargetsInactive();
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
                assert publicationCompletedIffAllTargetsInactive();
                ackOnce(exp);
            }

        }

        private class ApplyCommitResponseHandler implements ActionListener<TransportResponse.Empty> {

            @Override
            public void onResponse(TransportResponse.Empty ignored) {
                if (publicationTargetStateMachine.isFailed()) {
                    logger.debug("ApplyCommitResponseHandler.handleResponse: already failed, ignoring response from [{}]",
                        discoveryNode);
                    ackOnce(null); // still need to ack
                    return;
                }
                publicationTargetStateMachine.setState(PublicationTargetState.APPLIED_COMMIT);
                onPossibleCompletion();
                assert publicationCompletedIffAllTargetsInactive();
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
                assert publicationCompletedIffAllTargetsInactive();
                ackOnce(exp);
            }
        }
    }
}
