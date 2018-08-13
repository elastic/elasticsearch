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
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;

public abstract class ElectionScheduler extends AbstractComponent {

    /*
     * It's provably impossible to guarantee that any leader election algorithm ever elects a leader, but they generally work (with
     * probability that approaches 1 over time) as long as elections occur sufficiently infrequently, compared to the time it takes to send
     * a message to another node and receive a response back. We do not know the round-trip latency here, but we can approximate it by
     * attempting elections randomly at reasonably high frequency and backing off (linearly) until one of them succeeds. We also place an
     * upper bound on the backoff so that if elections are failing due to a network partition that lasts for a long time then when the
     * partition heals there is an election attempt reasonably quickly.
     */

    // bounds on the time between election attempts
    private static final String ELECTION_MIN_TIMEOUT_SETTING_KEY = "cluster.election.min_timeout";
    private static final String ELECTION_BACK_OFF_TIME_SETTING_KEY = "cluster.election.back_off_time";
    private static final String ELECTION_MAX_TIMEOUT_SETTING_KEY = "cluster.election.max_timeout";

    public static final String REQUEST_PRE_VOTE_ACTION_NAME = "internal:cluster/request_pre_vote";

    public static final Setting<TimeValue> ELECTION_MIN_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_MIN_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), Property.NodeScope);

    public static final Setting<TimeValue> ELECTION_BACK_OFF_TIME_SETTING = Setting.timeSetting(ELECTION_BACK_OFF_TIME_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), Property.NodeScope);

    public static final Setting<TimeValue> ELECTION_MAX_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_MAX_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(200), Property.NodeScope);

    static String validationExceptionMessage(final String electionMinTimeout, final String electionMaxTimeout) {
        return new ParameterizedMessage(
            "Invalid election retry timeouts: [{}] is [{}] and [{}] is [{}], but [{}] should be at least 100ms longer than [{}]",
            ELECTION_MIN_TIMEOUT_SETTING_KEY, electionMinTimeout,
            ELECTION_MAX_TIMEOUT_SETTING_KEY, electionMaxTimeout,
            ELECTION_MAX_TIMEOUT_SETTING_KEY, ELECTION_MIN_TIMEOUT_SETTING_KEY).getFormattedMessage();
    }

    private final TimeValue minTimeout;
    private final TimeValue backoffTime;
    private final TimeValue maxTimeout;
    private final Random random;
    private final TransportService transportService;
    private final AtomicLong idSupplier = new AtomicLong();

    private volatile Object currentScheduler; // only care about its identity

    ElectionScheduler(Settings settings, Random random, TransportService transportService) {
        super(settings);

        this.random = random;
        this.transportService = transportService;

        minTimeout = ELECTION_MIN_TIMEOUT_SETTING.get(settings);
        backoffTime = ELECTION_BACK_OFF_TIME_SETTING.get(settings);
        maxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(settings);

        if (maxTimeout.millis() < minTimeout.millis() + 100) {
            throw new IllegalArgumentException(validationExceptionMessage(minTimeout.toString(), maxTimeout.toString()));
        }
    }

    public void start() {
        final ActiveScheduler currentScheduler;
        assert this.currentScheduler == null;
        this.currentScheduler = currentScheduler = new ActiveScheduler();
        currentScheduler.scheduleNextElection();
    }

    public void stop() {
        assert currentScheduler != null : currentScheduler;
        logger.debug("stopping {}", currentScheduler);
        currentScheduler = null;
    }

    @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
    private static long nonNegative(long n) {
        return n == Long.MIN_VALUE ? 0 : Math.abs(n);
    }

    /**
     * @param lowerBound inclusive lower bound
     * @param upperBound exclusive upper bound
     */
    private long randomLongBetween(long lowerBound, long upperBound) {
        assert 0 < upperBound - lowerBound;
        return nonNegative(random.nextLong()) % (upperBound - lowerBound) + lowerBound;
    }

    private long backOffCurrentDelay(long currentDelayMillis) {
        return Math.min(maxTimeout.getMillis(), currentDelayMillis + backoffTime.getMillis());
    }

    /**
     * Start an election. Calls to this method are not completely synchronised with the start/stop state of the scheduler.
     *
     * @param maxTermSeen The maximum term seen before this election starts. The election should pick a higher term.
     */
    protected abstract void startElection(long maxTermSeen);

    @Override
    public String toString() {
        return "ElectionScheduler{" +
            "minTimeout=" + minTimeout +
            ", maxTimeout=" + maxTimeout +
            ", backoffTime=" + backoffTime +
            ", currentScheduler=" + currentScheduler +
            '}';
    }

    protected abstract Iterable<DiscoveryNode> getBroadcastNodes();

    protected abstract boolean isElectionQuorum(VoteCollection voteCollection);

    protected abstract PreVoteResponse getLocalPreVoteResponse();

    private class ActiveScheduler {
        private AtomicLong currentDelayMillis = new AtomicLong(minTimeout.millis());
        private final long schedulerId = idSupplier.incrementAndGet();

        boolean isRunning() {
            return this == currentScheduler;
        }

        private void scheduleNextElection() {
            final long delay;
            if (isRunning() == false) {
                logger.debug("{} not scheduling election", this);
                return;
            }

            long backedOffDelay = this.currentDelayMillis.updateAndGet(ElectionScheduler.this::backOffCurrentDelay);
            delay = randomLongBetween(minTimeout.getMillis(), backedOffDelay + 1);
            logger.debug("{} scheduling election with delay [{}ms] (min={}, backoff={}, max={})",
                this, delay, minTimeout, backoffTime, maxTimeout);

            transportService.getThreadPool().schedule(TimeValue.timeValueMillis(delay), Names.GENERIC, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("unexpected exception in wakeup", e);
                    assert false : e;
                }

                @Override
                protected void doRun() {
                    if (isRunning() == false) {
                        logger.debug("{} not starting election", ActiveScheduler.this);
                        return;
                    }
                    long electionId = idSupplier.incrementAndGet();
                    logger.debug("{} starting pre-voting round {}", ActiveScheduler.this, electionId);
                    new PreVoteCollector(electionId).start();
                }

                @Override
                public void onAfter() {
                    scheduleNextElection();
                }

                @Override
                public String toString() {
                    return "scheduleNextElection[" + ActiveScheduler.this + "]";
                }

                @Override
                public boolean isForceExecution() {
                    // There are very few of these scheduled, and they back off, but it's important that they're not rejected as
                    // this could prevent a cluster from ever forming.
                    return true;
                }
            });
        }

        @Override
        public String toString() {
            return "ActiveScheduler[" + schedulerId + ", currentDelayMillis=" + currentDelayMillis + "]";
        }
    }

    private class PreVoteCollector {

        private final long electionId;

        private final Set<DiscoveryNode> preVotesReceived = newConcurrentSet();
        private final AtomicBoolean electionStarted = new AtomicBoolean();
        private final AtomicLong maxTermSeen;
        private final PreVoteResponse localPreVoteResponse;
        private final PreVoteRequest preVoteRequest;

        PreVoteCollector(long electionId) {
            this.electionId = electionId;
            localPreVoteResponse = getLocalPreVoteResponse();
            final long currentTerm = localPreVoteResponse.getCurrentTerm();
            preVoteRequest = new PreVoteRequest(transportService.getLocalNode(), currentTerm);
            maxTermSeen = new AtomicLong(currentTerm);
        }

        public void start() {
            logger.debug("{} starting", this);

            getBroadcastNodes().forEach(n -> transportService.sendRequest(n, REQUEST_PRE_VOTE_ACTION_NAME, preVoteRequest,
                new TransportResponseHandler<PreVoteResponse>() {
                    @Override
                    public void handleResponse(PreVoteResponse response) {
                        handlePreVoteResponse(response, n);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (exp.getRootCause() instanceof CoordinationStateRejectedException) {
                            logger.debug("{} failed: {}", this, exp.getRootCause().getMessage());
                        } else {
                            logger.debug(new ParameterizedMessage("{} failed", this), exp);
                        }
                    }

                    @Override
                    public String executor() {
                        return Names.GENERIC;
                    }

                    @Override
                    public String toString() {
                        return "TransportResponseHandler{" + PreVoteCollector.this + ", node=" + n + '}';
                    }
                }));
        }

        private void handlePreVoteResponse(PreVoteResponse response, DiscoveryNode sender) {
            final long currentId = idSupplier.get();
            if (currentId != electionId) {
                logger.debug("{} ignoring {} from {} as current id is now {}", this, response, sender, currentId);
                return;
            }

            final long currentMaxTermSeen = maxTermSeen.accumulateAndGet(response.getCurrentTerm(), Math::max);

            if (response.getLastAcceptedTerm() > localPreVoteResponse.getLastAcceptedTerm()
                || (response.getLastAcceptedTerm() == localPreVoteResponse.getLastAcceptedTerm()
                && response.getLastAcceptedVersion() > localPreVoteResponse.getLastAcceptedVersion())) {
                logger.debug("{} ignoring {} from {} as it is fresher", this, response);
                return;
            }

            preVotesReceived.add(sender);
            final VoteCollection voteCollection = new VoteCollection();
            preVotesReceived.forEach(voteCollection::addVote);

            if (isElectionQuorum(voteCollection) == false) {
                logger.debug("{} added {} from {}, no quorum yet", this, response, sender);
                return;
            }

            if (electionStarted.compareAndSet(false, true) == false) {
                logger.debug("{} added {} from {} but election has already started", this, response, sender);
                return;
            }

            logger.debug("{} added {} from {}, starting election in term > {}", this, response, sender, currentMaxTermSeen);
            startElection(currentMaxTermSeen);
        }

        @Override
        public String toString() {
            return "PreVoteCollector{" +
                "electionId=" + electionId +
                ", localPreVoteResponse=" + localPreVoteResponse +
                '}';
        }
    }
}
