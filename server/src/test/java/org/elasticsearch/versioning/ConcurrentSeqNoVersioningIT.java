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
package org.elasticsearch.versioning;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.AbstractDisruptionTestCase;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 4, maxNumDataNodes = 6,
    transportClientRatio = 0)
@TestLogging("_root:DEBUG,org.elasticsearch.action.bulk:TRACE,org.elasticsearch.action.get:TRACE," +
    "org.elasticsearch.discovery:TRACE,org.elasticsearch.action.support.replication:TRACE," +
    "org.elasticsearch.cluster.service:TRACE,org.elasticsearch.indices.recovery:TRACE," +
    "org.elasticsearch.indices.cluster:TRACE,org.elasticsearch.index.shard:TRACE")
public class ConcurrentSeqNoVersioningIT extends AbstractDisruptionTestCase {

    private static final Pattern EXTRACT_VERSION = Pattern.compile("current document has seqNo \\[(\\d+)\\] and primary term \\[(\\d+)\\]");

    // Test info: disrupt network for up to 8s in a number of rounds and check that we only get true positive CAS results when running
    // multiple threads doing CAS updates.
    // Wait up to 1 minute (+10s in thread to ensure it does not time out) for threads to complete previous round before initiating next
    // round.
    // Following issues are accepted for now:
    // 1. Under certain circumstances (network partitions and other failures) we can end up giving a false negative response, ie. report
    // back that a CAS failed due to version conflict even though it actually succeeded.
    // 2. If we end up reporting back any other failure, it is unknown if the write succeeded or not (or will succeed in the future).
    // 3. If you read data out, you may see dirty writes, ie. writes that will end up being discarded due to node failure/network
    // disruption.
    // 4. Likewise, the CAS check can be done against a dirty write and thus fail even if it ought to succeed.
    // 5. A CAS check can be done against a stale primary and fail due to that.
    // 6. A CAS failure does not give back the current seqno/primary-term to use for the next request (and if it did, it could be wrong
    // due to a dirty write anyway).
    public void testSeqNoCASLinearizability() {
        // 1-8 seconds, bias towards shorter.
        final int disruptTimeSeconds = 1 << randomInt(3);

        assertAcked(prepareCreate("test")
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(3))
            ));

        ensureGreen();


        int numberOfKeys = randomIntBetween(1,10);

        logger.info("--> Indexing initial doc for {} keys", numberOfKeys);
        List<Partition> partitions =
            IntStream.range(0, numberOfKeys)
                .mapToObj(i -> client().prepareIndex("test", "type", "ID:" + i).setSource("value", -1).get())
                .map(response ->
                    new Partition(response.getId(), new Version(response.getPrimaryTerm(), response.getSeqNo())))
                .collect(Collectors.toList());

        int threadCount = randomIntBetween(3, 20);
        CyclicBarrier roundBarrier = new CyclicBarrier(threadCount + 1); // +1 for main thread.

        List<CASUpdateThread> threads =
            IntStream.range(0, threadCount)
                .mapToObj(i -> new CASUpdateThread(i, roundBarrier, partitions, disruptTimeSeconds+1))
                .collect(Collectors.toList());

        logger.info("--> Starting {} threads", threadCount);
        threads.forEach(Thread::start);

        try {
            int rounds = randomIntBetween(2, 5);

            logger.info("--> Running {} rounds", rounds);

            for (int i = 0; i < rounds; ++i) {
                ServiceDisruptionScheme disruptionScheme = addRandomDisruptionScheme();
                roundBarrier.await(1, TimeUnit.MINUTES);
                disruptionScheme.startDisrupting();
                logger.info("--> round {}", i);
                try {
                    roundBarrier.await(disruptTimeSeconds, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    roundBarrier.reset();
                }
                internalCluster().clearDisruptionScheme(false);
                // heal cluster faster to reduce test time.
                ensureFullyConnectedCluster();
            }
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            logger.error("Timed out, dumping stack traces of all threads:");
            threads.forEach(
                thread -> logger.info(thread.toString() + ":\n" + ExceptionsHelper.formatStackTrace(thread.getStackTrace())));
            throw new RuntimeException(e);
        } finally {
            logger.info("--> terminating test");
            threads.forEach(CASUpdateThread::terminate);
            threads.forEach(CASUpdateThread::await);
            threads.stream().filter(Thread::isAlive).forEach(t -> fail("Thread still alive: " + t));
        }

        partitions.forEach(Partition::assertLinearizable);
    }


    private class CASUpdateThread extends Thread {
        private final CyclicBarrier roundBarrier;
        private final List<Partition> partitions;
        private final int timeout;

        private volatile boolean stop;
        private final Random random = new Random(randomLong());

        private CASUpdateThread(int threadNum, CyclicBarrier roundBarrier, List<Partition> partitions, int timeout) {
            super("CAS-Update-" + threadNum);
            this.roundBarrier = roundBarrier;
            this.partitions = partitions;
            this.timeout = timeout;
            setDaemon(true);
        }

        public void run() {
            while (stop == false) {
                try {
                    roundBarrier.await(70, TimeUnit.SECONDS);

                    int numberOfUpdates = randomIntBetween(3*partitions.size(), 13*partitions.size());
                    for (int i = 0; i < numberOfUpdates; ++i) {
                        final int keyIndex = random.nextInt(partitions.size());
                        final Partition partition = partitions.get(keyIndex);

                        final int seqNoChangePct = random.nextInt(100);
                        final boolean futureSeqNo = seqNoChangePct < 10;
                        Version version = partition.latestKnownVersion();
                        if (futureSeqNo) {
                            version = version.nextSeqNo(random.nextInt(4)+1);
                        } else if (seqNoChangePct < 15) {
                            version = version.previousSeqNo(random.nextInt(4)+1);
                        }

                        final int termChangePct = random.nextInt(100);
                        final boolean futureTerm = termChangePct < 5;
                        if (futureTerm) {
                            version = version.nextTerm();
                        } else if (termChangePct < 10) {
                            version = version.previousTerm();
                        }

                        Consumer<HistoryOutput> historyResponse = partition.invoke(version);
                        try {
                            // we should be able to remove timeout or fail hard on timeouts if we fix network disruptions to be
                            // realistic, ie. not silently throw data out.
                            IndexResponse indexResponse = client().prepareIndex("test", "type", partition.id)
                                .setSource("value", random.nextInt())
                                .setIfPrimaryTerm(version.primaryTerm)
                                .setIfSeqNo(version.seqNo)
                                .execute().actionGet(timeout, TimeUnit.SECONDS);
                            IndexResponseHistoryOutput historyOutput = new IndexResponseHistoryOutput(indexResponse);
                            historyResponse.accept(historyOutput);
                            // validate version and seqNo strictly increasing for successful CAS to avoid that overhead during
                            // linearizability checking.
                            assertThat(historyOutput.outputVersion, greaterThan(version));
                            assertThat(historyOutput.outputVersion.seqNo, greaterThan(version.seqNo));
                        } catch (VersionConflictEngineException e) {
                            historyResponse.accept(new CASFailureHistoryOutput(e));
                        } catch (RuntimeException e) {
                            // if we used a future seqNo, we cannot know if it will overwrite a future update when failing with
                            // unknown error
                            if (futureSeqNo == false && futureTerm == false)
                                historyResponse.accept(new FailureHistoryOutput());
                        }
                    }

                } catch (InterruptedException e) {
                    assert stop : "should only be interrupted when stopped";
                } catch (BrokenBarrierException e) {
                    // a thread can go here either because it completed before disruption ended, timeout on main thread causes broken
                    // barrier
                } catch (TimeoutException e) {
                    // this is timeout on the barrier, unexpected.
                    throw new AssertionError("Unexpected timeout in thread: " + Thread.currentThread(), e);
                }
            }
        }

        public void terminate() {
            stop = true;
            this.interrupt();
        }

        public void await() {
            try {
                join(60000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class AtomicVersion {
        private final AtomicReference<Version> current;

        private AtomicVersion(Version initialVersion) {
            this.current = new AtomicReference<>(initialVersion);
        }

        public Version get() {
            return current.get();
        }

        public void consume(Version version) {
            if (version == null)
                return;
            this.current.updateAndGet(current -> version.compareTo(current) <= 0 ? current : version);
        }
    }

    private class Partition {
        private final String id;
        private final AtomicVersion atomicVersion;
        private final Version initialVersion;
        private final LinearizabilityChecker.History history = new LinearizabilityChecker.History();

        private Partition(String id, Version initialVersion) {
            this.id = id;
            this.atomicVersion = new AtomicVersion(initialVersion);
            this.initialVersion = initialVersion;
        }

        public Version latestKnownVersion() {
            return atomicVersion.get();
        }

        public Consumer<HistoryOutput> invoke(Version version) {
            int eventId = history.invoke(version);
            logger.debug("invocation partition ({}) event ({}) version ({})", id, eventId, version);
            return output -> consumeOutput(output, eventId);
        }

        private void consumeOutput(HistoryOutput output, int eventId) {
            history.respond(eventId, output);
            logger.debug("response partition ({}) event ({}) output ({})", id, eventId, output);
            // we try to use the highest seen version for the next request. We could think that this could lead to one dirty read that
            // causes us to stick to errors for the rest of the run. But if we have a dirty read/CAS failure, it must be on an old primary
            // and the new primary will have a larger primaryTerm and a subsequent CAS failure will ensure we notice the new primaryTerm
            // and seqNo
            atomicVersion.consume(output.getVersion());
        }

        public boolean isLinearizable() {
            logger.info("--> Linearizability checking history of size: {} for key: {} and initialVersion: {}: {}", history.size(),
                id, initialVersion, history);
            return isLinearizable(new CASSequentialSpec(initialVersion))
                 & isLinearizable(new CASSimpleSequentialSpec(initialVersion));
        }

        private boolean isLinearizable(LinearizabilityChecker.SequentialSpec spec) {
            boolean linearizable =
                new LinearizabilityChecker().isLinearizable(spec, history,
                    missingResponseGenerator());
            if (linearizable == false) {
                // we dump base64 encoded data, since the nature of this test is that it does not reproduce even with same seed.
                logger.error("Linearizability check failed. Spec: {}, initial version: {}, serialized history: {}", spec, initialVersion,
                    base64Serialize(history));
            }
            return linearizable;
        }

        public void assertLinearizable() {
            assertTrue("Must be linearizable", isLinearizable());
        }

    }

    private static class CASSequentialSpec implements LinearizabilityChecker.SequentialSpec {

        private final Version initialVersion;

        private CASSequentialSpec(Version initialVersion) {
            this.initialVersion = initialVersion;
        }

        @Override
        public Object initialState() {
            return new SuccessState(initialVersion);
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            return ((HistoryOutput) output).nextState((State) currentState, (Version) input).map(i -> i); // Optional<?> to Optional<Object>
        }
    }


    private static class CASSimpleSequentialSpec implements LinearizabilityChecker.SequentialSpec {
        private final Version initialVersion;

        private CASSimpleSequentialSpec(Version initialVersion) {
            this.initialVersion = initialVersion;
        }

        @Override
        public Object initialState() {
            return new SimpleState(initialVersion, false);
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            SimpleState state = (SimpleState) currentState;
            if (output instanceof IndexResponseHistoryOutput) {
                if (input.equals(state.safeVersion) ||
                    (state.lastFailed && ((Version) input).compareTo(state.safeVersion) > 0)) {
                    return Optional.of(new SimpleState(((IndexResponseHistoryOutput) output).getVersion(), false));
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.of(state.failed());
            }
        }
    }

    private static class SimpleState {
        private final Version safeVersion;
        private final boolean lastFailed;

        private SimpleState(Version safeVersion, boolean lastFailed) {
            this.safeVersion = safeVersion;
            this.lastFailed = lastFailed;
        }


        public SimpleState failed() {
            return lastFailed ? this : new SimpleState(safeVersion, true);
        }
    }

    /**
     * Our version, which is primaryTerm,seqNo.
     */
    private static final class Version implements NamedWriteable, Comparable<Version> {
        public final long primaryTerm;
        public final long seqNo;

        Version(long primaryTerm, long seqNo) {
            this.primaryTerm = primaryTerm;
            this.seqNo = seqNo;
        }

        Version(StreamInput input) throws IOException {
            this.primaryTerm = input.readLong();
            this.seqNo = input.readLong();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Version version = (Version) o;
            return primaryTerm == version.primaryTerm &&
                seqNo == version.seqNo;
        }

        @Override
        public int hashCode() {
            return Objects.hash(primaryTerm, seqNo);
        }

        @Override
        public int compareTo(Version other) {
            int termCompare = Long.compare(primaryTerm, other.primaryTerm);
            if (termCompare != 0)
                return termCompare;
            return Long.compare(seqNo, other.seqNo);

        }

        @Override
        public String toString() {
            return "{" + "primaryTerm=" + primaryTerm + ", seqNo=" + seqNo + '}';
        }

        public Version nextSeqNo(int increment) {
            return new Version(primaryTerm, seqNo + increment);
        }

        public Version previousSeqNo(int decrement) {
            return new Version(primaryTerm, Math.max(seqNo - decrement, 0));
        }

        @Override
        public String getWriteableName() {
            return "version";
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(primaryTerm);
            out.writeLong(seqNo);
        }

        public Version previousTerm() {
            return new Version(primaryTerm-1, seqNo);
        }

        public Version nextTerm() {
            return new Version(primaryTerm+1, seqNo);
        }
    }

    /**
     * State and its subclasses model the state of the partition (key) for the linearizability checker.
     *
     * We go a step deeper than just modelling successes, since we can then do more validations.
     */
    private abstract static class State {
        /**
         * The last known successfully replicated version. Any newer version must be larger than this.
         */
        public final Version safeVersion;

        private State(Version safeVersion) {
            this.safeVersion = safeVersion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            State state = (State) o;
            return safeVersion.equals(state.safeVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(safeVersion);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{version=" + safeVersion + "}";
        }

        abstract Optional<State> casSuccess(Version inputVersion, Version outputVersion);

        // We can get CAS failures in following situations:
        // 1. Real: a concurrent or previous write updated this.
        // 2. Fail on our own write: the write to a replica was already done, but not responded to primary (or multiple replicas). The
        // replica is promoted to primary. The write on the original primary is bubbled up to Reroute phase and retried. The retry will
        // fail on its own update on the new primary.
        // 3. Fail after other fail: another write CAS failed, but did do the write anyway (not dirty). We then CAS fail only due to
        // that write.
        // 4. Fail on dirty write: the primary we talk to is no longer the real primary. CAS failures would thus occur against dirty
        // writes (but those must still be concurrent with us, we do guarantee to only respond sucessfully to non-dirty writes) or
        // stale date
        abstract Optional<State> casFail(Version inputVersion, Version outputVersion);

        Optional<State> fail() {
            return Optional.of(new FailState(safeVersion));
        }
    }

    private static class SuccessState extends State {
        private SuccessState(Version version) {
            super(version);
        }

        @Override
        Optional<State> casSuccess(Version inputVersion, Version outputVersion) {
            if (safeVersion.equals(inputVersion)) {
                return Optional.of(new SuccessState(outputVersion));
            } else {
                return Optional.empty();
            }
        }

        Optional<State> casFail(Version inputVersion, Version outputVersion) {
            if (inputVersion.equals(safeVersion) == false && safeVersion.equals(outputVersion)) {
                return Optional.of(this); // since version is unchanged, we know CAS did not write anything and thus state is unchanged.
            }

            if (outputVersion.primaryTerm < safeVersion.primaryTerm) {
                // A stale read against an old primary, but we know then that the safe state did not change, ie. this write should be
                // ignored. It cannot have made surviving interim writes, since if so, the final primaryTerm should be greater than or
                // equal to safeVersion.primaryTerm
                // So the last successful write is still the right representation for the state.
                return Optional.of(new SuccessState(safeVersion));
            } else { //outputVersion.primaryTerm >= safeVersion.primaryTerm
                // failed on own write (or regular CAS failure, cannot see the difference, but we assume own write for linearizability).
                return Optional.of(new CASFailOwnWriteState(safeVersion, outputVersion));
            }
        }

    }

    /**
     * Any CAS Failure that fails only due to its own write brings us here.
     *
     * Additionally following CAS failures on top of this state stays in this state, see #casFail method.
     */
    private static class CASFailOwnWriteState extends State {
        private Version casFailVersion;

        private CASFailOwnWriteState(Version safeVersion, Version casFailVersion) {
            super(safeVersion);
            this.casFailVersion = casFailVersion;
            assert this.casFailVersion.primaryTerm >= safeVersion.primaryTerm;
        }

        @Override
        Optional<State> casSuccess(Version inputVersion, Version outputVersion) {
            if (inputVersion.compareTo(safeVersion) >= 0 && inputVersion.compareTo(casFailVersion) <= 0) {
                // A CAS fail on own write can happen in two main scenarios:
                // 1. The primary replicates to one replica R1 but fails to replicate to other replica. It is then demoted and R1 is
                // promoted to primary
                // 2. The coordinating node looses connection to primary. Coordinating node will do a retry after connection is
                // reestablished (against same or new primary).

                // Both situations lead to interim writes that are never returned to the client. When we guess seqnos, we risk hitting
                // those.

                // Interim write explanation for scenario 1:
                // Suppose last update was (t=1,s=0) on n1.
                // We successfully write (t=1,s=1) on n1. During replication, we find that n2 is primary and successfully write (t=2,
                // s=2) on n2. Again during replication, we find that n3 is primary and then CAS fail on n3 against our
                // own write (t=2, s=2).
                // The ghost write (t=1, s=1) was never seen neither as success nor CAS fail write and we therefore have to accept that
                // CAS can succeed against any version where safeVersion <= version < (casFailVersion.primaryTerm,0)

                // Interim write explanation for scenario 2:
                // Suppose last update was (t=1, s=0) on n1.
                // A write is sent to coordinator c1 which sends request to n1. n1 successfully writes (t=1, s=1). Before responding,
                // connection is broken. c1 notices broken link and schedules a retry.
                // New write goes directly to n1. It made up the input seqno 1. This write succeeds with output-version (t=1, s=2).
                // Connection is reestablished between c1 and n1 and the retry goes to n1. The retry fails, output-version (t=1, s=2).
                // Notice that we never saw any success or cas fail with output version (t=1, s=1) even though we successfully wrote it.

                // Based on above, the only assertion we can make here is that the input-version must be between previous safeVersion
                // and last casFailVersion.

                return Optional.of(new SuccessState(outputVersion));
                // todo: add more advanced network disruptions with floating partitioning to provoke above in more cases.
            }

            return Optional.empty();
        }

        @Override
        Optional<State> casFail(Version inputVersion, Version outputVersion) {
            // we can fail against one of:
            // 1. our own write (outputVersion > casFailVersion)
            // 2. any of the previous interim/ghost writes (safeVersion < outputVersion <= casFailVersion
            // .primaryTerm) (see comment above for info on interim/ghost writes).
            // 3. last success write. (safeVersion == outputVersion). This is either a regular fail or a stale read failure, we cannot
            // tell since we do not know which shard ends up winning.
            // 4. A plain stale read (safeVersion.primaryTerm > outputVersion.primaryTerm)

            // for 1-3, we have to continue being in CASFailOwnWriteState
            // of this failure or a previous failure in a future response.
            // We use the max cas fail version, which is correct due to the checking in casSuccess.
            // for 4, we ignore the stale read (does not affect state).

            Version newCASFailVersion = outputVersion.compareTo(casFailVersion) > 0 ? outputVersion : casFailVersion;
            return Optional.of(new CASFailOwnWriteState(safeVersion, newCASFailVersion));
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{version=" + safeVersion + ", failVersion=" + casFailVersion + "}";
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o) && casFailVersion.equals(((CASFailOwnWriteState) o).casFailVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), casFailVersion);
        }
    }


    /**
     * We move to this state when a (non-CAS) failure occurs.
     *
     * We then generally know very little, except that the safeVersion is a minimum version.
     *
     * Notice that since we only do CAS updates, we can sensibly handle this. If another CAS succeeds after this, we know that the
     * previous CAS will not be able to succeed. Beware of the "next seqNo" handling in CASUpdateThread, which is necessary to be able to
     * handle failed operations correctly here.
     */
    private static class FailState extends State {

        private FailState(Version version) {
            super(version);
        }

        @Override
        Optional<State> casSuccess(Version inputVersion, Version outputVersion) {
            if (safeVersion.compareTo(inputVersion) > 0 || safeVersion.compareTo(outputVersion) > 0)
                return Optional.empty();

            // the previous write could have been accepted or rejected so we cannot validate the version more precisely.
            // but we know that any previous writes cannot succeed, since they all use seqNo <= inputVersion.seqNo.
            return Optional.of(new SuccessState(outputVersion));
        }

        @Override
        Optional<State> casFail(Version inputVersion, Version outputVersion) {
            // we still do not know if the failed write could sneak in so have to stay in FailState.
            return Optional.of(this);
        }

        @Override
        Optional<State> fail() {
            return Optional.of(this);
        }
    }


    /**
     * HistoryOutput serves both as the output of calls and delegating the sequential spec to the right State methods.
     */
    private interface HistoryOutput extends NamedWriteable {
        Optional<State> nextState(State currentState, Version input);

        Version getVersion();
    }

    private static class IndexResponseHistoryOutput implements HistoryOutput {
        private final Version outputVersion;

        private IndexResponseHistoryOutput(IndexResponse response) {
            this.outputVersion = new Version(response.getPrimaryTerm(), response.getSeqNo());
        }

        private IndexResponseHistoryOutput(StreamInput input) throws IOException {
            this.outputVersion = new Version(input);
        }

        @Override
        public Optional<State> nextState(State currentState, Version input) {
            return currentState.casSuccess(input, outputVersion);
        }

        @Override
        public Version getVersion() {
            return outputVersion;
        }

        @Override
        public String toString() {
            return "Index{" + outputVersion + "}";
        }

        @Override
        public String getWriteableName() {
            return "index";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            outputVersion.writeTo(out);
        }
    }

    private static class CASFailureHistoryOutput implements HistoryOutput {
        private Version outputVersion;
        private CASFailureHistoryOutput(VersionConflictEngineException exception) {
            this.outputVersion = parseException(exception.getMessage());
        }

        private CASFailureHistoryOutput(StreamInput input) throws IOException {
            this.outputVersion = new Version(input);
        }

        private Version parseException(String message) {
            // ugly, but having these observed versions available improves the linearizability checking. Additionally, this ensures
            // progress since if we did not parse this out, no writes would succeed after a fail on own write failure (unless we were
            // lucky enough to guess the seqNo/primaryTerm with the random futureTerm/futureSeqNo handling in CASUpdateThread).
            try {
                Matcher matcher = EXTRACT_VERSION.matcher(message);
                matcher.find();
                return new Version(Long.parseLong(matcher.group(2)), Long.parseLong(matcher.group(1)));
            } catch (RuntimeException e) {
                throw new RuntimeException("Unable to parse message: " + message, e);
            }
        }

        @Override
        public Optional<State> nextState(State currentState, Version input) {
            return currentState.casFail(input, outputVersion);
        }

        @Override
        public Version getVersion() {
            return outputVersion;
        }

        @Override
        public String toString() {
            return "CASFail{" + outputVersion + "}";
        }

        @Override
        public String getWriteableName() {
            return "casfail";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            outputVersion.writeTo(out);
        }
    }

    private static class FailureHistoryOutput implements HistoryOutput {

        private FailureHistoryOutput() {

        }
        private FailureHistoryOutput(@SuppressWarnings("unused") StreamInput streamInput) {

        }

        @Override
        public Optional<State> nextState(State currentState, Version input) {
            return currentState.fail();
        }

        @Override
        public Version getVersion() {
            return null;
        }

        @Override
        public String toString() {
            return "Fail";
        }

        @Override
        public String getWriteableName() {
            return "fail";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // nothing to write.
        }
    }

    private static Function<Object, Object> missingResponseGenerator() {
        return input -> new FailureHistoryOutput();
    }

    @SuppressForbidden(reason = "we only serialize data in the test case to be able to deserialize it for debugging")
    private String base64Serialize(LinearizabilityChecker.History history) {
        BytesStreamOutput output = new BytesStreamOutput();
        try {
            List<LinearizabilityChecker.Event> events = history.copyEvents();
            output.writeInt(events.size());
            for (LinearizabilityChecker.Event event : events) {
                writeEvent(event, output);
            }
            output.close();
            return Base64.getEncoder().encodeToString(BytesReference.toBytes(output.bytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassCastException e) {
            return "Unable to produce base64 serialized version of history: " + history +"\n  Message: " + e.getMessage();
        }
    }

    private static LinearizabilityChecker.History readHistory(StreamInput input) throws IOException {
        int size = input.readInt();
        List<LinearizabilityChecker.Event> events = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            events.add(readEvent(input));
        }
        return new LinearizabilityChecker.History(events);
    }

    private static void writeEvent(LinearizabilityChecker.Event event, BytesStreamOutput output) throws IOException {
        output.writeEnum(event.type);
        output.writeNamedWriteable((NamedWriteable) event.value);
        output.writeInt(event.id);
    }

    private static LinearizabilityChecker.Event readEvent(StreamInput input) throws IOException {
        return new LinearizabilityChecker.Event(input.readEnum(LinearizabilityChecker.EventType.class),
            input.readNamedWriteable(NamedWriteable.class), input.readInt());
    }

    @SuppressForbidden(reason = "system err is ok for a command line tool")
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("usage: <file> <primaryTerm> <seqNo>");
        } else {
            try {
                runLinearizabilityChecker(new FileInputStream(args[0]), Long.parseLong(args[1]), Long.parseLong(args[2]));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressForbidden(reason = "system out is ok for a command line tool and deserialize is also ok in this debugging tool")
    private static void runLinearizabilityChecker(FileInputStream fileInputStream, long primaryTerm, long seqNo) throws IOException {
        StreamInput is = new InputStreamStreamInput(Base64.getDecoder().wrap(fileInputStream));
        is = new NamedWriteableAwareStreamInput(is, createNamedWriteableRegistry());

        LinearizabilityChecker.History history = readHistory(is);

        Version initialVersion = new Version(primaryTerm, seqNo);
        boolean result =
            new LinearizabilityChecker().isLinearizable(new CASSequentialSpec(initialVersion), history,
                missingResponseGenerator());

        System.out.println(new LinearizabilityChecker().visualize(new CASSequentialSpec(initialVersion), history,
            missingResponseGenerator()));

        System.out.println("Linearizable?: " + result);
    }

    private static NamedWriteableRegistry createNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays.asList(
            new NamedWriteableRegistry.Entry(NamedWriteable.class, "version", Version::new),
            new NamedWriteableRegistry.Entry(NamedWriteable.class, "index", IndexResponseHistoryOutput::new),
            new NamedWriteableRegistry.Entry(NamedWriteable.class, "casfail", CASFailureHistoryOutput::new),
            new NamedWriteableRegistry.Entry(NamedWriteable.class, "fail", FailureHistoryOutput::new)
        ));
    }

}

