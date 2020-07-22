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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;


/**
 * This test stress tests CAS updates using sequence number based versioning (ifPrimaryTerm/ifSeqNo).
 *
 * <p>The following is a summary of the expected CAS write behaviour of the system:</p>
 *
 * <ul>
 *     <li>acknowledged CAS writes are guaranteed to have taken place between invocation and response and cannot be lost. It is
 *     guaranteed that the previous value had the specified primaryTerm and seqNo</li>
 *     <li>CAS writes resulting in a VersionConflictEngineException might or might not have taken place or may take place in the future
 *     provided the primaryTerm and seqNo still matches. The reason we cannot assume it will not take place after receiving the failure
 *     is that a request can fork into two because of retries on disconnect, and now race against itself. The retry might complete (and do a
 *     dirty or stale read) before the forked off request gets to execute, and that one might still subsequently succeed.
 *
 *     Such writes are not necessarily fully replicated and can be lost. There is no
 *     guarantee that the previous value did not have the specified primaryTerm and seqNo</li>
 *     <li>CAS writes with other exceptions might or might not have taken place. If they have taken place, then after invocation but not
 *     necessarily before response. Such writes are not necessarily fully replicated and can be lost.
 *     </li>
 * </ul>
 *
 * A deeper technical explanation of the behaviour is given here:
 *
 * <ul>
 *     <li>A CAS can fail on its own write in at least two ways. In both cases, the write might have taken place even though we get a
 *     version conflict response. Even though we might observe the write (by reading (not done in this test) or another CAS write), the
 *     write could be lost since it is not fully replicated. Details:
 *     <ul>
 *         <li>A write is successfully stored on primary and one replica (r1). Replication to second replica fails, primary is demoted
 *         and r1 is promoted to primary. The request is repeated on r1, but this time the request fails due to its own write.</li>
 *         <li>A coordinator sends write to primary, which stores write successfully (and replicates it). Connection is lost before
 *         response is sent back. Once connection is back, coordinator will retry against either same or new primary, but this time the
 *         request will fail due to its own write.
 *         </li>
 *     </ul>
 *     </li>
 *     <li>A CAS can fail on stale reads. A CAS failure is only checked on the supposedly primary node. However, the primary might not be
 *     the newest primary (could be isolated or just not have been told yet). So a CAS check is suspect to stale reads (like any
 *     read) and can thus fail due to reading stale data. Notice that a CAS success is fully replicated and thus guaranteed to not
 *     suffer from stale (or dirty) reads.
 *     </li>
 *     <li>A CAS can fail on a dirty read, i.e., a non-replicated write that ends up being discarded.</li>
 *     <li>For any other failure, we do not know if the write will succeed after the failure. However, we do know that if we
 *     subsequently get back a CAS success with seqNo s, any previous failures with ifSeqNo &lt; s will not be able to succeed (but could
 *     produce dirty writes on a stale primary).
 *     </li>
 *     <li>A CAS failure or any other failure can eventually succeed after receiving the failure response due to reroute and retries,
 *     see above.</li>
 *     <li>A CAS failure throws a VersionConflictEngineException which does not directly contain the current seqno/primary-term to use for
 *     the next request. It is contained in the message (and we parse it out in the test), but notice that the numbers given here could be
 *     stale or dirty, i.e., come from a stale primary or belong to a write that ends up being discarded.</li>
 * </ul>
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 4, maxNumDataNodes = 6)
public class ConcurrentSeqNoVersioningIT extends AbstractDisruptionTestCase {

    private static final Pattern EXTRACT_VERSION = Pattern.compile("current document has seqNo \\[(\\d+)\\] and primary term \\[(\\d+)\\]");

    // Test info: disrupt network for up to 8s in a number of rounds and check that we only get true positive CAS results when running
    // multiple threads doing CAS updates.
    // Wait up to 1 minute (+10s in thread to ensure it does not time out) for threads to complete previous round before initiating next
    // round.
    public void testSeqNoCASLinearizability() {
        final int disruptTimeSeconds = scaledRandomIntBetween(1, 8);

        assertAcked(prepareCreate("test")
            .setSettings(Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2))
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(3))
            ));

        ensureGreen();

        int numberOfKeys = randomIntBetween(1, 10);

        logger.info("--> Indexing initial doc for {} keys", numberOfKeys);
        List<Partition> partitions =
            IntStream.range(0, numberOfKeys)
                .mapToObj(i -> client().prepareIndex("test").setId("ID:" + i).setSource("value", -1).get())
                .map(response ->
                    new Partition(response.getId(), new Version(response.getPrimaryTerm(), response.getSeqNo())))
                .collect(Collectors.toList());

        int threadCount = randomIntBetween(3, 20);
        CyclicBarrier roundBarrier = new CyclicBarrier(threadCount + 1); // +1 for main thread.

        List<CASUpdateThread> threads =
            IntStream.range(0, threadCount)
                .mapToObj(i -> new CASUpdateThread(i, roundBarrier, partitions, disruptTimeSeconds + 1))
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

                    int numberOfUpdates = randomIntBetween(3, 13)  * partitions.size();
                    for (int i = 0; i < numberOfUpdates; ++i) {
                        final int keyIndex = random.nextInt(partitions.size());
                        final Partition partition = partitions.get(keyIndex);

                        final int seqNoChangePct = random.nextInt(100);

                        // we use either the latest observed or the latest successful version, to increase chance of getting successful
                        // CAS'es and races. If we were to use only the latest successful version, any CAS fail on own write would mean that
                        // all future CAS'es would fail unless we guess the seqno/term below. On the other hand, using latest observed
                        // version exclusively we risk a single CAS fail on a dirty read to cause the same. Doing both randomly and adding
                        // variance to seqno/term should ensure we progress fine in most runs.
                        Version version = random.nextBoolean() ? partition.latestObservedVersion() : partition.latestSuccessfulVersion();

                        if (seqNoChangePct < 10) {
                            version = version.nextSeqNo(random.nextInt(4) + 1);
                        } else if (seqNoChangePct < 15) {
                            version = version.previousSeqNo(random.nextInt(4) + 1);
                        }

                        final int termChangePct = random.nextInt(100);
                        if (termChangePct < 5) {
                            version = version.nextTerm();
                        } else if (termChangePct < 10) {
                            version = version.previousTerm();
                        }

                        IndexRequest indexRequest = new IndexRequest("test").id(partition.id)
                            .source("value", random.nextInt())
                            .setIfPrimaryTerm(version.primaryTerm)
                            .setIfSeqNo(version.seqNo);
                        Consumer<HistoryOutput> historyResponse = partition.invoke(version);
                        try {
                            // we should be able to remove timeout or fail hard on timeouts
                            IndexResponse indexResponse = client().index(indexRequest).actionGet(timeout, TimeUnit.SECONDS);
                            IndexResponseHistoryOutput historyOutput = new IndexResponseHistoryOutput(indexResponse);
                            historyResponse.accept(historyOutput);
                            // validate version and seqNo strictly increasing for successful CAS to avoid that overhead during
                            // linearizability checking.
                            assertThat(historyOutput.outputVersion, greaterThan(version));
                            assertThat(historyOutput.outputVersion.seqNo, greaterThan(version.seqNo));
                        } catch (VersionConflictEngineException e) {
                            // if we supplied an input version <= latest successful version, we can safely assume that any failed
                            // operation will no longer be able to complete after the next successful write and we can therefore terminate
                            // the operation wrt. linearizability.
                            // todo: collect the failed responses and terminate when CAS with higher output version is successful, since
                            // this is the guarantee we offer.
                            if (version.compareTo(partition.latestSuccessfulVersion()) <= 0) {
                                historyResponse.accept(new CASFailureHistoryOutput(e));
                            }
                        } catch (RuntimeException e) {
                            // if we supplied an input version <= to latest successful version, we can safely assume that any failed
                            // operation will no longer be able to complete after the next successful write and we can therefore terminate
                            // the operation wrt. linearizability.
                            // todo: collect the failed responses and terminate when CAS with higher output version is successful, since
                            // this is the guarantee we offer.
                            if (version.compareTo(partition.latestSuccessfulVersion()) <= 0) {
                                historyResponse.accept(new FailureHistoryOutput());
                            }
                            logger.info(
                                new ParameterizedMessage("Received failure for request [{}], version [{}]", indexRequest, version),
                                e);
                            if (stop) {
                                // interrupt often comes as a RuntimeException so check to stop here too.
                                return;
                            }
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
            return new Version(primaryTerm - 1, seqNo);
        }

        public Version nextTerm() {
            return new Version(primaryTerm + 1, seqNo);
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
        private final AtomicVersion latestSuccessfulVersion;
        private final AtomicVersion latestObservedVersion;
        private final Version initialVersion;
        private final LinearizabilityChecker.History history = new LinearizabilityChecker.History();

        private Partition(String id, Version initialVersion) {
            this.id = id;
            this.latestSuccessfulVersion = new AtomicVersion(initialVersion);
            this.latestObservedVersion = new AtomicVersion(initialVersion);
            this.initialVersion = initialVersion;
        }

        // latest version that was observed, possibly dirty read of a write that does not survive
        public Version latestObservedVersion() {
            return latestObservedVersion.get();
        }

        // latest version for which we got a successful response on a write.
        public Version latestSuccessfulVersion() {
            return latestSuccessfulVersion.get();
        }

        public Consumer<HistoryOutput> invoke(Version version) {
            int eventId = history.invoke(version);
            logger.debug("invocation partition ({}) event ({}) version ({})", id, eventId, version);
            return output -> consumeOutput(output, eventId);
        }

        private void consumeOutput(HistoryOutput output, int eventId) {
            history.respond(eventId, output);
            logger.debug("response partition ({}) event ({}) output ({})", id, eventId, output);
            latestObservedVersion.consume(output.getVersion());
            if (output instanceof IndexResponseHistoryOutput) {
                latestSuccessfulVersion.consume(output.getVersion());
            }
        }

        public void assertLinearizable() {
            logger.info("--> Linearizability checking history of size: {} for key: {} and initialVersion: {}: {}", history.size(),
                id, initialVersion, history);
            LinearizabilityChecker.SequentialSpec spec = new CASSequentialSpec(initialVersion);
            boolean linearizable = false;
            try {
                final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY);
                final AtomicBoolean abort = new AtomicBoolean();
                // Large histories can be problematic and have the linearizability checker run OOM
                // Bound the time how long the checker can run on such histories (Values empirically determined)
                if (history.size() > 300) {
                    scheduler.schedule(() -> abort.set(true), 10, TimeUnit.SECONDS);
                }
                linearizable = new LinearizabilityChecker().isLinearizable(spec, history, missingResponseGenerator(), abort::get);
                ThreadPool.terminate(scheduler, 1, TimeUnit.SECONDS);
                if (abort.get() && linearizable == false) {
                    linearizable = true; // let the test pass
                }
            } finally {
                // implicitly test that we can serialize all histories.
                String serializedHistory = base64Serialize(history);
                if (linearizable == false) {
                    // we dump base64 encoded data, since the nature of this test is that it does not reproduce even with same seed.
                    logger.error("Linearizability check failed. Spec: {}, initial version: {}, serialized history: {}",
                        spec, initialVersion, serializedHistory);
                }
            }
            assertTrue("Must be linearizable", linearizable);
        }
    }

    private static class CASSequentialSpec implements LinearizabilityChecker.SequentialSpec {
        private final Version initialVersion;

        private CASSequentialSpec(Version initialVersion) {
            this.initialVersion = initialVersion;
        }

        @Override
        public Object initialState() {
            return casSuccess(initialVersion);
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            State state = (State) currentState;
            if (output instanceof IndexResponseHistoryOutput) {
                if (input.equals(state.safeVersion) ||
                    (state.lastFailed && ((Version) input).compareTo(state.safeVersion) > 0)) {
                    return Optional.of(casSuccess(((IndexResponseHistoryOutput) output).getVersion()));
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.of(state.failed());
            }
        }
    }

    private static final class State {
        private final Version safeVersion;
        private final boolean lastFailed;

        private State(Version safeVersion, boolean lastFailed) {
            this.safeVersion = safeVersion;
            this.lastFailed = lastFailed;
        }

        public State failed() {
            return lastFailed ? this : casFail(safeVersion);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            State that = (State) o;
            return lastFailed == that.lastFailed &&
                safeVersion.equals(that.safeVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(safeVersion, lastFailed);
        }

        @Override
        public String toString() {
            return "State{" +
                "safeVersion=" + safeVersion +
                ", lastFailed=" + lastFailed +
                '}';
        }
    }

    private static State casFail(Version stateVersion) {
        return new State(stateVersion, true);
    }

    private static State casSuccess(Version version1) {
        return new State(version1, false);
    }

    /**
     * HistoryOutput contains the information from the output of calls.
     */
    private interface HistoryOutput extends NamedWriteable {
        Version getVersion();
    }

    private static class IndexResponseHistoryOutput implements HistoryOutput {
        private final Version outputVersion;

        private IndexResponseHistoryOutput(IndexResponse response) {
            this(new Version(response.getPrimaryTerm(), response.getSeqNo()));
        }

        private IndexResponseHistoryOutput(StreamInput input) throws IOException {
            this(new Version(input));
        }

        private IndexResponseHistoryOutput(Version outputVersion) {
            this.outputVersion = outputVersion;
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

    /**
     * We treat CAS failures (version conflicts) identically to failures in linearizability checker, but keep this separate
     * to parse out the latest observed version and to ease debugging.
     */
    private static class CASFailureHistoryOutput implements HistoryOutput {
        private Version outputVersion;
        private CASFailureHistoryOutput(VersionConflictEngineException exception) {
            this(parseException(exception.getMessage()));
        }

        private CASFailureHistoryOutput(StreamInput input) throws IOException {
            this(new Version(input));
        }

        private CASFailureHistoryOutput(Version outputVersion) {
            this.outputVersion = outputVersion;
        }

        private static Version parseException(String message) {
            // parsing out the version increases chance of hitting races against CAS successes, since if we did not parse this out, no
            // writes would succeed after a fail on own write failure (unless we were lucky enough to guess the seqNo/primaryTerm using the
            // random futureTerm/futureSeqNo handling in CASUpdateThread).
            try {
                Matcher matcher = EXTRACT_VERSION.matcher(message);
                matcher.find();
                return new Version(Long.parseLong(matcher.group(2)), Long.parseLong(matcher.group(1)));
            } catch (RuntimeException e) {
                throw new RuntimeException("Unable to parse message: " + message, e);
            }
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

    /**
     * A non version conflict failure.
     */
    private static class FailureHistoryOutput implements HistoryOutput {

        private FailureHistoryOutput() {
        }

        private FailureHistoryOutput(@SuppressWarnings("unused") StreamInput streamInput) {
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
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: <file> <primaryTerm> <seqNo>");
        } else {
            runLinearizabilityChecker(new FileInputStream(args[0]), Long.parseLong(args[1]), Long.parseLong(args[2]));
        }
    }

    @SuppressForbidden(reason = "system out is ok for a command line tool")
    private static void runLinearizabilityChecker(FileInputStream fileInputStream, long primaryTerm, long seqNo) throws IOException {
        StreamInput is = new InputStreamStreamInput(Base64.getDecoder().wrap(fileInputStream));
        is = new NamedWriteableAwareStreamInput(is, createNamedWriteableRegistry());

        LinearizabilityChecker.History history = readHistory(is);

        Version initialVersion = new Version(primaryTerm, seqNo);
        boolean result =
            new LinearizabilityChecker().isLinearizable(new CASSequentialSpec(initialVersion), history,
                missingResponseGenerator());

        System.out.println(LinearizabilityChecker.visualize(new CASSequentialSpec(initialVersion), history,
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

    public void testSequentialSpec() {
        // Generate 3 increasing versions
        Version version1 = new Version(randomIntBetween(1, 5), randomIntBetween(0, 100));
        Version version2 = futureVersion(version1);
        Version version3 = futureVersion(version2);

        List<Version> versions = List.of(version1, version2, version3);

        LinearizabilityChecker.SequentialSpec spec = new CASSequentialSpec(version1);

        assertThat(spec.initialState(), equalTo(casSuccess(version1)));

        assertThat(spec.nextState(casSuccess(version1), version1, new IndexResponseHistoryOutput(version2)),
            equalTo(Optional.of(casSuccess(version2))));
        assertThat(spec.nextState(casFail(version1), version2, new IndexResponseHistoryOutput(version3)),
            equalTo(Optional.of(casSuccess(version3))));
        assertThat(spec.nextState(casSuccess(version1), version2, new IndexResponseHistoryOutput(version3)),
            equalTo(Optional.empty()));
        assertThat(spec.nextState(casSuccess(version2), version1, new IndexResponseHistoryOutput(version3)),
            equalTo(Optional.empty()));
        assertThat(spec.nextState(casFail(version2), version1, new IndexResponseHistoryOutput(version3)),
            equalTo(Optional.empty()));

        // for version conflicts, we keep state version with lastFailed set, regardless of input/output version.
        versions.forEach(stateVersion ->
            versions.forEach(inputVersion ->
                versions.forEach(outputVersion -> {
                    assertThat(spec.nextState(casSuccess(stateVersion), inputVersion, new CASFailureHistoryOutput(outputVersion)),
                        equalTo(Optional.of(casFail(stateVersion))));
                    assertThat(spec.nextState(casFail(stateVersion), inputVersion, new CASFailureHistoryOutput(outputVersion)),
                        equalTo(Optional.of(casFail(stateVersion))));
                })
            )
        );

        // for non version conflict failures, we keep state version with lastFailed set, regardless of input version.
        versions.forEach(stateVersion ->
                versions.forEach(inputVersion -> {
                        assertThat(spec.nextState(casSuccess(stateVersion), inputVersion, new FailureHistoryOutput()),
                            equalTo(Optional.of(casFail(stateVersion))));
                        assertThat(spec.nextState(casFail(stateVersion), inputVersion, new FailureHistoryOutput()),
                            equalTo(Optional.of(casFail(stateVersion))));
                })
        );
    }

    private Version futureVersion(Version version) {
        Version futureVersion = version.nextSeqNo(randomIntBetween(1,10));
        if (randomBoolean())
            futureVersion = futureVersion.nextTerm();
        return futureVersion;
    }
 }
