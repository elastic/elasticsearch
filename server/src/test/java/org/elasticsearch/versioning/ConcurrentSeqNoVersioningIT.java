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
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.AbstractDisruptionTestCase;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
// we only use serializable to be able to debug test failures, circumventing the checkstyle check using spaces.
import java . io . Serializable;
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


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 3, maxNumDataNodes = 5,
    transportClientRatio = 0)
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
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(),
                    randomTimeValue(0, Math.max(disruptTimeSeconds*3/2, 2), "s"))
            ));

        ensureGreen();

        ServiceDisruptionScheme disruptionScheme = addRandomDisruptionScheme();

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
                .mapToObj(i -> new CASUpdateThread(i, roundBarrier, partitions))
                .collect(Collectors.toList());

        logger.info("--> Starting {} threads", threadCount);
        threads.forEach(Thread::start);

        try {
            int rounds = randomIntBetween(2, 5);

            logger.info("--> Running {} rounds", rounds);

            for (int i = 0; i < rounds; ++i) {
                roundBarrier.await(1, TimeUnit.MINUTES);
                disruptionScheme.startDisrupting();
                logger.info("--> round {}", i);
                try {
                    roundBarrier.await(disruptTimeSeconds, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    roundBarrier.reset();
                }
                disruptionScheme.stopDisrupting();
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

        private volatile boolean stop;
        private final Random random = new Random(randomLong());

        private CASUpdateThread(int threadNum, CyclicBarrier roundBarrier, List<Partition> partitions) {
            super("CAS-Update-" + threadNum);
            this.roundBarrier = roundBarrier;
            this.partitions = partitions;
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
                        final int pct = random.nextInt(100);
                        final boolean futureSeqNo = pct < 10;
                        Version version = partition.latestKnownVersion();
                        if (futureSeqNo) {
                            version = version.nextSeqNo();
                        } else if (pct < 15) {
                            version = version.previousSeqNo();
                        }
                        Consumer<HistoryOutput> historyResponse = partition.invoke(version);
                        try {
                            // we should be able to remove timeout or fail hard on timeouts if we fix network disruptions to be
                            // realistic, ie. not silently throw data out.
                            IndexResponse indexResponse = client().prepareIndex("test", "type", partition.id)
                                .setSource("value", random.nextInt())
                                .setIfPrimaryTerm(version.primaryTerm)
                                .setIfSeqNo(version.seqNo)
                                .execute().actionGet(40, TimeUnit.SECONDS);
                            historyResponse.accept(new IndexResponseHistoryOutput(indexResponse));
                        } catch (VersionConflictEngineException e) {
                            historyResponse.accept(new CASFailureHistoryOutput(e));
                        } catch (RuntimeException e) {
                            // if we used a future seqNo, we cannot know if it will overwrite a future update when failing with
                            // unknown error
                            if (futureSeqNo == false)
                                historyResponse.accept(new FailureHistoryOutput());
                        }
                    }

                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    // check to stop
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
            for (;;) {
                Version current = this.current.get();
                if (version.lteq(current) || this.current.compareAndSet(current, version)) {
                    break;
                }
            }
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
            Consumer<Object> historyResponse = history.invoke2(version);
            return output -> consumeOutput(output, historyResponse);
        }

        private void consumeOutput(HistoryOutput output, Consumer<Object> historyResponse) {
            historyResponse.accept(output);
            // we try to use the highest seen version for the next request. We could think that this could lead to one dirty read that
            // causes us to stick to errors for the rest of the run. But if we have a dirty read/CAS failure, it must be on an old primary
            // and the new primary will have a larger primaryTerm and a subsequent CAS failure will ensure we notice the new primaryTerm
            // and seqNo
            atomicVersion.consume(output.getVersion());
        }

        public boolean isLinearizable() {
            logger.info("--> Linearizability checking history of size: {} for key: {} and initialVersion: {}: {}", history.size(),
                id, initialVersion, history);
            boolean linearizable =
                new LinearizabilityChecker().isLinearizable(new CASSequentialSpec(new SuccessState(initialVersion)), history,
                    missingResponseGenerator());
            if (linearizable == false) {
                // we dump base64 encoded data, since the nature of this test is that it does not reproduce even with same seed.
                logger.info("Linearizability check failed. Initial version: {}, serialized history: {}", initialVersion,
                    base64Serialize(history));
            }
            return linearizable;
        }

        public void assertLinearizable() {
            assertTrue("Must be linearizable", isLinearizable());
        }

    }

    private static class CASSequentialSpec implements LinearizabilityChecker.SequentialSpec {

        private final State initialState;

        private CASSequentialSpec(State initialState) {
            this.initialState = initialState;
        }

        @Override
        public Object initialState() {
            return initialState;
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            return ((HistoryOutput) output).nextState((State) currentState, (Version) input).map(i -> i); // Optional<?> to Optional<Object>
        }
    }


    /**
     * Our version, which is primaryTerm,seqNo.
     */
    private static final class Version implements Serializable {
        public final long primaryTerm;
        public final long seqNo;

        Version(long primaryTerm, long seqNo) {
            this.primaryTerm = primaryTerm;
            this.seqNo = seqNo;
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

        public boolean lteq(Version other) {
            return primaryTerm < other.primaryTerm
                || (primaryTerm == other.primaryTerm && seqNo <= other.seqNo);
        }

        public boolean gt(Version other) {
            return lteq(other) == false;
        }
        @Override
        public String toString() {
            return "{" + "primaryTerm=" + primaryTerm + ", seqNo=" + seqNo + '}';
        }

        public Version nextSeqNo() {
            return new Version(primaryTerm, seqNo + 1);
        }

        public Version previousSeqNo() {
            return new Version(primaryTerm, seqNo - 1);
        }
    }

    /**
     * State and its subclasses model the state of the partition (key) for the linearizability checker.
     *
     * We go a step deeper than just modelling successes, since we can then then do more validations.
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
        // 2. Fail on our own write: the write to a replica was already done, but not responded to primary (or multiple replicas). And
        // replica is promoted to primary, causing the write to fail on its own update.
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
            if (safeVersion.equals(inputVersion) || casFailVersion.equals(inputVersion))
                return Optional.of(new SuccessState(outputVersion));

            if (inputVersion.gt(safeVersion) && inputVersion.primaryTerm < casFailVersion.primaryTerm) {
                // After a CAS fail on own write, we do not know if a number of interim updates were made before the final CAS.
                // Suppose last term was 1 (t=1,s=0) on n1.
                // Suppose we write on n1 successfully (t=1,s=1). Then during replication, we find that n2 is primary and write here,
                // again successfully (t=2, s=2). Again during replication, we find that n3 is primary and then CAS fail on n3 against our
                // own write (t=2, s=2).
                // The ghost write (t=1, s=1) was never seen neither as success nor CAS fail write and we therefore have to accept that
                // CAS can succeed against any version where safeVersion <= version < (casFailVersion.primaryTerm,0)
                // This is so even if n3 and n2 does not die, since this following successful CAS write could complete (including
                // replication) between storing the CAS fail write and the primary noticing that it is not the primary.

                // Notice that if we see a CAS succeed in same term as last casFailVersion, inputVersion must match the seqNo too since
                // same term means same primary and thus no change of state since last operation (we do CAS validation on a believed to
                // be primary).
                return Optional.of(new SuccessState(outputVersion));
                // todo: add more advanced network disruptions with floating partitioning to provoke above in more cases.
            }

            return Optional.empty();
        }

        @Override
        Optional<State> casFail(Version inputVersion, Version outputVersion) {
            // see comment above in casSuccess, we can fail against one of:
            // 1. our own write (outputVersion > casFailVersion)
            // 2. any of the previous interim/ghost writes (safeVersion < outputVersion <= casFailVersion
            // .primaryTerm)
            // 3. last success write. (safeVersion == outputVersion). This is either a regular fail or a stale read failure, we cannot
            // tell since we do not know which shard ends up winning.
            // 4. A plain stale read (safeVersion.primaryTerm > outputVersion.primaryTerm)

            // for 1-3, we have to continue being in CASFailOwnWriteState
            // of this failure or a previous failure in a future response.
            // We use the max cas fail version, which is correct due to the checking in casSuccess.
            // for 4, we ignore the stale read (does not affect state).

            Version newCASFailVersion = outputVersion.gt(casFailVersion) ? outputVersion : casFailVersion;
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
            if (safeVersion.gt(inputVersion) || safeVersion.gt(outputVersion))
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
    private interface HistoryOutput extends Serializable {
        Optional<State> nextState(State currentState, Version input);

        Version getVersion();
    }

    private static class IndexResponseHistoryOutput implements HistoryOutput {
        private final Version outputVersion;

        private IndexResponseHistoryOutput(IndexResponse response) {
            this.outputVersion = new Version(response.getPrimaryTerm(), response.getSeqNo());
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
    }

    private static class CASFailureHistoryOutput implements HistoryOutput {
        private Version outputVersion;
        private CASFailureHistoryOutput(VersionConflictEngineException exception) {
            this.outputVersion = parseException(exception.getMessage());
        }

        private Version parseException(String message) {
            // ugly, but having these observed versions available improves the linearizability checking and ensures progress.
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
    }

    private static class FailureHistoryOutput implements HistoryOutput {

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
    }

    private static Function<Object, Object> missingResponseGenerator() {
        return input -> new FailureHistoryOutput();
    }

    @SuppressForbidden(reason = "we only serialize data in the test case to be able to deserialize it for debugging")
    private String base64Serialize(LinearizabilityChecker.History history) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(outStream);
            oos.writeObject(history);
            oos.close();
            return Base64.getEncoder().encodeToString(outStream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
    private static void runLinearizabilityChecker(FileInputStream fileInputStream, long primaryTerm, long seqNo) throws IOException,
        ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(Base64.getDecoder().wrap(fileInputStream));

        LinearizabilityChecker.History history = (LinearizabilityChecker.History) ois.readObject();

        Version initialVersion = new Version(primaryTerm, seqNo);
        boolean result =
            new LinearizabilityChecker().isLinearizable(new CASSequentialSpec(new SuccessState(initialVersion)), history,
                missingResponseGenerator());

        System.out.println("Linearizable?: " + result);
    }
}

