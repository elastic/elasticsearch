/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import org.elasticsearch.cluster.coordination.LinearizabilityChecker;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.LinearizabilityCheckAborted;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class S3RegisterCASLinearizabilityTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        TestThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    private static String getRequiredProperty(String key) {
        return Strings.requireNonBlank(System.getProperty(key), "Required system property `" + key + "` is not set");
    }

    private S3ClientRef getClientRef() {
        final String region = getRequiredProperty("test.s3.region");
        final String accessKey = System.getProperty("test.s3.access_key");
        final String secretKey = System.getProperty("test.s3.secret_key");
        final String sessionToken = System.getProperty("test.s3.session_token");
        if (accessKey != null && secretKey != null) {
            return new S3ClientRef(region, accessKey, secretKey, sessionToken);
        } else {
            // Running the test in a development mode. Make sure you local AWS development environment is correctly configured
            // For instructions see https://github.com/elastic/elasticsearch-benchmarks/blob/master/docs/authentication.md#aws-auth
            return new S3ClientRef(region);
        }
    }

    public void testCASIsLinearizable() throws Exception {
        final int maxThreadSleepTimeInMillis = randomFrom(10, 200);
        runCASTest(maxThreadSleepTimeInMillis, testState -> {
            final var threads = testState.registerThreads();

            final var history = testState.history();
            final var sequentialSpec = testState.sequentialSpec();

            threads.forEach(RegisterUpdaterThread::await);

            try {
                boolean isLinearizable = LinearizabilityChecker.isLinearizable(sequentialSpec, history);
                assertThat(
                    LinearizabilityChecker.visualize(sequentialSpec, history, (invId -> { throw new IllegalStateException(); })),
                    isLinearizable,
                    is(true)
                );
            } catch (LinearizabilityCheckAborted e) {
                logger.warn("linearizability check was aborted, assuming linearizable.", e);
            }
        });
    }

    public void testCASLiveness() throws Exception {
        final int maxThreadSleepTimeInMillis = 200;
        runCASTest(maxThreadSleepTimeInMillis, testState -> {
            final var threads = testState.registerThreads();

            assertBusy(() -> {
                final int totalSuccessfulWrites = threads.stream().mapToInt(RegisterUpdaterThread::getSuccessfulWrites).sum();
                assertThat(totalSuccessfulWrites, is(greaterThanOrEqualTo(1)));
            }, 1, TimeUnit.MINUTES);

            testState.abortUpdates().run();
        });
    }

    private void runCASTest(int maxThreadSleepTimeInMillis, CheckedConsumer<TestState, Exception> testBody) throws Exception {
        final var bucket = getRequiredProperty("test.s3.bucket");
        assertThat(bucket, not(blankOrNullString()));
        final String basePath = System.getProperty("test.s3.base_path", "stateless-cas-linearizability");
        final var registerKey = basePath + "/" + UUIDs.randomBase64UUID();

        try (var clientRef = getClientRef()) {
            Integer initialValue = null;
            if (randomBoolean()) {
                initialValue = randomInt();
                // set the initial value
                clientRef.getClient().putObject(bucket, registerKey, Integer.toString(initialValue));
            }

            final var running = new AtomicBoolean(true);
            final var history = new LinearizabilityChecker.History();
            final var sequentialSpec = new CASSequentialSpec(initialValue);

            final var numberOfThreads = randomIntBetween(2, 16);
            final var startBarrier = new CyclicBarrier(numberOfThreads + 1);
            final List<RegisterUpdaterThread> threads = new ArrayList<>();
            for (int i = 0; i < numberOfThreads; i++) {
                threads.add(
                    new RegisterUpdaterThread(
                        i,
                        startBarrier,
                        history,
                        running,
                        maxThreadSleepTimeInMillis,
                        () -> new S3Register(
                            new DisruptableS3Client(clientRef.getClient()),
                            bucket,
                            registerKey,
                            Randomness.get(),
                            threadPool
                        ) {
                            @Override
                            Integer getCurrentValue() throws IOException {
                                int invocationId = history.invoke(null);
                                try {
                                    var value = super.getCurrentValue();
                                    history.respond(invocationId, value);
                                    return value;
                                } catch (Exception e) {
                                    history.respond(invocationId, null);
                                    throw e;
                                }
                            }
                        }
                    )
                );
            }

            threads.forEach(RegisterUpdaterThread::start);

            startBarrier.await();

            try {
                testBody.accept(new TestState(sequentialSpec, history, threads, () -> running.set(false)));
                threads.forEach(RegisterUpdaterThread::await);
            } finally {
                clean(clientRef.getClient(), bucket, registerKey);
            }
        }
    }

    void clean(AmazonS3 client, String bucket, String key) {
        client.deleteObject(bucket, key);
    }

    record TestState(
        CASSequentialSpec sequentialSpec,
        LinearizabilityChecker.History history,
        List<RegisterUpdaterThread> registerThreads,
        Runnable abortUpdates
    ) {}

    static class RegisterUpdaterThread extends Thread {
        private final CyclicBarrier startBarrier;
        private final LinearizabilityChecker.History history;
        private final Supplier<S3Register> s3RegisterSupplier;
        private final int threadNumber;
        private final AtomicBoolean running;
        private final int maxSleepTimeInMillis;
        private final AtomicInteger successfulWrites = new AtomicInteger();

        RegisterUpdaterThread(
            int threadNumber,
            CyclicBarrier startBarrier,
            LinearizabilityChecker.History history,
            AtomicBoolean running,
            int maxSleepTimeInMillis,
            Supplier<S3Register> s3RegisterSupplier
        ) {
            super("register-updater-" + threadNumber);
            this.s3RegisterSupplier = s3RegisterSupplier;
            this.startBarrier = startBarrier;
            this.history = history;
            this.threadNumber = threadNumber;
            this.running = running;
            this.maxSleepTimeInMillis = maxSleepTimeInMillis;
            setDaemon(true);
        }

        public void run() {
            // We need to initialize it here so Randomness#get() is created in the appropriate thread
            S3Register s3Register = s3RegisterSupplier.get();
            waitForStartBarrier();
            final int iterations = randomIntBetween(50, 100);
            for (int i = 0; i < iterations && running.get(); i++) {
                if (randomBoolean()) {
                    sleep(randomIntBetween(0, maxSleepTimeInMillis));
                }

                Integer currentVersion;
                try {
                    currentVersion = s3Register.getCurrentValue();
                } catch (IOException e) {
                    // Try again in a bit
                    continue;
                }

                var proposedVersion = randomInt() + threadNumber;
                var casOperation = new S3Register.CASOperation(currentVersion, proposedVersion);
                var invocationId = history.invoke(casOperation);
                try {
                    var result = s3Register.performOperation(casOperation);
                    if (result == S3Register.CASStatus.SUCCESSFUL) {
                        successfulWrites.incrementAndGet();
                    }
                    history.respond(invocationId, result);
                } catch (Exception e) {
                    // All failure scenarios are handled in S3Register#performOperation, but we want to be extra
                    // careful here.
                    history.respond(invocationId, S3Register.CASStatus.UNKNOWN);
                }
            }
        }

        void sleep(int millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void waitForStartBarrier() {
            try {
                startBarrier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }

        void await() {
            try {
                join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        public int getSuccessfulWrites() {
            return successfulWrites.get();
        }
    }

    record State(@Nullable Integer currentValue, Set<Integer> possibleValues, boolean unknownWrites) {
        State casSucceeded(int newValue) {
            return new State(newValue, Set.of(), false);
        }

        State casUnknown(int possibleNewValue) {
            Set<Integer> updatedPossibleValues = new HashSet<>(possibleValues);
            updatedPossibleValues.add(possibleNewValue);
            return new State(currentValue, Collections.unmodifiableSet(updatedPossibleValues), true);
        }
    }

    static class CASSequentialSpec implements LinearizabilityChecker.SequentialSpec {
        private final State initialState;

        CASSequentialSpec(@Nullable Integer initialValue) {
            this.initialState = new State(initialValue, Set.of(), false);
        }

        @Override
        public Object initialState() {
            return initialState;
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            State state = (State) currentState;
            // reads are signaled with a null input
            if (input == null) {
                // output == null signals that we got an error while reading the value
                if (output == null || validRead(state, ((Integer) output))) {
                    return Optional.of(state);
                }
            } else {
                S3Register.CASOperation casOperation = (S3Register.CASOperation) input;
                S3Register.CASStatus opStatus = (S3Register.CASStatus) output;
                switch (opStatus) {
                    case SUCCESSFUL -> {
                        if (validRead(state, casOperation.expectedValue())) {
                            return Optional.of(state.casSucceeded(casOperation.newValue()));
                        }
                    }
                    case UNKNOWN -> {
                        return Optional.of(state.casUnknown(casOperation.newValue()));
                    }
                    case FAILED -> {
                        return Optional.of(state);
                    }
                    default -> throw new IllegalArgumentException("Unexpected status");
                }
            }

            return Optional.empty();
        }

        static boolean validRead(State state, Integer readValue) {
            return Objects.equals(state.currentValue(), readValue)
                || (state.unknownWrites() && readValue != null && state.possibleValues().contains(readValue));
        }
    }

    static class S3ClientRef implements Closeable {
        private final AmazonS3 client;

        S3ClientRef(String region) {
            this.client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                // Allows concurrent cancellations when there are many competing threads, the default is 50.
                .withClientConfiguration(new ClientConfiguration().withMaxConnections(15_000))
                .build();
        }

        S3ClientRef(String region, String accessKey, String secretKey, String sessionToken) {
            this(region, new BasicSessionCredentials(accessKey, secretKey, sessionToken));
        }

        S3ClientRef(String region, @Nullable AWSCredentials staticCredentials) {
            var credentialsProvider = staticCredentials != null
                ? new AWSStaticCredentialsProvider(staticCredentials)
                : DefaultAWSCredentialsProviderChain.getInstance();
            this.client = AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(credentialsProvider).build();
        }

        public AmazonS3 getClient() {
            return client;
        }

        @Override
        public void close() {
            client.shutdown();
            IdleConnectionReaper.shutdown();
        }
    }

    static class DisruptableS3Client extends AbstractAmazonS3 {
        private final AmazonS3 delegate;

        DisruptableS3Client(AmazonS3 delegate) {
            this.delegate = delegate;
        }

        @Override
        public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws SdkClientException {
            return delegate.initiateMultipartUpload(request);
        }

        @Override
        public UploadPartResult uploadPart(UploadPartRequest request) throws SdkClientException {
            return delegate.uploadPart(request);
        }

        @Override
        public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) throws SdkClientException {
            final var response = delegate.completeMultipartUpload(request);

            if (rarely()) {
                throw new SdkClientException(new IOException("Simulated connection closed after a successful write"));
            }

            return response;
        }

        @Override
        public void abortMultipartUpload(AbortMultipartUploadRequest request) throws SdkClientException {
            delegate.abortMultipartUpload(request);
        }

        @Override
        public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws SdkClientException {
            return delegate.listMultipartUploads(request);
        }

        @Override
        public S3Object getObject(String bucketName, String key) throws SdkClientException {
            return delegate.getObject(bucketName, key);
        }

        @Override
        public PutObjectResult putObject(String bucketName, String key, String content) throws SdkClientException {
            return delegate.putObject(bucketName, key, content);
        }
    }
}
