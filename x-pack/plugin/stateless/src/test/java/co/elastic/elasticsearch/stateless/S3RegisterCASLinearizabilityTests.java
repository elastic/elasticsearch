package co.elastic.elasticsearch.stateless;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

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
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;

@ESTestCase.WithoutSecurityManager
public class S3RegisterCASLinearizabilityTests extends ESTestCase {
    private S3ClientRef getClientRef() {
        final String region = System.getProperty("test.s3.region", "eu-central-1");
        final String accessKey = System.getProperty("test.s3.access_key", null);
        final String secretKey = System.getProperty("test.s3.secret_key", null);

        if (accessKey != null && secretKey != null) {
            return new S3ClientRef(region, accessKey, secretKey);
        }

        // Fallback to the default credential provider when the system properties are not defined
        return new S3ClientRef(region);
    }

    private String getBucket() {
        return System.getProperty("test.s3.bucket", "francisco-stateless-tests");
    }

    @AwaitsFix(bugUrl = "https://elasticco.atlassian.net/browse/ES-4967")
    public void testCASIsLinearizable() throws Exception {
        final var bucket = getBucket();
        final var registerKey = UUIDs.randomBase64UUID();

        try (var clientRef = getClientRef()) {
            Integer initialValue = null;
            if (randomBoolean()) {
                initialValue = randomInt();
                // set the initial value
                clientRef.getClient().putObject(bucket, registerKey, Integer.toString(initialValue));
            }

            final var linearizabilityChecker = new LinearizabilityChecker();
            final var history = new LinearizabilityChecker.History();
            final var sequentialSpec = new CASSequentialSpec(initialValue);

            // The SDK connection pool size is 50 by default, we must not
            // go over that value, otherwise the threads might have to wait
            // in order to get a connection.
            final var numberOfThreads = randomIntBetween(2, 12);
            final var startBarrier = new CyclicBarrier(numberOfThreads + 1);
            final List<RegisterUpdaterThread> threads = new ArrayList<>();
            for (int i = 0; i < numberOfThreads; i++) {
                threads.add(
                    new RegisterUpdaterThread(
                        i,
                        startBarrier,
                        history,
                        () -> new S3Register(new DisruptableS3Client(clientRef.getClient()), bucket, registerKey, Randomness.get()) {
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

            threads.forEach(RegisterUpdaterThread::await);

            assertThat(
                LinearizabilityChecker.visualize(sequentialSpec, history, (invId -> { throw new IllegalStateException(); })),
                linearizabilityChecker.isLinearizable(sequentialSpec, history),
                is(true)
            );

            clean(clientRef.getClient(), bucket, registerKey);
        }
    }

    void clean(AmazonS3 client, String bucket, String key) {
        client.deleteObject(bucket, key);
    }

    static class RegisterUpdaterThread extends Thread {
        private final CyclicBarrier startBarrier;
        private final LinearizabilityChecker.History history;
        private final Supplier<S3Register> s3RegisterSupplier;

        RegisterUpdaterThread(
            int threadNumber,
            CyclicBarrier startBarrier,
            LinearizabilityChecker.History history,
            Supplier<S3Register> s3RegisterSupplier
        ) {
            super("register-updater-" + threadNumber);
            this.s3RegisterSupplier = s3RegisterSupplier;
            this.startBarrier = startBarrier;
            this.history = history;
            setDaemon(true);
        }

        public void run() {
            // We need to initialize it here so Randomness#get() is created in the appropriate thread
            S3Register s3Register = s3RegisterSupplier.get();
            waitForStartBarrier();
            final int iterations = randomIntBetween(50, 100);
            for (int i = 0; i < iterations; i++) {
                if (randomBoolean()) {
                    sleep(randomIntBetween(0, 200));
                }

                Integer currentVersion;
                try {
                    currentVersion = s3Register.getCurrentValue();
                } catch (IOException e) {
                    // Try again in a bit
                    continue;
                }

                var proposedVersion = randomInt();
                var casOperation = new S3Register.CASOperation(currentVersion, proposedVersion);
                var invocationId = history.invoke(casOperation);
                try {
                    var result = s3Register.performOperation(casOperation);
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
                throw new RuntimeException(e);
            }
        }

        void waitForStartBarrier() {
            try {
                startBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }

        void await() {
            try {
                join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
            this.client = AmazonS3ClientBuilder.standard().withRegion(region).build();
        }

        S3ClientRef(String region, String accessKey, String secretKey) {
            this(region, new BasicAWSCredentials(accessKey, secretKey));
        }

        S3ClientRef(String region, @Nullable BasicAWSCredentials staticCredentials) {
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
