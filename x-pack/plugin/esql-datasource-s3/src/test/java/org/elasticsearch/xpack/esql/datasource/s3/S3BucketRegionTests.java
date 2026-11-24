/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3BucketRegionTests extends ESTestCase {

    private static final String ACCESS_KEY = "AKIDEXAMPLE";
    private static final String SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    private static final String BUCKET = "foreign-bucket";
    private static final String KEY = "data/file.bin";
    private static final byte[] BODY = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forBreaker(NoopCircuitBreaker.INSTANCE);

    public void testConcurrentColdReadsOfForeignRegionBucketResolveRegionOnce() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "ca-central-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                int threads = 16;
                CyclicBarrier start = new CyclicBarrier(threads);
                CountDownLatch done = new CountDownLatch(threads);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
                for (int i = 0; i < threads; i++) {
                    Thread t = new Thread(() -> {
                        try {
                            start.await(10, TimeUnit.SECONDS);
                            try (InputStream in = provider.newObject(path).newStream()) {
                                assertArrayEquals(BODY, in.readAllBytes());
                            }
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        } finally {
                            done.countDown();
                        }
                    });
                    t.start();
                }
                assertTrue(done.await(30, TimeUnit.SECONDS));
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }
                assertEquals(1, fake.headBucketCount(BUCKET));
                assertEquals(0, fake.wrongRegionGets(BUCKET));
            }
        }
    }

    public void testRegionTakenFromSuccessfulHeadBucket() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "ca-central-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                try (InputStream in = provider.newObject(StoragePath.of("s3://" + BUCKET + "/" + KEY)).newStream()) {
                    assertArrayEquals(BODY, in.readAllBytes());
                }
                assertEquals(1, fake.headBucketCount(BUCKET));
                assertEquals(0, fake.wrongRegionGets(BUCKET));
            }
        }
    }

    public void testTwoBucketsInDifferentRegionsUnderOneDataSource() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket("bucket-a", "eu-west-1");
            fake.putBucket("bucket-b", "ap-southeast-2");
            fake.putObject("bucket-a", KEY, BODY);
            byte[] other = new byte[] { 9, 9, 9 };
            fake.putObject("bucket-b", KEY, other);
            S3Configuration config = S3Configuration.fromMap(
                Map.of("access_key", ACCESS_KEY, "secret_key", SECRET_KEY, "endpoint", fake.endpoint().toString())
            );
            try (S3StorageProvider provider = new S3StorageProvider(config)) {
                try (InputStream in = provider.newObject(StoragePath.of("s3://bucket-a/" + KEY)).newStream()) {
                    assertArrayEquals(BODY, in.readAllBytes());
                }
                try (InputStream in = provider.newObject(StoragePath.of("s3://bucket-b/" + KEY)).newStream()) {
                    assertArrayEquals(other, in.readAllBytes());
                }
            }
        }
    }

    public void testProbeFailureIsNotCached() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "ca-central-1");
            fake.putObject(BUCKET, KEY, BODY);
            // SDK Standard retries HeadBucket 503s, so keep failing until this probe gives up.
            fake.failNextHeadBuckets(Integer.MAX_VALUE);
            try (S3StorageProvider provider = provider(fake)) {
                StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
                expectThrows(ExternalUnavailableException.class, () -> provider.newObject(path).newStream());
                int failedProbeHeads = fake.headBucketCount(BUCKET);
                assertThat(failedProbeHeads, greaterThan(0));
                fake.failNextHeadBuckets(0);
                try (InputStream in = provider.newObject(path).newStream()) {
                    assertArrayEquals(BODY, in.readAllBytes());
                }
                assertThat(fake.headBucketCount(BUCKET), greaterThan(failedProbeHeads));
            }
        }
    }

    public void testExplicitRegionSkipsProbe() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "eu-west-1");
            fake.putObject(BUCKET, KEY, BODY);
            S3Configuration config = S3Configuration.fromMap(
                Map.of("access_key", ACCESS_KEY, "secret_key", SECRET_KEY, "region", "eu-west-1")
            );
            try (S3StorageProvider provider = S3StorageProvider.forTestEndpoint(config, fake.endpoint())) {
                try (InputStream in = provider.newObject(StoragePath.of("s3://" + BUCKET + "/" + KEY)).newStream()) {
                    assertArrayEquals(BODY, in.readAllBytes());
                }
                assertEquals(0, fake.headBucketCount(BUCKET));
            }
            S3Configuration wrong = S3Configuration.fromMap(
                Map.of("access_key", ACCESS_KEY, "secret_key", SECRET_KEY, "region", "us-west-1")
            );
            try (S3StorageProvider provider = S3StorageProvider.forTestEndpoint(wrong, fake.endpoint())) {
                expectThrows(Exception.class, () -> provider.newObject(StoragePath.of("s3://" + BUCKET + "/" + KEY)).newStream());
                assertEquals(0, fake.headBucketCount(BUCKET));
            }
        }
    }

    public void testStaleRegionSelfHealsOnce() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "eu-west-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
                var object = provider.newObject(path);
                try (InputStream in = object.newStream()) {
                    assertArrayEquals(BODY, in.readAllBytes());
                }
                fake.setHome(BUCKET, "ap-southeast-2");
                try (InputStream in = object.newStream()) {
                    assertArrayEquals(BODY, in.readAllBytes());
                }
                fake.setHome(BUCKET, "ca-central-1");
                try (InputStream in = object.newStream()) {
                    assertArrayEquals(BODY, in.readAllBytes());
                }
            }
        }
    }

    public void testAsyncStaleRegionSelfHealsEachRead() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "eu-west-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
                StorageObject object = provider.newObject(path, BODY.length);
                assertAsyncBody(object);
                fake.setHome(BUCKET, "ap-southeast-2");
                assertAsyncBody(object);
                fake.setHome(BUCKET, "ca-central-1");
                assertAsyncBody(object);
            }
        }
    }

    public void testNoBucketRegionHeaderFallsBackToSeedRegion() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "us-east-1");
            fake.putObject(BUCKET, KEY, BODY);
            fake.sendRegionHeader(false);
            fake.rejectWrongScope(false);
            S3Configuration config = S3Configuration.fromMap(
                Map.of("access_key", ACCESS_KEY, "secret_key", SECRET_KEY, "endpoint", fake.endpoint().toString())
            );
            try (S3StorageProvider provider = new S3StorageProvider(config)) {
                try (InputStream in = provider.newObject(StoragePath.of("s3://" + BUCKET + "/" + KEY)).newStream()) {
                    assertArrayEquals(BODY, in.readAllBytes());
                }
            }
        }
    }

    public void testCloseClosesPerRegionClients() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket("bucket-a", "eu-west-1");
            fake.putBucket("bucket-b", "ap-southeast-2");
            fake.putObject("bucket-a", KEY, BODY);
            fake.putObject("bucket-b", KEY, BODY);
            S3StorageProvider provider = provider(fake);
            try (InputStream in = provider.newObject(StoragePath.of("s3://bucket-a/" + KEY)).newStream()) {
                assertArrayEquals(BODY, in.readAllBytes());
            }
            try (InputStream in = provider.newObject(StoragePath.of("s3://bucket-b/" + KEY)).newStream()) {
                assertArrayEquals(BODY, in.readAllBytes());
            }
            provider.close();
            ExternalUnavailableException thrown = expectThrows(
                ExternalUnavailableException.class,
                () -> provider.newObject(StoragePath.of("s3://bucket-a/" + KEY)).newStream()
            );
            assertThat(thrown.getMessage(), containsString("unavailable"));
        }
    }

    public void testRangeAndAsyncReadsUseResolvedRegion() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "ca-central-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
                try (InputStream in = provider.newObject(path, BODY.length).newStream(2, 3)) {
                    assertArrayEquals(new byte[] { 3, 4, 5 }, in.readAllBytes());
                }
                assertAsyncBody(provider.newObject(path, BODY.length));
                assertEquals(0, fake.wrongRegionGets(BUCKET));
            }
        }
    }

    public void testColdAsyncReadUsesResolvedRegion() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "ca-central-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
                assertAsyncBody(provider.newObject(path, BODY.length));
                assertEquals(1, fake.headBucketCount(BUCKET));
                assertEquals(0, fake.wrongRegionGets(BUCKET));
            }
        }
    }

    public void testColdAsyncCancelCompletesWithoutAbortingProbe() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "ca-central-1");
            fake.putObject(BUCKET, KEY, BODY);
            fake.holdHeadBucket(entered, release);
            try (S3StorageProvider provider = provider(fake)) {
                StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
                CountDownLatch done = new CountDownLatch(1);
                AtomicReference<Exception> failure = new AtomicReference<>();
                Releasable cancel = provider.newObject(path, BODY.length)
                    .startReadBytesAsync(0, BODY.length, FACTORY, pool, new ActionListener<>() {
                        @Override
                        public void onResponse(DirectReadBuffer result) {
                            result.close();
                            done.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            failure.set(e);
                            done.countDown();
                        }
                    });
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                cancel.close();
                assertTrue("cancelled read must complete while HeadBucket is still blocked", done.await(2, TimeUnit.SECONDS));
                assertThat(failure.get(), instanceOf(TaskCancelledException.class));
                release.countDown();
                try (InputStream in = provider.newObject(path).newStream()) {
                    assertArrayEquals(BODY, in.readAllBytes());
                }
                assertEquals(1, fake.headBucketCount(BUCKET));
            }
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    public void testProbeNoSuchBucketIsNotObjectNotFound() {
        S3Client probeSync = mock(S3Client.class);
        when(probeSync.headBucket(any(HeadBucketRequest.class))).thenThrow(
            NoSuchBucketException.builder().message("The specified bucket does not exist").statusCode(404).build()
        );
        BucketRegionResolver resolver = new BucketRegionResolver(
            "us-east-1",
            null,
            probeSync,
            mock(S3AsyncClient.class),
            TimeUnit.SECONDS.toNanos(5),
            region -> new BucketRegionResolver.ClientPair(mock(S3Client.class), mock(S3AsyncClient.class))
        );
        try {
            ExternalClientException thrown = expectThrows(ExternalClientException.class, () -> resolver.clientsFor("missing"));
            assertEquals(ExternalException.Condition.METADATA_UNAVAILABLE, thrown.condition());
            assertFalse(ExternalException.isStaleCachedObject(thrown));
            assertThat(thrown.getMessage(), containsString("NoSuchBucket"));
        } finally {
            resolver.close();
        }
    }

    public void testResolverCloseClosesExtraPairs() throws Exception {
        S3Client probeSync = mock(S3Client.class);
        S3AsyncClient probeAsync = mock(S3AsyncClient.class);
        S3Client extraSync = mock(S3Client.class);
        S3AsyncClient extraAsync = mock(S3AsyncClient.class);
        AtomicInteger closed = new AtomicInteger();
        doAnswer(invocation -> {
            closed.incrementAndGet();
            return null;
        }).when(extraSync).close();
        doAnswer(invocation -> {
            closed.incrementAndGet();
            return null;
        }).when(extraAsync).close();
        BucketRegionResolver resolver = new BucketRegionResolver(
            "us-east-1",
            "eu-west-1",
            probeSync,
            probeAsync,
            TimeUnit.SECONDS.toNanos(5),
            region -> new BucketRegionResolver.ClientPair(extraSync, extraAsync)
        );
        resolver.clientsFor("bucket");
        resolver.close();
        assertEquals(2, closed.get());
        verify(probeSync, never()).close();
        verify(probeAsync, never()).close();
    }

    public void testRememberRegionDoesNotOverwriteNewer() throws Exception {
        AtomicReference<String> built = new AtomicReference<>();
        BucketRegionResolver resolver = new BucketRegionResolver(
            "us-east-1",
            null,
            mock(S3Client.class),
            mock(S3AsyncClient.class),
            TimeUnit.SECONDS.toNanos(5),
            region -> {
                built.set(region);
                return new BucketRegionResolver.ClientPair(mock(S3Client.class), mock(S3AsyncClient.class));
            }
        );
        resolver.rememberRegion("bucket", "eu-west-1");
        resolver.rememberRegion("bucket", "ap-southeast-2");
        resolver.clientsFor("bucket");
        assertEquals("eu-west-1", built.get());
        resolver.close();
    }

    public void testListChildrenUsesResolvedRegion() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "ca-central-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                var children = provider.listChildren(StoragePath.of("s3://" + BUCKET + "/data"), 16);
                assertNotNull(children);
                assertEquals(1, fake.headBucketCount(BUCKET));
                assertEquals(0, fake.wrongRegionGets(BUCKET));
            }
        }
    }

    public void testListChildrenSelfHealsStaleRegion() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "eu-west-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                var first = provider.listChildren(StoragePath.of("s3://" + BUCKET + "/data"), 16);
                assertNotNull(first);
                fake.setHome(BUCKET, "ap-southeast-2");
                var second = provider.listChildren(StoragePath.of("s3://" + BUCKET + "/data"), 16);
                assertNotNull(second);
            }
        }
    }

    public void testExistsSelfHealsStaleRegion() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "eu-west-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
                assertTrue(provider.exists(path));
                fake.setHome(BUCKET, "ap-southeast-2");
                assertTrue(provider.exists(path));
            }
        }
    }

    public void testListObjectsSelfHealsStaleRegion() throws Exception {
        try (RegionalS3Fake fake = new RegionalS3Fake()) {
            fake.putBucket(BUCKET, "eu-west-1");
            fake.putObject(BUCKET, KEY, BODY);
            try (S3StorageProvider provider = provider(fake)) {
                StoragePath prefix = StoragePath.of("s3://" + BUCKET + "/data");
                try (StorageIterator first = provider.listObjects(prefix, true)) {
                    drain(first);
                }
                fake.setHome(BUCKET, "ap-southeast-2");
                try (StorageIterator second = provider.listObjects(prefix, true)) {
                    drain(second);
                }
            }
        }
    }

    private static void assertAsyncBody(StorageObject object) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> buffer = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        object.readBytesAsync(0, BODY.length, FACTORY, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer result) {
                buffer.set(result);
                done.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                done.countDown();
            }
        });
        assertTrue(done.await(10, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw failure.get();
        }
        try (DirectReadBuffer result = buffer.get()) {
            byte[] copied = new byte[result.buffer().remaining()];
            result.buffer().get(copied);
            assertArrayEquals(BODY, copied);
        }
    }

    private static void drain(StorageIterator iterator) {
        while (iterator.hasNext()) {
            iterator.next();
        }
    }

    private static S3StorageProvider provider(RegionalS3Fake fake) {
        S3Configuration config = S3Configuration.fromMap(Map.of("access_key", ACCESS_KEY, "secret_key", SECRET_KEY));
        return S3StorageProvider.forTestEndpoint(config, fake.endpoint());
    }
}
