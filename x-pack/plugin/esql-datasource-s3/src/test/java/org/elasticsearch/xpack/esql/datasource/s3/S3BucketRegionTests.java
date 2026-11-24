/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

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
                ExternalClientException thrown = expectThrows(ExternalClientException.class, object::newStream);
                assertThat(thrown.getMessage(), containsString("set region on the dataset"));
                assertThat(thrown.getMessage(), containsString("ca-central-1"));
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
                CountDownLatch done = new CountDownLatch(1);
                AtomicReference<DirectReadBuffer> buffer = new AtomicReference<>();
                AtomicReference<Exception> failure = new AtomicReference<>();
                provider.newObject(path, BODY.length).readBytesAsync(0, BODY.length, FACTORY, Runnable::run, new ActionListener<>() {
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
                assertEquals(0, fake.wrongRegionGets(BUCKET));
            }
        }
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

    private static S3StorageProvider provider(RegionalS3Fake fake) {
        S3Configuration config = S3Configuration.fromMap(Map.of("access_key", ACCESS_KEY, "secret_key", SECRET_KEY));
        return S3StorageProvider.forTestEndpoint(config, fake.endpoint());
    }
}
