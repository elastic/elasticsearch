/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.fixtures.minio.MinioTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class S3RepositoryThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {
    static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("tests.use.fixture", "true"));

    @ClassRule
    public static MinioTestContainer minio = new MinioTestContainer(USE_FIXTURE);

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(S3RepositoryPlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.s3.account"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.key"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", System.getProperty("test.s3.account"));
        secureSettings.setString("s3.client.default.secret_key", System.getProperty("test.s3.key"));
        return secureSettings;
    }

    @Override
    protected Settings nodeSettings() {
        final var settings = Settings.builder().put(super.nodeSettings());
        if (randomBoolean()) {
            final var defaultMillis = S3Service.REPOSITORY_S3_CAS_TTL_SETTING.get(Settings.EMPTY).millis();
            settings.put(
                S3Service.REPOSITORY_S3_CAS_TTL_SETTING.getKey(),
                TimeValue.timeValueMillis(randomLongBetween(defaultMillis, defaultMillis * 2))
            );
        }
        if (randomBoolean()) {
            final var defaultMillis = S3Service.REPOSITORY_S3_CAS_ANTI_CONTENTION_DELAY_SETTING.get(Settings.EMPTY).millis();
            settings.put(
                S3Service.REPOSITORY_S3_CAS_ANTI_CONTENTION_DELAY_SETTING.getKey(),
                TimeValue.timeValueMillis(randomLongBetween(defaultMillis, defaultMillis * 2))
            );
        }
        return settings.build();
    }

    @Override
    protected void createRepository(String repoName) {
        Settings.Builder settings = Settings.builder()
            .put("bucket", System.getProperty("test.s3.bucket"))
            .put("base_path", System.getProperty("test.s3.base", "testpath"));
        final String endpoint = USE_FIXTURE ? minio.getAddress() : System.getProperty("test.s3.endpoint");
        if (endpoint != null) {
            settings.put("endpoint", endpoint);
        } else {
            // only test different storage classes when running against the default endpoint, i.e. a genuine S3 service
            if (randomBoolean()) {
                final String storageClass = randomFrom(
                    "standard",
                    "reduced_redundancy",
                    "standard_ia",
                    "onezone_ia",
                    "intelligent_tiering"
                );
                logger.info("--> using storage_class [{}]", storageClass);
                settings.put("storage_class", storageClass);
            }
        }
        AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            repoName
        ).setType("s3").setSettings(settings).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    public void testCompareAndExchangeCleanup() throws IOException {
        final var timeOffsetMillis = new AtomicLong();
        final var threadpool = new TestThreadPool(getTestName()) {
            @Override
            public long absoluteTimeInMillis() {
                return super.absoluteTimeInMillis() + timeOffsetMillis.get();
            }
        };
        // construct our own repo instance so we can inject a threadpool that allows to control the passage of time
        try (
            var repository = new S3Repository(
                node().injector().getInstance(RepositoriesService.class).repository(TEST_REPO_NAME).getMetadata(),
                xContentRegistry(),
                node().injector().getInstance(PluginsService.class).filterPlugins(S3RepositoryPlugin.class).findFirst().get().getService(),
                ClusterServiceUtils.createClusterService(threadpool),
                BigArrays.NON_RECYCLING_INSTANCE,
                new RecoverySettings(node().settings(), node().injector().getInstance(ClusterService.class).getClusterSettings()),
                S3RepositoriesMetrics.NOOP
            )
        ) {
            repository.start();

            final var blobStore = (S3BlobStore) repository.blobStore();
            final var blobContainer = (S3BlobContainer) blobStore.blobContainer(repository.basePath().add(getTestName()));

            try (var clientReference = blobStore.clientReference()) {
                final var client = clientReference.client();
                final var bucketName = S3Repository.BUCKET_SETTING.get(repository.getMetadata().settings());
                final var registerBlobPath = blobContainer.buildKey("key");

                class TestHarness {
                    boolean tryCompareAndSet(BytesReference expected, BytesReference updated) {
                        return safeAwait(l -> blobContainer.compareAndSetRegister(randomPurpose(), "key", expected, updated, l));
                    }

                    BytesReference readRegister() {
                        final OptionalBytesReference result = safeAwait(l -> blobContainer.getRegister(randomPurpose(), "key", l));
                        return result.bytesReference();
                    }

                    List<MultipartUpload> listMultipartUploads() {
                        return client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withPrefix(registerBlobPath))
                            .getMultipartUploads();
                    }
                }

                var testHarness = new TestHarness();

                final var bytes1 = new BytesArray(new byte[] { (byte) 1 });
                final var bytes2 = new BytesArray(new byte[] { (byte) 2 });
                assertTrue(testHarness.tryCompareAndSet(BytesArray.EMPTY, bytes1));

                // show we're looking at the right blob
                assertEquals(bytes1, testHarness.readRegister());
                assertArrayEquals(
                    bytes1.array(),
                    client.getObject(new GetObjectRequest(bucketName, registerBlobPath)).getObjectContent().readAllBytes()
                );

                // a fresh ongoing upload blocks other CAS attempts
                client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, registerBlobPath));
                assertThat(testHarness.listMultipartUploads(), hasSize(1));

                assertFalse(testHarness.tryCompareAndSet(bytes1, bytes2));
                final var multipartUploads = testHarness.listMultipartUploads();
                assertThat(multipartUploads, hasSize(1));

                // repo clock may not be exactly aligned with ours, but it should be close
                final var age = blobStore.getThreadPool().absoluteTimeInMillis() - multipartUploads.get(0)
                    .getInitiated()
                    .toInstant()
                    .toEpochMilli();
                final var ageRangeMillis = TimeValue.timeValueMinutes(1).millis();
                assertThat(age, allOf(greaterThanOrEqualTo(-ageRangeMillis), lessThanOrEqualTo(ageRangeMillis)));

                // if the upload exceeds the TTL then CAS attempts will abort it
                timeOffsetMillis.addAndGet(blobStore.getCompareAndExchangeTimeToLive().millis() - Math.min(0, age));
                assertTrue(testHarness.tryCompareAndSet(bytes1, bytes2));
                assertThat(testHarness.listMultipartUploads(), hasSize(0));
                assertEquals(bytes2, testHarness.readRegister());
            } finally {
                blobContainer.delete(randomPurpose());
            }
        } finally {
            ThreadPool.terminate(threadpool, 10, TimeUnit.SECONDS);
        }
    }

    public void testReadFromPositionLargerThanBlobLength() {
        testReadFromPositionLargerThanBlobLength(
            e -> asInstanceOf(AmazonS3Exception.class, e.getCause()).getStatusCode() == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()
        );
    }
}
