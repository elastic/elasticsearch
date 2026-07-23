/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.TestUtils;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestStatus;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.io.Streams.readFully;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomRetryingPurpose;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.MEGABYTES_COPIED_PER_CHUNK_SETTING;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class GoogleCloudStorageThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.google.fixture", "true"));

    @ClassRule
    public static GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(USE_FIXTURE, "bucket", "o/oauth2/token");

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());

        if (USE_FIXTURE) {
            builder.put("gcs.client.default.endpoint", fixture.getAddress());
            builder.put("gcs.client.default.token_uri", fixture.getAddress() + "/o/oauth2/token");
        }
        builder.put(MEGABYTES_COPIED_PER_CHUNK_SETTING.getConcreteSettingForNamespace("default").getKey(), 1);

        return builder.build();
    }

    @Override
    protected SecureSettings credentials() {
        if (USE_FIXTURE == false) {
            assertThat(System.getProperty("test.google.account"), not(blankOrNullString()));
        }
        assertThat(System.getProperty("test.google.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        if (USE_FIXTURE) {
            secureSettings.setFile("gcs.client.default.credentials_file", TestUtils.createServiceAccount(random()));
        } else {
            secureSettings.setFile(
                "gcs.client.default.credentials_file",
                Base64.getDecoder().decode(System.getProperty("test.google.account"))
            );
        }
        return secureSettings;
    }

    @Override
    protected void createRepository(final String repoName) {
        AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            repoName
        )
            .setType("gcs")
            .setSettings(
                Settings.builder()
                    .put("bucket", System.getProperty("test.google.bucket"))
                    .put("base_path", System.getProperty("test.google.base", "/"))
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    public void testReadFromPositionLargerThanBlobLength() {
        testReadFromPositionLargerThanBlobLength(
            e -> asInstanceOf(StorageException.class, e.getCause()).getCode() == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()
        );
    }

    public void testResumeAfterUpdate() {

        // The blob needs to be large enough that it won't be entirely buffered on the first request
        final int enoughBytesToNotBeEntirelyBuffered = Math.toIntExact(ByteSizeValue.ofMb(5).getBytes());

        final BlobStoreRepository repo = getRepository();
        final String blobKey = randomIdentifier();
        final byte[] initialValue = randomByteArrayOfLength(enoughBytesToNotBeEntirelyBuffered);
        executeOnBlobStore(repo, container -> {
            container.writeBlob(randomPurpose(), blobKey, new BytesArray(initialValue), true);

            try (InputStream inputStream = container.readBlob(randomRetryingPurpose(), blobKey)) {
                // Trigger the first request for the blob, partially read it
                int read = inputStream.read();
                assert read != -1;

                // Close the current underlying stream (this will force a resume)
                asInstanceOf(GoogleCloudStorageRetryingInputStream.class, inputStream).closeCurrentStream();

                // Update the file
                byte[] updatedValue = randomByteArrayOfLength(enoughBytesToNotBeEntirelyBuffered);
                container.writeBlob(randomPurpose(), blobKey, new BytesArray(updatedValue), false);

                // Read the rest of the stream, it should throw because the contents changed
                String message = assertThrows(NoSuchFileException.class, () -> readFully(inputStream)).getMessage();
                assertThat(message, containsString("unavailable on resume (contents changed, or object deleted):"));
            } catch (Exception e) {
                fail(e);
            }
            return null;
        });
    }

    public void testCopy() {
        final var sourceBlobName = randomIdentifier();
        final var blobBytes = randomBytesReference(randomIntBetween(100, 2_000_000));
        final var destinationBlobName = randomIdentifier();
        final var repository = getRepository();
        final var targetBytes = executeOnBlobStore(repository, sourceBlobContainer -> {
            sourceBlobContainer.writeBlob(randomPurpose(), sourceBlobName, blobBytes, true);
            try {
                assertBusy(() -> assertTrue(sourceBlobContainer.blobExists(randomPurpose(), sourceBlobName)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            final var destinationBlobContainer = repository.blobStore().blobContainer(repository.basePath().add("target"));
            destinationBlobContainer.copyBlob(
                randomPurpose(),
                sourceBlobContainer,
                sourceBlobName,
                destinationBlobName,
                blobBytes.length()
            );
            return destinationBlobContainer.readBlob(randomPurpose(), destinationBlobName).readAllBytes();
        });
        assertArrayEquals(BytesReference.toBytes(blobBytes), targetBytes);
    }

    /**
     * Verifies that the {@code data_storage_class} / {@code metadata_storage_class} settings are applied to the correct uploads. We
     * configure two distinct storage classes, write blobs with each {@link OperationPurpose} via the single-part, resumable and
     * server-side copy paths, and read the storage class back from the object metadata.
     * <p>
     * One of the two configured classes is always {@code STANDARD} (the default class) or {@code REGIONAL} (the legacy default class
     * used by the test bucket elasticsearch-ci-thirdparty) and the other is a colder class ({@code NEARLINE} or {@code COLDLINE}),
     * so the positive assertions cover both an explicit default and an explicit non-default class.
     * <p>
     * Unlike Azure (which exposes an "inferred" flag), GCS always reports a concrete storage class for an object: the fixture reports no
     * class when none was configured, while a real bucket falls back to its default class (test buckets default to {@code STANDARD} or
     * {@code REGIONAL}). The non-snapshot purpose therefore asserts only that no colder class leaked onto the upload.
     */
    public void testStorageClassPerOperationPurpose() throws Exception {
        // always configure STANDARD plus one colder class so the positive assertions exercise both the default and a non-default class
        final List<StorageClass> snapshotStorageClasses = shuffledList(
            List.of(StorageClass.STANDARD, randomFrom(StorageClass.NEARLINE, StorageClass.COLDLINE))
        );
        final StorageClass dataStorageClass = snapshotStorageClasses.get(0);
        final StorageClass metadataStorageClass = snapshotStorageClasses.get(1);
        logger.info("--> data_storage_class [{}], metadata_storage_class [{}]", dataStorageClass, metadataStorageClass);

        final String repoName = "test-storage-class-repo";
        final AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            repoName
        )
            .setType("gcs")
            .setSettings(
                Settings.builder()
                    .put("bucket", System.getProperty("test.google.bucket"))
                    .put("base_path", System.getProperty("test.google.base", "/") + randomAlphaOfLength(8))
                    .put(GoogleCloudStorageRepository.DATA_STORAGE_CLASS.getKey(), dataStorageClass.toString())
                    .put(GoogleCloudStorageRepository.METADATA_STORAGE_CLASS.getKey(), metadataStorageClass.toString())
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        try {
            final BlobStoreRepository repository = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(
                repoName
            );
            final GoogleCloudStorageBlobStore blobStore = asInstanceOf(GoogleCloudStorageBlobStore.class, repository.blobStore());
            final BlobContainer blobContainer = blobStore.blobContainer(repository.basePath());
            final String bucket = System.getProperty("test.google.bucket");
            final String keyPrefix = repository.basePath().buildAsString();
            try {
                // a purpose that resolves to no explicit storage class (i.e. neither SNAPSHOT_DATA nor SNAPSHOT_METADATA)
                final OperationPurpose noClassPurpose = randomFrom(
                    OperationPurpose.REPOSITORY_ANALYSIS,
                    OperationPurpose.CLUSTER_STATE,
                    OperationPurpose.INDICES,
                    OperationPurpose.TRANSLOG
                );
                for (final OperationPurpose purpose : List.of(
                    OperationPurpose.SNAPSHOT_DATA,
                    OperationPurpose.SNAPSHOT_METADATA,
                    noClassPurpose
                )) {
                    final StorageClass expectedStorageClass = blobStore.resolveStorageClass(purpose);

                    // single-part upload (blob stays below the large blob threshold, so the multipart upload path is used)
                    final String singlePartName = randomIdentifier();
                    final BytesReference singlePartBytes = randomBytesReference(between(1, 512));
                    blobContainer.writeBlob(purpose, singlePartName, singlePartBytes, true);
                    assertStorageClass(blobStore, bucket, keyPrefix + singlePartName, expectedStorageClass, "single-part upload", purpose);

                    // resumable upload (exceeds the large blob threshold so the resumable upload path is used)
                    final String resumableName = randomIdentifier();
                    final int resumableSize = GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE + between(1, 1024);
                    final byte[] resumableBytes = randomByteArrayOfLength(resumableSize);
                    blobContainer.writeBlob(purpose, resumableName, new ByteArrayInputStream(resumableBytes), resumableSize, true);
                    assertStorageClass(blobStore, bucket, keyPrefix + resumableName, expectedStorageClass, "resumable upload", purpose);

                    // server-side copy (source is the small single-part blob written above)
                    final String copyName = randomIdentifier();
                    blobContainer.copyBlob(purpose, blobContainer, singlePartName, copyName, singlePartBytes.length());
                    assertStorageClass(blobStore, bucket, keyPrefix + copyName, expectedStorageClass, "server-side copy", purpose);
                }
            } finally {
                blobContainer.delete(randomPurpose());
            }
        } finally {
            clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).get();
        }
    }

    private static void assertStorageClass(
        GoogleCloudStorageBlobStore blobStore,
        String bucket,
        String blobKey,
        @Nullable StorageClass expectedStorageClass,
        String uploadType,
        OperationPurpose purpose
    ) throws IOException {
        final StorageClass actualStorageClass = blobStore.client().meteredGet(purpose, BlobId.of(bucket, blobKey)).getStorageClass();
        final String message = uploadType + " with purpose [" + purpose + "]";
        if (expectedStorageClass != null) {
            assertThat(message, actualStorageClass, equalTo(expectedStorageClass));
        } else {
            // No storage class configured for this purpose: the fixture reports no class, while a real bucket reports its default class
            // (test buckets default to STANDARD). Either way, no colder class should have been applied to the blob.
            assertThat(
                message + " should not carry a colder storage class",
                actualStorageClass,
                anyOf(nullValue(), equalTo(StorageClass.STANDARD), equalTo(StorageClass.REGIONAL))
            );
        }
    }
}
