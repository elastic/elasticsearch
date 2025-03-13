/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3BlobStoreContainerTests extends ESTestCase {

    public void testExecuteSingleUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.GB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeSingleUpload(randomPurpose(), blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize)
        );
        assertEquals("Upload request size [" + blobSize + "] can't be larger than 5gb", e.getMessage());
    }

    public void testExecuteSingleUploadBlobSizeLargerThanBufferSize() {
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bufferSizeInBytes()).thenReturn(ByteSizeUnit.MB.toBytes(1));

        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeSingleUpload(
                randomPurpose(),
                blobStore,
                blobName,
                new ByteArrayInputStream(new byte[0]),
                ByteSizeUnit.MB.toBytes(2)
            )
        );
        assertEquals("Upload request size [2097152] can't be larger than buffer size", e.getMessage());
    }

    public void testExecuteSingleUpload() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = BlobPath.EMPTY;
        if (randomBoolean()) {
            IntStream.of(randomIntBetween(1, 5)).forEach(value -> blobPath.add("path_" + value));
        }

        final int bufferSize = randomIntBetween(1024, 2048);
        final int blobSize = randomIntBetween(0, bufferSize);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn((long) bufferSize);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        final boolean serverSideEncryption = randomBoolean();
        when(blobStore.serverSideEncryption()).thenReturn(serverSideEncryption);

        final StorageClass storageClass = randomFrom(StorageClass.values());
        when(blobStore.getStorageClass()).thenReturn(storageClass);

        final CannedAccessControlList cannedAccessControlList = randomBoolean() ? randomFrom(CannedAccessControlList.values()) : null;
        if (cannedAccessControlList != null) {
            when(blobStore.getCannedACL()).thenReturn(cannedAccessControlList);
        }

        final AmazonS3 client = configureMockClient(blobStore);

        final ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        when(client.putObject(argumentCaptor.capture())).thenReturn(new PutObjectResult());

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[blobSize]);
        blobContainer.executeSingleUpload(randomPurpose(), blobStore, blobName, inputStream, blobSize);

        final PutObjectRequest request = argumentCaptor.getValue();
        assertEquals(bucketName, request.getBucketName());
        assertEquals(blobPath.buildAsString() + blobName, request.getKey());
        assertEquals(inputStream, request.getInputStream());
        assertEquals(blobSize, request.getMetadata().getContentLength());
        assertEquals(storageClass.toString(), request.getStorageClass());
        assertEquals(cannedAccessControlList, request.getCannedAcl());
        if (serverSideEncryption) {
            assertEquals(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION, request.getMetadata().getSSEAlgorithm());
        }
    }

    public void testExecuteMultipartUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.TB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeMultipartUpload(randomPurpose(), blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize)
        );
        assertEquals("Multipart upload request size [" + blobSize + "] can't be larger than 5tb", e.getMessage());
    }

    public void testExecuteMultipartUploadBlobSizeTooSmall() {
        final long blobSize = ByteSizeUnit.MB.toBytes(randomIntBetween(1, 4));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeMultipartUpload(randomPurpose(), blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize)
        );
        assertEquals("Multipart upload request size [" + blobSize + "] can't be smaller than 5mb", e.getMessage());
    }

    public void testExecuteMultipartUpload() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = BlobPath.EMPTY;
        if (randomBoolean()) {
            IntStream.of(randomIntBetween(1, 5)).forEach(value -> BlobPath.EMPTY.add("path_" + value));
        }

        final long blobSize = ByteSizeUnit.GB.toBytes(randomIntBetween(1, 128));
        final long bufferSize = ByteSizeUnit.MB.toBytes(randomIntBetween(5, 1024));

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);

        final boolean serverSideEncryption = randomBoolean();
        when(blobStore.serverSideEncryption()).thenReturn(serverSideEncryption);

        final StorageClass storageClass = randomFrom(StorageClass.values());
        when(blobStore.getStorageClass()).thenReturn(storageClass);

        final CannedAccessControlList cannedAccessControlList = randomBoolean() ? randomFrom(CannedAccessControlList.values()) : null;
        if (cannedAccessControlList != null) {
            when(blobStore.getCannedACL()).thenReturn(cannedAccessControlList);
        }

        final AmazonS3 client = configureMockClient(blobStore);

        final ArgumentCaptor<InitiateMultipartUploadRequest> initArgCaptor = ArgumentCaptor.forClass(InitiateMultipartUploadRequest.class);
        final InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
        initResult.setUploadId(randomAlphaOfLength(10));
        when(client.initiateMultipartUpload(initArgCaptor.capture())).thenReturn(initResult);

        final ArgumentCaptor<UploadPartRequest> uploadArgCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);

        final List<String> expectedEtags = new ArrayList<>();
        final long partSize = Math.min(bufferSize, blobSize);
        long totalBytes = 0;
        do {
            expectedEtags.add(randomAlphaOfLength(50));
            totalBytes += partSize;
        } while (totalBytes < blobSize);

        when(client.uploadPart(uploadArgCaptor.capture())).thenAnswer(invocationOnMock -> {
            final UploadPartRequest request = (UploadPartRequest) invocationOnMock.getArguments()[0];
            final UploadPartResult response = new UploadPartResult();
            response.setPartNumber(request.getPartNumber());
            response.setETag(expectedEtags.get(request.getPartNumber() - 1));
            return response;
        });

        final ArgumentCaptor<CompleteMultipartUploadRequest> compArgCaptor = ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
        when(client.completeMultipartUpload(compArgCaptor.capture())).thenReturn(new CompleteMultipartUploadResult());

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
        blobContainer.executeMultipartUpload(randomPurpose(), blobStore, blobName, inputStream, blobSize);

        final InitiateMultipartUploadRequest initRequest = initArgCaptor.getValue();
        assertEquals(bucketName, initRequest.getBucketName());
        assertEquals(blobPath.buildAsString() + blobName, initRequest.getKey());
        assertEquals(storageClass, initRequest.getStorageClass());
        assertEquals(cannedAccessControlList, initRequest.getCannedACL());
        if (serverSideEncryption) {
            assertEquals(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION, initRequest.getObjectMetadata().getSSEAlgorithm());
        }

        final Tuple<Long, Long> numberOfParts = S3BlobContainer.numberOfMultiparts(blobSize, bufferSize);

        final List<UploadPartRequest> uploadRequests = uploadArgCaptor.getAllValues();
        assertEquals(numberOfParts.v1().intValue(), uploadRequests.size());

        for (int i = 0; i < uploadRequests.size(); i++) {
            final UploadPartRequest uploadRequest = uploadRequests.get(i);

            assertEquals(bucketName, uploadRequest.getBucketName());
            assertEquals(blobPath.buildAsString() + blobName, uploadRequest.getKey());
            assertEquals(initResult.getUploadId(), uploadRequest.getUploadId());
            assertEquals(i + 1, uploadRequest.getPartNumber());
            assertEquals(inputStream, uploadRequest.getInputStream());

            if (i == (uploadRequests.size() - 1)) {
                assertTrue(uploadRequest.isLastPart());
                assertEquals(numberOfParts.v2().longValue(), uploadRequest.getPartSize());
            } else {
                assertFalse(uploadRequest.isLastPart());
                assertEquals(bufferSize, uploadRequest.getPartSize());
            }
        }

        final CompleteMultipartUploadRequest compRequest = compArgCaptor.getValue();
        assertEquals(bucketName, compRequest.getBucketName());
        assertEquals(blobPath.buildAsString() + blobName, compRequest.getKey());
        assertEquals(initResult.getUploadId(), compRequest.getUploadId());

        final List<String> actualETags = compRequest.getPartETags().stream().map(PartETag::getETag).collect(Collectors.toList());
        assertEquals(expectedEtags, actualETags);

        closeMockClient(blobStore);
    }

    public void testExecuteMultipartUploadAborted() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = BlobPath.EMPTY;

        final long blobSize = ByteSizeUnit.MB.toBytes(765);
        final long bufferSize = ByteSizeUnit.MB.toBytes(150);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);
        when(blobStore.getStorageClass()).thenReturn(randomFrom(StorageClass.values()));

        final AmazonS3 client = mock(AmazonS3.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        doAnswer(invocation -> {
            clientReference.incRef();
            return clientReference;
        }).when(blobStore).clientReference();

        final String uploadId = randomAlphaOfLength(25);

        final int stage = randomInt(2);
        final List<AmazonClientException> exceptions = Arrays.asList(
            new AmazonClientException("Expected initialization request to fail"),
            new AmazonClientException("Expected upload part request to fail"),
            new AmazonClientException("Expected completion request to fail")
        );

        if (stage == 0) {
            // Fail the initialization request
            when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenThrow(exceptions.get(stage));

        } else if (stage == 1) {
            final InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
            initResult.setUploadId(uploadId);
            when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(initResult);

            // Fail the upload part request
            when(client.uploadPart(any(UploadPartRequest.class))).thenThrow(exceptions.get(stage));

        } else {
            final InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
            initResult.setUploadId(uploadId);
            when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(initResult);

            when(client.uploadPart(any(UploadPartRequest.class))).thenAnswer(invocationOnMock -> {
                final UploadPartRequest request = (UploadPartRequest) invocationOnMock.getArguments()[0];
                final UploadPartResult response = new UploadPartResult();
                response.setPartNumber(request.getPartNumber());
                response.setETag(randomAlphaOfLength(20));
                return response;
            });

            // Fail the completion request
            when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(exceptions.get(stage));
        }

        final ArgumentCaptor<AbortMultipartUploadRequest> argumentCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        doNothing().when(client).abortMultipartUpload(argumentCaptor.capture());

        final IOException e = expectThrows(IOException.class, () -> {
            final S3BlobContainer blobContainer = new S3BlobContainer(BlobPath.EMPTY, blobStore);
            blobContainer.executeMultipartUpload(randomPurpose(), blobStore, blobName, new ByteArrayInputStream(new byte[0]), blobSize);
        });

        assertEquals("Unable to upload object [" + blobName + "] using multipart upload", e.getMessage());
        assertThat(e.getCause(), instanceOf(AmazonClientException.class));
        assertEquals(exceptions.get(stage).getMessage(), e.getCause().getMessage());

        if (stage == 0) {
            verify(client, times(1)).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
            verify(client, times(0)).uploadPart(any(UploadPartRequest.class));
            verify(client, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verify(client, times(0)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        } else {
            verify(client, times(1)).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));

            if (stage == 1) {
                verify(client, times(1)).uploadPart(any(UploadPartRequest.class));
                verify(client, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            } else {
                verify(client, times(6)).uploadPart(any(UploadPartRequest.class));
                verify(client, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            }

            verify(client, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

            final AbortMultipartUploadRequest abortRequest = argumentCaptor.getValue();
            assertEquals(bucketName, abortRequest.getBucketName());
            assertEquals(blobName, abortRequest.getKey());
            assertEquals(uploadId, abortRequest.getUploadId());
        }

        closeMockClient(blobStore);
    }

    private static AmazonS3 configureMockClient(S3BlobStore blobStore) {
        final AmazonS3 client = mock(AmazonS3.class);
        try (AmazonS3Reference clientReference = new AmazonS3Reference(client)) {
            clientReference.mustIncRef(); // held by the mock, ultimately released in closeMockClient
            when(blobStore.clientReference()).then(invocation -> {
                clientReference.mustIncRef();
                return clientReference;
            });
        }
        return client;
    }

    private static void closeMockClient(S3BlobStore blobStore) {
        final var finalClientReference = blobStore.clientReference();
        assertFalse(finalClientReference.decRef());
        assertTrue(finalClientReference.decRef());
        assertFalse(finalClientReference.hasReferences());
    }

    public void testNumberOfMultipartsWithZeroPartSize() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3BlobContainer.numberOfMultiparts(randomNonNegativeLong(), 0L)
        );
        assertEquals("Part size must be greater than zero", e.getMessage());
    }

    public void testNumberOfMultiparts() {
        final ByteSizeUnit unit = randomFrom(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB, ByteSizeUnit.GB);
        final long size = unit.toBytes(randomIntBetween(2, 1000));
        final int factor = randomIntBetween(2, 10);

        // Fits in 1 empty part
        assertNumberOfMultiparts(1, 0L, 0L, size);

        // Fits in 1 part exactly
        assertNumberOfMultiparts(1, size, size, size);
        assertNumberOfMultiparts(1, size, size, size * factor);

        // Fits in N parts exactly
        assertNumberOfMultiparts(factor, size, size * factor, size);

        // Fits in N parts plus a bit more
        final long remaining = randomIntBetween(1, (size > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) size - 1);
        assertNumberOfMultiparts(factor + 1, remaining, (size * factor) + remaining, size);
    }

    public void testInitCannedACL() {
        String[] aclList = new String[] {
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "log-delivery-write",
            "bucket-owner-read",
            "bucket-owner-full-control" };

        // empty acl
        assertThat(S3BlobStore.initCannedACL(null), equalTo(CannedAccessControlList.Private));
        assertThat(S3BlobStore.initCannedACL(""), equalTo(CannedAccessControlList.Private));

        // it should init cannedACL correctly
        for (String aclString : aclList) {
            CannedAccessControlList acl = S3BlobStore.initCannedACL(aclString);
            assertThat(acl.toString(), equalTo(aclString));
        }

        // it should accept all aws cannedACLs
        for (CannedAccessControlList awsList : CannedAccessControlList.values()) {
            CannedAccessControlList acl = S3BlobStore.initCannedACL(awsList.toString());
            assertThat(acl, equalTo(awsList));
        }
    }

    public void testInvalidCannedACL() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initCannedACL("test_invalid"));
        assertThat(ex.getMessage(), equalTo("cannedACL is not valid: [test_invalid]"));
    }

    public void testInitStorageClass() {
        // it should default to `standard`
        assertThat(S3BlobStore.initStorageClass(null), equalTo(StorageClass.Standard));
        assertThat(S3BlobStore.initStorageClass(""), equalTo(StorageClass.Standard));

        // it should accept [standard, standard_ia, onezone_ia, reduced_redundancy, intelligent_tiering]
        assertThat(S3BlobStore.initStorageClass("standard"), equalTo(StorageClass.Standard));
        assertThat(S3BlobStore.initStorageClass("standard_ia"), equalTo(StorageClass.StandardInfrequentAccess));
        assertThat(S3BlobStore.initStorageClass("onezone_ia"), equalTo(StorageClass.OneZoneInfrequentAccess));
        assertThat(S3BlobStore.initStorageClass("reduced_redundancy"), equalTo(StorageClass.ReducedRedundancy));
        assertThat(S3BlobStore.initStorageClass("intelligent_tiering"), equalTo(StorageClass.IntelligentTiering));
    }

    public void testCaseInsensitiveStorageClass() {
        assertThat(S3BlobStore.initStorageClass("sTandaRd"), equalTo(StorageClass.Standard));
        assertThat(S3BlobStore.initStorageClass("sTandaRd_Ia"), equalTo(StorageClass.StandardInfrequentAccess));
        assertThat(S3BlobStore.initStorageClass("oNeZoNe_iA"), equalTo(StorageClass.OneZoneInfrequentAccess));
        assertThat(S3BlobStore.initStorageClass("reduCED_redundancy"), equalTo(StorageClass.ReducedRedundancy));
        assertThat(S3BlobStore.initStorageClass("intelLigeNt_tieriNG"), equalTo(StorageClass.IntelligentTiering));
    }

    public void testInvalidStorageClass() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initStorageClass("whatever"));
        assertThat(ex.getMessage(), equalTo("`whatever` is not a valid S3 Storage Class."));
    }

    public void testRejectGlacierStorageClass() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initStorageClass("glacier"));
        assertThat(ex.getMessage(), equalTo("Glacier storage class is not supported"));
    }

    private static void assertNumberOfMultiparts(final int expectedParts, final long expectedRemaining, long totalSize, long partSize) {
        final Tuple<Long, Long> result = S3BlobContainer.numberOfMultiparts(totalSize, partSize);

        assertEquals("Expected number of parts [" + expectedParts + "] but got [" + result.v1() + "]", expectedParts, (long) result.v1());
        assertEquals("Expected remaining [" + expectedRemaining + "] but got [" + result.v2() + "]", expectedRemaining, (long) result.v2());
    }
}
