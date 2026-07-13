/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketMetadataTableConfigurationRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketMetadataTableConfigurationResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateSessionRequest;
import software.amazon.awssdk.services.s3.model.CreateSessionResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketAnalyticsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketAnalyticsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketIntelligentTieringConfigurationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketIntelligentTieringConfigurationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketInventoryConfigurationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketInventoryConfigurationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketMetadataTableConfigurationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketMetadataTableConfigurationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketMetricsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketMetricsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketOwnershipControlsRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketOwnershipControlsResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketReplicationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketReplicationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketTaggingResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletePublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.DeletePublicAccessBlockResponse;
import software.amazon.awssdk.services.s3.model.EncryptionTypeMismatchException;
import software.amazon.awssdk.services.s3.model.GetBucketAccelerateConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAccelerateConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAnalyticsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAnalyticsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.GetBucketIntelligentTieringConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketIntelligentTieringConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketInventoryConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketInventoryConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLoggingRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLoggingResponse;
import software.amazon.awssdk.services.s3.model.GetBucketMetadataTableConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketMetadataTableConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketMetricsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketMetricsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketNotificationConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketOwnershipControlsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketOwnershipControlsResponse;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyStatusRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyStatusResponse;
import software.amazon.awssdk.services.s3.model.GetBucketReplicationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketReplicationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketRequestPaymentRequest;
import software.amazon.awssdk.services.s3.model.GetBucketRequestPaymentResponse;
import software.amazon.awssdk.services.s3.model.GetBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetBucketTaggingResponse;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.GetBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.GetBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldResponse;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTorrentRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTorrentResponse;
import software.amazon.awssdk.services.s3.model.GetPublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.GetPublicAccessBlockResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.InvalidRequestException;
import software.amazon.awssdk.services.s3.model.InvalidWriteOffsetException;
import software.amazon.awssdk.services.s3.model.ListBucketAnalyticsConfigurationsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketAnalyticsConfigurationsResponse;
import software.amazon.awssdk.services.s3.model.ListBucketIntelligentTieringConfigurationsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketIntelligentTieringConfigurationsResponse;
import software.amazon.awssdk.services.s3.model.ListBucketInventoryConfigurationsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketInventoryConfigurationsResponse;
import software.amazon.awssdk.services.s3.model.ListBucketMetricsConfigurationsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketMetricsConfigurationsResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListDirectoryBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListDirectoryBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.ObjectAlreadyInActiveTierErrorException;
import software.amazon.awssdk.services.s3.model.ObjectNotInActiveTierErrorException;
import software.amazon.awssdk.services.s3.model.PutBucketAccelerateConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketAccelerateConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketAclRequest;
import software.amazon.awssdk.services.s3.model.PutBucketAclResponse;
import software.amazon.awssdk.services.s3.model.PutBucketAnalyticsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketAnalyticsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.PutBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.PutBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.PutBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.PutBucketIntelligentTieringConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketIntelligentTieringConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketInventoryConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketInventoryConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketLoggingRequest;
import software.amazon.awssdk.services.s3.model.PutBucketLoggingResponse;
import software.amazon.awssdk.services.s3.model.PutBucketMetricsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketMetricsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketOwnershipControlsRequest;
import software.amazon.awssdk.services.s3.model.PutBucketOwnershipControlsResponse;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.PutBucketReplicationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketReplicationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketRequestPaymentRequest;
import software.amazon.awssdk.services.s3.model.PutBucketRequestPaymentResponse;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingResponse;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.PutBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.PutBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectAclResponse;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldResponse;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.PutPublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.PutPublicAccessBlockResponse;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.RestoreObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.TooManyPartsException;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.WriteGetObjectResponseRequest;
import software.amazon.awssdk.services.s3.model.WriteGetObjectResponseResponse;
import software.amazon.awssdk.services.s3.paginators.ListBucketsIterable;
import software.amazon.awssdk.services.s3.paginators.ListDirectoryBucketsIterable;
import software.amazon.awssdk.services.s3.paginators.ListMultipartUploadsIterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.paginators.ListPartsIterable;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import org.elasticsearch.core.SuppressForbidden;

import java.nio.file.Path;
import java.util.function.Consumer;

@SuppressForbidden(reason = "implements AWS api that uses java.io.File!")
public class AmazonS3Wrapper implements S3Client {

    protected S3Client delegate;

    public AmazonS3Wrapper(S3Client delegate) {
        this.delegate = delegate;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public String serviceName() {
        return "AmazonS3Wrapper";
    }

    @Override
    public AbortMultipartUploadResponse abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest)
        throws NoSuchUploadException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.abortMultipartUpload(abortMultipartUploadRequest);
    }

    @Override
    public AbortMultipartUploadResponse abortMultipartUpload(Consumer<AbortMultipartUploadRequest.Builder> abortMultipartUploadRequest)
        throws NoSuchUploadException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.abortMultipartUpload(abortMultipartUploadRequest);
    }

    @Override
    public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.completeMultipartUpload(completeMultipartUploadRequest);
    }

    @Override
    public CompleteMultipartUploadResponse completeMultipartUpload(
        Consumer<CompleteMultipartUploadRequest.Builder> completeMultipartUploadRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.completeMultipartUpload(completeMultipartUploadRequest);
    }

    @Override
    public CopyObjectResponse copyObject(CopyObjectRequest copyObjectRequest) throws ObjectNotInActiveTierErrorException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.copyObject(copyObjectRequest);
    }

    @Override
    public CopyObjectResponse copyObject(Consumer<CopyObjectRequest.Builder> copyObjectRequest) throws ObjectNotInActiveTierErrorException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.copyObject(copyObjectRequest);
    }

    @Override
    public CreateBucketResponse createBucket(CreateBucketRequest createBucketRequest) throws BucketAlreadyExistsException,
        BucketAlreadyOwnedByYouException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.createBucket(createBucketRequest);
    }

    @Override
    public CreateBucketResponse createBucket(Consumer<CreateBucketRequest.Builder> createBucketRequest) throws BucketAlreadyExistsException,
        BucketAlreadyOwnedByYouException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.createBucket(createBucketRequest);
    }

    @Override
    public CreateBucketMetadataTableConfigurationResponse createBucketMetadataTableConfiguration(
        CreateBucketMetadataTableConfigurationRequest createBucketMetadataTableConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.createBucketMetadataTableConfiguration(createBucketMetadataTableConfigurationRequest);
    }

    @Override
    public CreateBucketMetadataTableConfigurationResponse createBucketMetadataTableConfiguration(
        Consumer<CreateBucketMetadataTableConfigurationRequest.Builder> createBucketMetadataTableConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.createBucketMetadataTableConfiguration(createBucketMetadataTableConfigurationRequest);
    }

    @Override
    public CreateMultipartUploadResponse createMultipartUpload(CreateMultipartUploadRequest createMultipartUploadRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.createMultipartUpload(createMultipartUploadRequest);
    }

    @Override
    public CreateMultipartUploadResponse createMultipartUpload(Consumer<CreateMultipartUploadRequest.Builder> createMultipartUploadRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.createMultipartUpload(createMultipartUploadRequest);
    }

    @Override
    public CreateSessionResponse createSession(CreateSessionRequest createSessionRequest) throws NoSuchBucketException, AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.createSession(createSessionRequest);
    }

    @Override
    public CreateSessionResponse createSession(Consumer<CreateSessionRequest.Builder> createSessionRequest) throws NoSuchBucketException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.createSession(createSessionRequest);
    }

    @Override
    public DeleteBucketResponse deleteBucket(DeleteBucketRequest deleteBucketRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.deleteBucket(deleteBucketRequest);
    }

    @Override
    public DeleteBucketResponse deleteBucket(Consumer<DeleteBucketRequest.Builder> deleteBucketRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.deleteBucket(deleteBucketRequest);
    }

    @Override
    public DeleteBucketAnalyticsConfigurationResponse deleteBucketAnalyticsConfiguration(
        DeleteBucketAnalyticsConfigurationRequest deleteBucketAnalyticsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketAnalyticsConfiguration(deleteBucketAnalyticsConfigurationRequest);
    }

    @Override
    public DeleteBucketAnalyticsConfigurationResponse deleteBucketAnalyticsConfiguration(
        Consumer<DeleteBucketAnalyticsConfigurationRequest.Builder> deleteBucketAnalyticsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketAnalyticsConfiguration(deleteBucketAnalyticsConfigurationRequest);
    }

    @Override
    public DeleteBucketCorsResponse deleteBucketCors(DeleteBucketCorsRequest deleteBucketCorsRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.deleteBucketCors(deleteBucketCorsRequest);
    }

    @Override
    public DeleteBucketCorsResponse deleteBucketCors(Consumer<DeleteBucketCorsRequest.Builder> deleteBucketCorsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketCors(deleteBucketCorsRequest);
    }

    @Override
    public DeleteBucketEncryptionResponse deleteBucketEncryption(DeleteBucketEncryptionRequest deleteBucketEncryptionRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketEncryption(deleteBucketEncryptionRequest);
    }

    @Override
    public DeleteBucketEncryptionResponse deleteBucketEncryption(
        Consumer<DeleteBucketEncryptionRequest.Builder> deleteBucketEncryptionRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketEncryption(deleteBucketEncryptionRequest);
    }

    @Override
    public DeleteBucketIntelligentTieringConfigurationResponse deleteBucketIntelligentTieringConfiguration(
        DeleteBucketIntelligentTieringConfigurationRequest deleteBucketIntelligentTieringConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketIntelligentTieringConfiguration(deleteBucketIntelligentTieringConfigurationRequest);
    }

    @Override
    public DeleteBucketIntelligentTieringConfigurationResponse deleteBucketIntelligentTieringConfiguration(
        Consumer<DeleteBucketIntelligentTieringConfigurationRequest.Builder> deleteBucketIntelligentTieringConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketIntelligentTieringConfiguration(deleteBucketIntelligentTieringConfigurationRequest);
    }

    @Override
    public DeleteBucketInventoryConfigurationResponse deleteBucketInventoryConfiguration(
        DeleteBucketInventoryConfigurationRequest deleteBucketInventoryConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketInventoryConfiguration(deleteBucketInventoryConfigurationRequest);
    }

    @Override
    public DeleteBucketInventoryConfigurationResponse deleteBucketInventoryConfiguration(
        Consumer<DeleteBucketInventoryConfigurationRequest.Builder> deleteBucketInventoryConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketInventoryConfiguration(deleteBucketInventoryConfigurationRequest);
    }

    @Override
    public DeleteBucketLifecycleResponse deleteBucketLifecycle(DeleteBucketLifecycleRequest deleteBucketLifecycleRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketLifecycle(deleteBucketLifecycleRequest);
    }

    @Override
    public DeleteBucketLifecycleResponse deleteBucketLifecycle(Consumer<DeleteBucketLifecycleRequest.Builder> deleteBucketLifecycleRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketLifecycle(deleteBucketLifecycleRequest);
    }

    @Override
    public DeleteBucketMetadataTableConfigurationResponse deleteBucketMetadataTableConfiguration(
        DeleteBucketMetadataTableConfigurationRequest deleteBucketMetadataTableConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketMetadataTableConfiguration(deleteBucketMetadataTableConfigurationRequest);
    }

    @Override
    public DeleteBucketMetadataTableConfigurationResponse deleteBucketMetadataTableConfiguration(
        Consumer<DeleteBucketMetadataTableConfigurationRequest.Builder> deleteBucketMetadataTableConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketMetadataTableConfiguration(deleteBucketMetadataTableConfigurationRequest);
    }

    @Override
    public DeleteBucketMetricsConfigurationResponse deleteBucketMetricsConfiguration(
        DeleteBucketMetricsConfigurationRequest deleteBucketMetricsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketMetricsConfiguration(deleteBucketMetricsConfigurationRequest);
    }

    @Override
    public DeleteBucketMetricsConfigurationResponse deleteBucketMetricsConfiguration(
        Consumer<DeleteBucketMetricsConfigurationRequest.Builder> deleteBucketMetricsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketMetricsConfiguration(deleteBucketMetricsConfigurationRequest);
    }

    @Override
    public DeleteBucketOwnershipControlsResponse deleteBucketOwnershipControls(
        DeleteBucketOwnershipControlsRequest deleteBucketOwnershipControlsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketOwnershipControls(deleteBucketOwnershipControlsRequest);
    }

    @Override
    public DeleteBucketOwnershipControlsResponse deleteBucketOwnershipControls(
        Consumer<DeleteBucketOwnershipControlsRequest.Builder> deleteBucketOwnershipControlsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketOwnershipControls(deleteBucketOwnershipControlsRequest);
    }

    @Override
    public DeleteBucketPolicyResponse deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.deleteBucketPolicy(deleteBucketPolicyRequest);
    }

    @Override
    public DeleteBucketPolicyResponse deleteBucketPolicy(Consumer<DeleteBucketPolicyRequest.Builder> deleteBucketPolicyRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketPolicy(deleteBucketPolicyRequest);
    }

    @Override
    public DeleteBucketReplicationResponse deleteBucketReplication(DeleteBucketReplicationRequest deleteBucketReplicationRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketReplication(deleteBucketReplicationRequest);
    }

    @Override
    public DeleteBucketReplicationResponse deleteBucketReplication(
        Consumer<DeleteBucketReplicationRequest.Builder> deleteBucketReplicationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketReplication(deleteBucketReplicationRequest);
    }

    @Override
    public DeleteBucketTaggingResponse deleteBucketTagging(DeleteBucketTaggingRequest deleteBucketTaggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketTagging(deleteBucketTaggingRequest);
    }

    @Override
    public DeleteBucketTaggingResponse deleteBucketTagging(Consumer<DeleteBucketTaggingRequest.Builder> deleteBucketTaggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketTagging(deleteBucketTaggingRequest);
    }

    @Override
    public DeleteBucketWebsiteResponse deleteBucketWebsite(DeleteBucketWebsiteRequest deleteBucketWebsiteRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketWebsite(deleteBucketWebsiteRequest);
    }

    @Override
    public DeleteBucketWebsiteResponse deleteBucketWebsite(Consumer<DeleteBucketWebsiteRequest.Builder> deleteBucketWebsiteRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteBucketWebsite(deleteBucketWebsiteRequest);
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.deleteObject(deleteObjectRequest);
    }

    @Override
    public DeleteObjectResponse deleteObject(Consumer<DeleteObjectRequest.Builder> deleteObjectRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.deleteObject(deleteObjectRequest);
    }

    @Override
    public DeleteObjectTaggingResponse deleteObjectTagging(DeleteObjectTaggingRequest deleteObjectTaggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteObjectTagging(deleteObjectTaggingRequest);
    }

    @Override
    public DeleteObjectTaggingResponse deleteObjectTagging(Consumer<DeleteObjectTaggingRequest.Builder> deleteObjectTaggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deleteObjectTagging(deleteObjectTaggingRequest);
    }

    @Override
    public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.deleteObjects(deleteObjectsRequest);
    }

    @Override
    public DeleteObjectsResponse deleteObjects(Consumer<DeleteObjectsRequest.Builder> deleteObjectsRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.deleteObjects(deleteObjectsRequest);
    }

    @Override
    public DeletePublicAccessBlockResponse deletePublicAccessBlock(DeletePublicAccessBlockRequest deletePublicAccessBlockRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deletePublicAccessBlock(deletePublicAccessBlockRequest);
    }

    @Override
    public DeletePublicAccessBlockResponse deletePublicAccessBlock(
        Consumer<DeletePublicAccessBlockRequest.Builder> deletePublicAccessBlockRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.deletePublicAccessBlock(deletePublicAccessBlockRequest);
    }

    @Override
    public GetBucketAccelerateConfigurationResponse getBucketAccelerateConfiguration(
        GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketAccelerateConfiguration(getBucketAccelerateConfigurationRequest);
    }

    @Override
    public GetBucketAccelerateConfigurationResponse getBucketAccelerateConfiguration(
        Consumer<GetBucketAccelerateConfigurationRequest.Builder> getBucketAccelerateConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketAccelerateConfiguration(getBucketAccelerateConfigurationRequest);
    }

    @Override
    public GetBucketAclResponse getBucketAcl(GetBucketAclRequest getBucketAclRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.getBucketAcl(getBucketAclRequest);
    }

    @Override
    public GetBucketAclResponse getBucketAcl(Consumer<GetBucketAclRequest.Builder> getBucketAclRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getBucketAcl(getBucketAclRequest);
    }

    @Override
    public GetBucketAnalyticsConfigurationResponse getBucketAnalyticsConfiguration(
        GetBucketAnalyticsConfigurationRequest getBucketAnalyticsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketAnalyticsConfiguration(getBucketAnalyticsConfigurationRequest);
    }

    @Override
    public GetBucketAnalyticsConfigurationResponse getBucketAnalyticsConfiguration(
        Consumer<GetBucketAnalyticsConfigurationRequest.Builder> getBucketAnalyticsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketAnalyticsConfiguration(getBucketAnalyticsConfigurationRequest);
    }

    @Override
    public GetBucketCorsResponse getBucketCors(GetBucketCorsRequest getBucketCorsRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.getBucketCors(getBucketCorsRequest);
    }

    @Override
    public GetBucketCorsResponse getBucketCors(Consumer<GetBucketCorsRequest.Builder> getBucketCorsRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getBucketCors(getBucketCorsRequest);
    }

    @Override
    public GetBucketEncryptionResponse getBucketEncryption(GetBucketEncryptionRequest getBucketEncryptionRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketEncryption(getBucketEncryptionRequest);
    }

    @Override
    public GetBucketEncryptionResponse getBucketEncryption(Consumer<GetBucketEncryptionRequest.Builder> getBucketEncryptionRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketEncryption(getBucketEncryptionRequest);
    }

    @Override
    public GetBucketIntelligentTieringConfigurationResponse getBucketIntelligentTieringConfiguration(
        GetBucketIntelligentTieringConfigurationRequest getBucketIntelligentTieringConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketIntelligentTieringConfiguration(getBucketIntelligentTieringConfigurationRequest);
    }

    @Override
    public GetBucketIntelligentTieringConfigurationResponse getBucketIntelligentTieringConfiguration(
        Consumer<GetBucketIntelligentTieringConfigurationRequest.Builder> getBucketIntelligentTieringConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketIntelligentTieringConfiguration(getBucketIntelligentTieringConfigurationRequest);
    }

    @Override
    public GetBucketInventoryConfigurationResponse getBucketInventoryConfiguration(
        GetBucketInventoryConfigurationRequest getBucketInventoryConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketInventoryConfiguration(getBucketInventoryConfigurationRequest);
    }

    @Override
    public GetBucketInventoryConfigurationResponse getBucketInventoryConfiguration(
        Consumer<GetBucketInventoryConfigurationRequest.Builder> getBucketInventoryConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketInventoryConfiguration(getBucketInventoryConfigurationRequest);
    }

    @Override
    public GetBucketLifecycleConfigurationResponse getBucketLifecycleConfiguration(
        GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketLifecycleConfiguration(getBucketLifecycleConfigurationRequest);
    }

    @Override
    public GetBucketLifecycleConfigurationResponse getBucketLifecycleConfiguration(
        Consumer<GetBucketLifecycleConfigurationRequest.Builder> getBucketLifecycleConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketLifecycleConfiguration(getBucketLifecycleConfigurationRequest);
    }

    @Override
    public GetBucketLocationResponse getBucketLocation(GetBucketLocationRequest getBucketLocationRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getBucketLocation(getBucketLocationRequest);
    }

    @Override
    public GetBucketLocationResponse getBucketLocation(Consumer<GetBucketLocationRequest.Builder> getBucketLocationRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketLocation(getBucketLocationRequest);
    }

    @Override
    public GetBucketLoggingResponse getBucketLogging(GetBucketLoggingRequest getBucketLoggingRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getBucketLogging(getBucketLoggingRequest);
    }

    @Override
    public GetBucketLoggingResponse getBucketLogging(Consumer<GetBucketLoggingRequest.Builder> getBucketLoggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketLogging(getBucketLoggingRequest);
    }

    @Override
    public GetBucketMetadataTableConfigurationResponse getBucketMetadataTableConfiguration(
        GetBucketMetadataTableConfigurationRequest getBucketMetadataTableConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketMetadataTableConfiguration(getBucketMetadataTableConfigurationRequest);
    }

    @Override
    public GetBucketMetadataTableConfigurationResponse getBucketMetadataTableConfiguration(
        Consumer<GetBucketMetadataTableConfigurationRequest.Builder> getBucketMetadataTableConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketMetadataTableConfiguration(getBucketMetadataTableConfigurationRequest);
    }

    @Override
    public GetBucketMetricsConfigurationResponse getBucketMetricsConfiguration(
        GetBucketMetricsConfigurationRequest getBucketMetricsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketMetricsConfiguration(getBucketMetricsConfigurationRequest);
    }

    @Override
    public GetBucketMetricsConfigurationResponse getBucketMetricsConfiguration(
        Consumer<GetBucketMetricsConfigurationRequest.Builder> getBucketMetricsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketMetricsConfiguration(getBucketMetricsConfigurationRequest);
    }

    @Override
    public GetBucketNotificationConfigurationResponse getBucketNotificationConfiguration(
        GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketNotificationConfiguration(getBucketNotificationConfigurationRequest);
    }

    @Override
    public GetBucketNotificationConfigurationResponse getBucketNotificationConfiguration(
        Consumer<GetBucketNotificationConfigurationRequest.Builder> getBucketNotificationConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketNotificationConfiguration(getBucketNotificationConfigurationRequest);
    }

    @Override
    public GetBucketOwnershipControlsResponse getBucketOwnershipControls(
        GetBucketOwnershipControlsRequest getBucketOwnershipControlsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketOwnershipControls(getBucketOwnershipControlsRequest);
    }

    @Override
    public GetBucketOwnershipControlsResponse getBucketOwnershipControls(
        Consumer<GetBucketOwnershipControlsRequest.Builder> getBucketOwnershipControlsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketOwnershipControls(getBucketOwnershipControlsRequest);
    }

    @Override
    public GetBucketPolicyResponse getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getBucketPolicy(getBucketPolicyRequest);
    }

    @Override
    public GetBucketPolicyResponse getBucketPolicy(Consumer<GetBucketPolicyRequest.Builder> getBucketPolicyRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketPolicy(getBucketPolicyRequest);
    }

    @Override
    public GetBucketPolicyStatusResponse getBucketPolicyStatus(GetBucketPolicyStatusRequest getBucketPolicyStatusRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketPolicyStatus(getBucketPolicyStatusRequest);
    }

    @Override
    public GetBucketPolicyStatusResponse getBucketPolicyStatus(Consumer<GetBucketPolicyStatusRequest.Builder> getBucketPolicyStatusRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketPolicyStatus(getBucketPolicyStatusRequest);
    }

    @Override
    public GetBucketReplicationResponse getBucketReplication(GetBucketReplicationRequest getBucketReplicationRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketReplication(getBucketReplicationRequest);
    }

    @Override
    public GetBucketReplicationResponse getBucketReplication(Consumer<GetBucketReplicationRequest.Builder> getBucketReplicationRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketReplication(getBucketReplicationRequest);
    }

    @Override
    public GetBucketRequestPaymentResponse getBucketRequestPayment(GetBucketRequestPaymentRequest getBucketRequestPaymentRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketRequestPayment(getBucketRequestPaymentRequest);
    }

    @Override
    public GetBucketRequestPaymentResponse getBucketRequestPayment(
        Consumer<GetBucketRequestPaymentRequest.Builder> getBucketRequestPaymentRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketRequestPayment(getBucketRequestPaymentRequest);
    }

    @Override
    public GetBucketTaggingResponse getBucketTagging(GetBucketTaggingRequest getBucketTaggingRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getBucketTagging(getBucketTaggingRequest);
    }

    @Override
    public GetBucketTaggingResponse getBucketTagging(Consumer<GetBucketTaggingRequest.Builder> getBucketTaggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketTagging(getBucketTaggingRequest);
    }

    @Override
    public GetBucketVersioningResponse getBucketVersioning(GetBucketVersioningRequest getBucketVersioningRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketVersioning(getBucketVersioningRequest);
    }

    @Override
    public GetBucketVersioningResponse getBucketVersioning(Consumer<GetBucketVersioningRequest.Builder> getBucketVersioningRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketVersioning(getBucketVersioningRequest);
    }

    @Override
    public GetBucketWebsiteResponse getBucketWebsite(GetBucketWebsiteRequest getBucketWebsiteRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getBucketWebsite(getBucketWebsiteRequest);
    }

    @Override
    public GetBucketWebsiteResponse getBucketWebsite(Consumer<GetBucketWebsiteRequest.Builder> getBucketWebsiteRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getBucketWebsite(getBucketWebsiteRequest);
    }

    @Override
    public <ReturnT> ReturnT getObject(
        GetObjectRequest getObjectRequest,
        ResponseTransformer<GetObjectResponse, ReturnT> responseTransformer
    ) throws NoSuchKeyException, InvalidObjectStateException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObject(getObjectRequest, responseTransformer);
    }

    @Override
    public <ReturnT> ReturnT getObject(
        Consumer<GetObjectRequest.Builder> getObjectRequest,
        ResponseTransformer<GetObjectResponse, ReturnT> responseTransformer
    ) throws NoSuchKeyException, InvalidObjectStateException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObject(getObjectRequest, responseTransformer);
    }

    @Override
    public GetObjectResponse getObject(GetObjectRequest getObjectRequest, Path destinationPath) throws NoSuchKeyException,
        InvalidObjectStateException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObject(getObjectRequest, destinationPath);
    }

    @Override
    public GetObjectResponse getObject(Consumer<GetObjectRequest.Builder> getObjectRequest, Path destinationPath) throws NoSuchKeyException,
        InvalidObjectStateException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObject(getObjectRequest, destinationPath);
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest) throws NoSuchKeyException,
        InvalidObjectStateException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObject(getObjectRequest);
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(Consumer<GetObjectRequest.Builder> getObjectRequest) throws NoSuchKeyException,
        InvalidObjectStateException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObject(getObjectRequest);
    }

    @Override
    public ResponseBytes<GetObjectResponse> getObjectAsBytes(GetObjectRequest getObjectRequest) throws NoSuchKeyException,
        InvalidObjectStateException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectAsBytes(getObjectRequest);
    }

    @Override
    public ResponseBytes<GetObjectResponse> getObjectAsBytes(Consumer<GetObjectRequest.Builder> getObjectRequest) throws NoSuchKeyException,
        InvalidObjectStateException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectAsBytes(getObjectRequest);
    }

    @Override
    public GetObjectAclResponse getObjectAcl(GetObjectAclRequest getObjectAclRequest) throws NoSuchKeyException, AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getObjectAcl(getObjectAclRequest);
    }

    @Override
    public GetObjectAclResponse getObjectAcl(Consumer<GetObjectAclRequest.Builder> getObjectAclRequest) throws NoSuchKeyException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectAcl(getObjectAclRequest);
    }

    @Override
    public GetObjectAttributesResponse getObjectAttributes(GetObjectAttributesRequest getObjectAttributesRequest) throws NoSuchKeyException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectAttributes(getObjectAttributesRequest);
    }

    @Override
    public GetObjectAttributesResponse getObjectAttributes(Consumer<GetObjectAttributesRequest.Builder> getObjectAttributesRequest)
        throws NoSuchKeyException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectAttributes(getObjectAttributesRequest);
    }

    @Override
    public GetObjectLegalHoldResponse getObjectLegalHold(GetObjectLegalHoldRequest getObjectLegalHoldRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getObjectLegalHold(getObjectLegalHoldRequest);
    }

    @Override
    public GetObjectLegalHoldResponse getObjectLegalHold(Consumer<GetObjectLegalHoldRequest.Builder> getObjectLegalHoldRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectLegalHold(getObjectLegalHoldRequest);
    }

    @Override
    public GetObjectLockConfigurationResponse getObjectLockConfiguration(
        GetObjectLockConfigurationRequest getObjectLockConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectLockConfiguration(getObjectLockConfigurationRequest);
    }

    @Override
    public GetObjectLockConfigurationResponse getObjectLockConfiguration(
        Consumer<GetObjectLockConfigurationRequest.Builder> getObjectLockConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectLockConfiguration(getObjectLockConfigurationRequest);
    }

    @Override
    public GetObjectRetentionResponse getObjectRetention(GetObjectRetentionRequest getObjectRetentionRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getObjectRetention(getObjectRetentionRequest);
    }

    @Override
    public GetObjectRetentionResponse getObjectRetention(Consumer<GetObjectRetentionRequest.Builder> getObjectRetentionRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectRetention(getObjectRetentionRequest);
    }

    @Override
    public GetObjectTaggingResponse getObjectTagging(GetObjectTaggingRequest getObjectTaggingRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.getObjectTagging(getObjectTaggingRequest);
    }

    @Override
    public GetObjectTaggingResponse getObjectTagging(Consumer<GetObjectTaggingRequest.Builder> getObjectTaggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectTagging(getObjectTaggingRequest);
    }

    @Override
    public <ReturnT> ReturnT getObjectTorrent(
        GetObjectTorrentRequest getObjectTorrentRequest,
        ResponseTransformer<GetObjectTorrentResponse, ReturnT> responseTransformer
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectTorrent(getObjectTorrentRequest, responseTransformer);
    }

    @Override
    public <ReturnT> ReturnT getObjectTorrent(
        Consumer<GetObjectTorrentRequest.Builder> getObjectTorrentRequest,
        ResponseTransformer<GetObjectTorrentResponse, ReturnT> responseTransformer
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectTorrent(getObjectTorrentRequest, responseTransformer);
    }

    @Override
    public GetObjectTorrentResponse getObjectTorrent(GetObjectTorrentRequest getObjectTorrentRequest, Path destinationPath)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectTorrent(getObjectTorrentRequest, destinationPath);
    }

    @Override
    public GetObjectTorrentResponse getObjectTorrent(
        Consumer<GetObjectTorrentRequest.Builder> getObjectTorrentRequest,
        Path destinationPath
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectTorrent(getObjectTorrentRequest, destinationPath);
    }

    @Override
    public ResponseInputStream<GetObjectTorrentResponse> getObjectTorrent(GetObjectTorrentRequest getObjectTorrentRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectTorrent(getObjectTorrentRequest);
    }

    @Override
    public ResponseInputStream<GetObjectTorrentResponse> getObjectTorrent(Consumer<GetObjectTorrentRequest.Builder> getObjectTorrentRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectTorrent(getObjectTorrentRequest);
    }

    @Override
    public ResponseBytes<GetObjectTorrentResponse> getObjectTorrentAsBytes(GetObjectTorrentRequest getObjectTorrentRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectTorrentAsBytes(getObjectTorrentRequest);
    }

    @Override
    public ResponseBytes<GetObjectTorrentResponse> getObjectTorrentAsBytes(
        Consumer<GetObjectTorrentRequest.Builder> getObjectTorrentRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getObjectTorrentAsBytes(getObjectTorrentRequest);
    }

    @Override
    public GetPublicAccessBlockResponse getPublicAccessBlock(GetPublicAccessBlockRequest getPublicAccessBlockRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getPublicAccessBlock(getPublicAccessBlockRequest);
    }

    @Override
    public GetPublicAccessBlockResponse getPublicAccessBlock(Consumer<GetPublicAccessBlockRequest.Builder> getPublicAccessBlockRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.getPublicAccessBlock(getPublicAccessBlockRequest);
    }

    @Override
    public HeadBucketResponse headBucket(HeadBucketRequest headBucketRequest) throws NoSuchBucketException, AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.headBucket(headBucketRequest);
    }

    @Override
    public HeadBucketResponse headBucket(Consumer<HeadBucketRequest.Builder> headBucketRequest) throws NoSuchBucketException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.headBucket(headBucketRequest);
    }

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest) throws NoSuchKeyException, AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.headObject(headObjectRequest);
    }

    @Override
    public HeadObjectResponse headObject(Consumer<HeadObjectRequest.Builder> headObjectRequest) throws NoSuchKeyException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.headObject(headObjectRequest);
    }

    @Override
    public ListBucketAnalyticsConfigurationsResponse listBucketAnalyticsConfigurations(
        ListBucketAnalyticsConfigurationsRequest listBucketAnalyticsConfigurationsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBucketAnalyticsConfigurations(listBucketAnalyticsConfigurationsRequest);
    }

    @Override
    public ListBucketAnalyticsConfigurationsResponse listBucketAnalyticsConfigurations(
        Consumer<ListBucketAnalyticsConfigurationsRequest.Builder> listBucketAnalyticsConfigurationsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBucketAnalyticsConfigurations(listBucketAnalyticsConfigurationsRequest);
    }

    @Override
    public ListBucketIntelligentTieringConfigurationsResponse listBucketIntelligentTieringConfigurations(
        ListBucketIntelligentTieringConfigurationsRequest listBucketIntelligentTieringConfigurationsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBucketIntelligentTieringConfigurations(listBucketIntelligentTieringConfigurationsRequest);
    }

    @Override
    public ListBucketIntelligentTieringConfigurationsResponse listBucketIntelligentTieringConfigurations(
        Consumer<ListBucketIntelligentTieringConfigurationsRequest.Builder> listBucketIntelligentTieringConfigurationsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBucketIntelligentTieringConfigurations(listBucketIntelligentTieringConfigurationsRequest);
    }

    @Override
    public ListBucketInventoryConfigurationsResponse listBucketInventoryConfigurations(
        ListBucketInventoryConfigurationsRequest listBucketInventoryConfigurationsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBucketInventoryConfigurations(listBucketInventoryConfigurationsRequest);
    }

    @Override
    public ListBucketInventoryConfigurationsResponse listBucketInventoryConfigurations(
        Consumer<ListBucketInventoryConfigurationsRequest.Builder> listBucketInventoryConfigurationsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBucketInventoryConfigurations(listBucketInventoryConfigurationsRequest);
    }

    @Override
    public ListBucketMetricsConfigurationsResponse listBucketMetricsConfigurations(
        ListBucketMetricsConfigurationsRequest listBucketMetricsConfigurationsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBucketMetricsConfigurations(listBucketMetricsConfigurationsRequest);
    }

    @Override
    public ListBucketMetricsConfigurationsResponse listBucketMetricsConfigurations(
        Consumer<ListBucketMetricsConfigurationsRequest.Builder> listBucketMetricsConfigurationsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBucketMetricsConfigurations(listBucketMetricsConfigurationsRequest);
    }

    @Override
    public ListBucketsResponse listBuckets(ListBucketsRequest listBucketsRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.listBuckets(listBucketsRequest);
    }

    @Override
    public ListBucketsResponse listBuckets(Consumer<ListBucketsRequest.Builder> listBucketsRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.listBuckets(listBucketsRequest);
    }

    @Override
    public ListBucketsResponse listBuckets() throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBuckets();
    }

    @Override
    public ListBucketsIterable listBucketsPaginator() throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listBucketsPaginator();
    }

    @Override
    public ListBucketsIterable listBucketsPaginator(ListBucketsRequest listBucketsRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.listBucketsPaginator(listBucketsRequest);
    }

    @Override
    public ListBucketsIterable listBucketsPaginator(Consumer<ListBucketsRequest.Builder> listBucketsRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.listBucketsPaginator(listBucketsRequest);
    }

    @Override
    public ListDirectoryBucketsResponse listDirectoryBuckets(ListDirectoryBucketsRequest listDirectoryBucketsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listDirectoryBuckets(listDirectoryBucketsRequest);
    }

    @Override
    public ListDirectoryBucketsResponse listDirectoryBuckets(Consumer<ListDirectoryBucketsRequest.Builder> listDirectoryBucketsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listDirectoryBuckets(listDirectoryBucketsRequest);
    }

    @Override
    public ListDirectoryBucketsIterable listDirectoryBucketsPaginator(ListDirectoryBucketsRequest listDirectoryBucketsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listDirectoryBucketsPaginator(listDirectoryBucketsRequest);
    }

    @Override
    public ListDirectoryBucketsIterable listDirectoryBucketsPaginator(
        Consumer<ListDirectoryBucketsRequest.Builder> listDirectoryBucketsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listDirectoryBucketsPaginator(listDirectoryBucketsRequest);
    }

    @Override
    public ListMultipartUploadsResponse listMultipartUploads(ListMultipartUploadsRequest listMultipartUploadsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listMultipartUploads(listMultipartUploadsRequest);
    }

    @Override
    public ListMultipartUploadsResponse listMultipartUploads(Consumer<ListMultipartUploadsRequest.Builder> listMultipartUploadsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listMultipartUploads(listMultipartUploadsRequest);
    }

    @Override
    public ListMultipartUploadsIterable listMultipartUploadsPaginator(ListMultipartUploadsRequest listMultipartUploadsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listMultipartUploadsPaginator(listMultipartUploadsRequest);
    }

    @Override
    public ListMultipartUploadsIterable listMultipartUploadsPaginator(
        Consumer<ListMultipartUploadsRequest.Builder> listMultipartUploadsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listMultipartUploadsPaginator(listMultipartUploadsRequest);
    }

    @Override
    public ListObjectVersionsResponse listObjectVersions(ListObjectVersionsRequest listObjectVersionsRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.listObjectVersions(listObjectVersionsRequest);
    }

    @Override
    public ListObjectVersionsResponse listObjectVersions(Consumer<ListObjectVersionsRequest.Builder> listObjectVersionsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listObjectVersions(listObjectVersionsRequest);
    }

    @Override
    public ListObjectVersionsIterable listObjectVersionsPaginator(ListObjectVersionsRequest listObjectVersionsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listObjectVersionsPaginator(listObjectVersionsRequest);
    }

    @Override
    public ListObjectVersionsIterable listObjectVersionsPaginator(Consumer<ListObjectVersionsRequest.Builder> listObjectVersionsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listObjectVersionsPaginator(listObjectVersionsRequest);
    }

    @Override
    public ListObjectsResponse listObjects(ListObjectsRequest listObjectsRequest) throws NoSuchBucketException, AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.listObjects(listObjectsRequest);
    }

    @Override
    public ListObjectsResponse listObjects(Consumer<ListObjectsRequest.Builder> listObjectsRequest) throws NoSuchBucketException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.listObjects(listObjectsRequest);
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request) throws NoSuchBucketException, AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.listObjectsV2(listObjectsV2Request);
    }

    @Override
    public ListObjectsV2Response listObjectsV2(Consumer<ListObjectsV2Request.Builder> listObjectsV2Request) throws NoSuchBucketException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.listObjectsV2(listObjectsV2Request);
    }

    @Override
    public ListObjectsV2Iterable listObjectsV2Paginator(ListObjectsV2Request listObjectsV2Request) throws NoSuchBucketException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.listObjectsV2Paginator(listObjectsV2Request);
    }

    @Override
    public ListObjectsV2Iterable listObjectsV2Paginator(Consumer<ListObjectsV2Request.Builder> listObjectsV2Request)
        throws NoSuchBucketException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.listObjectsV2Paginator(listObjectsV2Request);
    }

    @Override
    public ListPartsResponse listParts(ListPartsRequest listPartsRequest) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.listParts(listPartsRequest);
    }

    @Override
    public ListPartsResponse listParts(Consumer<ListPartsRequest.Builder> listPartsRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.listParts(listPartsRequest);
    }

    @Override
    public ListPartsIterable listPartsPaginator(ListPartsRequest listPartsRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.listPartsPaginator(listPartsRequest);
    }

    @Override
    public ListPartsIterable listPartsPaginator(Consumer<ListPartsRequest.Builder> listPartsRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.listPartsPaginator(listPartsRequest);
    }

    @Override
    public PutBucketAccelerateConfigurationResponse putBucketAccelerateConfiguration(
        PutBucketAccelerateConfigurationRequest putBucketAccelerateConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketAccelerateConfiguration(putBucketAccelerateConfigurationRequest);
    }

    @Override
    public PutBucketAccelerateConfigurationResponse putBucketAccelerateConfiguration(
        Consumer<PutBucketAccelerateConfigurationRequest.Builder> putBucketAccelerateConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketAccelerateConfiguration(putBucketAccelerateConfigurationRequest);
    }

    @Override
    public PutBucketAclResponse putBucketAcl(PutBucketAclRequest putBucketAclRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.putBucketAcl(putBucketAclRequest);
    }

    @Override
    public PutBucketAclResponse putBucketAcl(Consumer<PutBucketAclRequest.Builder> putBucketAclRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putBucketAcl(putBucketAclRequest);
    }

    @Override
    public PutBucketAnalyticsConfigurationResponse putBucketAnalyticsConfiguration(
        PutBucketAnalyticsConfigurationRequest putBucketAnalyticsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketAnalyticsConfiguration(putBucketAnalyticsConfigurationRequest);
    }

    @Override
    public PutBucketAnalyticsConfigurationResponse putBucketAnalyticsConfiguration(
        Consumer<PutBucketAnalyticsConfigurationRequest.Builder> putBucketAnalyticsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketAnalyticsConfiguration(putBucketAnalyticsConfigurationRequest);
    }

    @Override
    public PutBucketCorsResponse putBucketCors(PutBucketCorsRequest putBucketCorsRequest) throws AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.putBucketCors(putBucketCorsRequest);
    }

    @Override
    public PutBucketCorsResponse putBucketCors(Consumer<PutBucketCorsRequest.Builder> putBucketCorsRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putBucketCors(putBucketCorsRequest);
    }

    @Override
    public PutBucketEncryptionResponse putBucketEncryption(PutBucketEncryptionRequest putBucketEncryptionRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketEncryption(putBucketEncryptionRequest);
    }

    @Override
    public PutBucketEncryptionResponse putBucketEncryption(Consumer<PutBucketEncryptionRequest.Builder> putBucketEncryptionRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketEncryption(putBucketEncryptionRequest);
    }

    @Override
    public PutBucketIntelligentTieringConfigurationResponse putBucketIntelligentTieringConfiguration(
        PutBucketIntelligentTieringConfigurationRequest putBucketIntelligentTieringConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketIntelligentTieringConfiguration(putBucketIntelligentTieringConfigurationRequest);
    }

    @Override
    public PutBucketIntelligentTieringConfigurationResponse putBucketIntelligentTieringConfiguration(
        Consumer<PutBucketIntelligentTieringConfigurationRequest.Builder> putBucketIntelligentTieringConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketIntelligentTieringConfiguration(putBucketIntelligentTieringConfigurationRequest);
    }

    @Override
    public PutBucketInventoryConfigurationResponse putBucketInventoryConfiguration(
        PutBucketInventoryConfigurationRequest putBucketInventoryConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketInventoryConfiguration(putBucketInventoryConfigurationRequest);
    }

    @Override
    public PutBucketInventoryConfigurationResponse putBucketInventoryConfiguration(
        Consumer<PutBucketInventoryConfigurationRequest.Builder> putBucketInventoryConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketInventoryConfiguration(putBucketInventoryConfigurationRequest);
    }

    @Override
    public PutBucketLifecycleConfigurationResponse putBucketLifecycleConfiguration(
        PutBucketLifecycleConfigurationRequest putBucketLifecycleConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketLifecycleConfiguration(putBucketLifecycleConfigurationRequest);
    }

    @Override
    public PutBucketLifecycleConfigurationResponse putBucketLifecycleConfiguration(
        Consumer<PutBucketLifecycleConfigurationRequest.Builder> putBucketLifecycleConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketLifecycleConfiguration(putBucketLifecycleConfigurationRequest);
    }

    @Override
    public PutBucketLoggingResponse putBucketLogging(PutBucketLoggingRequest putBucketLoggingRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putBucketLogging(putBucketLoggingRequest);
    }

    @Override
    public PutBucketLoggingResponse putBucketLogging(Consumer<PutBucketLoggingRequest.Builder> putBucketLoggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketLogging(putBucketLoggingRequest);
    }

    @Override
    public PutBucketMetricsConfigurationResponse putBucketMetricsConfiguration(
        PutBucketMetricsConfigurationRequest putBucketMetricsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketMetricsConfiguration(putBucketMetricsConfigurationRequest);
    }

    @Override
    public PutBucketMetricsConfigurationResponse putBucketMetricsConfiguration(
        Consumer<PutBucketMetricsConfigurationRequest.Builder> putBucketMetricsConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketMetricsConfiguration(putBucketMetricsConfigurationRequest);
    }

    @Override
    public PutBucketNotificationConfigurationResponse putBucketNotificationConfiguration(
        PutBucketNotificationConfigurationRequest putBucketNotificationConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketNotificationConfiguration(putBucketNotificationConfigurationRequest);
    }

    @Override
    public PutBucketNotificationConfigurationResponse putBucketNotificationConfiguration(
        Consumer<PutBucketNotificationConfigurationRequest.Builder> putBucketNotificationConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketNotificationConfiguration(putBucketNotificationConfigurationRequest);
    }

    @Override
    public PutBucketOwnershipControlsResponse putBucketOwnershipControls(
        PutBucketOwnershipControlsRequest putBucketOwnershipControlsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketOwnershipControls(putBucketOwnershipControlsRequest);
    }

    @Override
    public PutBucketOwnershipControlsResponse putBucketOwnershipControls(
        Consumer<PutBucketOwnershipControlsRequest.Builder> putBucketOwnershipControlsRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketOwnershipControls(putBucketOwnershipControlsRequest);
    }

    @Override
    public PutBucketPolicyResponse putBucketPolicy(PutBucketPolicyRequest putBucketPolicyRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putBucketPolicy(putBucketPolicyRequest);
    }

    @Override
    public PutBucketPolicyResponse putBucketPolicy(Consumer<PutBucketPolicyRequest.Builder> putBucketPolicyRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketPolicy(putBucketPolicyRequest);
    }

    @Override
    public PutBucketReplicationResponse putBucketReplication(PutBucketReplicationRequest putBucketReplicationRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketReplication(putBucketReplicationRequest);
    }

    @Override
    public PutBucketReplicationResponse putBucketReplication(Consumer<PutBucketReplicationRequest.Builder> putBucketReplicationRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketReplication(putBucketReplicationRequest);
    }

    @Override
    public PutBucketRequestPaymentResponse putBucketRequestPayment(PutBucketRequestPaymentRequest putBucketRequestPaymentRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketRequestPayment(putBucketRequestPaymentRequest);
    }

    @Override
    public PutBucketRequestPaymentResponse putBucketRequestPayment(
        Consumer<PutBucketRequestPaymentRequest.Builder> putBucketRequestPaymentRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketRequestPayment(putBucketRequestPaymentRequest);
    }

    @Override
    public PutBucketTaggingResponse putBucketTagging(PutBucketTaggingRequest putBucketTaggingRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putBucketTagging(putBucketTaggingRequest);
    }

    @Override
    public PutBucketTaggingResponse putBucketTagging(Consumer<PutBucketTaggingRequest.Builder> putBucketTaggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketTagging(putBucketTaggingRequest);
    }

    @Override
    public PutBucketVersioningResponse putBucketVersioning(PutBucketVersioningRequest putBucketVersioningRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketVersioning(putBucketVersioningRequest);
    }

    @Override
    public PutBucketVersioningResponse putBucketVersioning(Consumer<PutBucketVersioningRequest.Builder> putBucketVersioningRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketVersioning(putBucketVersioningRequest);
    }

    @Override
    public PutBucketWebsiteResponse putBucketWebsite(PutBucketWebsiteRequest putBucketWebsiteRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putBucketWebsite(putBucketWebsiteRequest);
    }

    @Override
    public PutBucketWebsiteResponse putBucketWebsite(Consumer<PutBucketWebsiteRequest.Builder> putBucketWebsiteRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putBucketWebsite(putBucketWebsiteRequest);
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody) throws InvalidRequestException,
        InvalidWriteOffsetException, TooManyPartsException, EncryptionTypeMismatchException, AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.putObject(putObjectRequest, requestBody);
    }

    @Override
    public PutObjectResponse putObject(Consumer<PutObjectRequest.Builder> putObjectRequest, RequestBody requestBody)
        throws InvalidRequestException, InvalidWriteOffsetException, TooManyPartsException, EncryptionTypeMismatchException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.putObject(putObjectRequest, requestBody);
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, Path sourcePath) throws InvalidRequestException,
        InvalidWriteOffsetException, TooManyPartsException, EncryptionTypeMismatchException, AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.putObject(putObjectRequest, sourcePath);
    }

    @Override
    public PutObjectResponse putObject(Consumer<PutObjectRequest.Builder> putObjectRequest, Path sourcePath) throws InvalidRequestException,
        InvalidWriteOffsetException, TooManyPartsException, EncryptionTypeMismatchException, AwsServiceException, SdkClientException,
        S3Exception {
        return delegate.putObject(putObjectRequest, sourcePath);
    }

    @Override
    public PutObjectAclResponse putObjectAcl(PutObjectAclRequest putObjectAclRequest) throws NoSuchKeyException, AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putObjectAcl(putObjectAclRequest);
    }

    @Override
    public PutObjectAclResponse putObjectAcl(Consumer<PutObjectAclRequest.Builder> putObjectAclRequest) throws NoSuchKeyException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.putObjectAcl(putObjectAclRequest);
    }

    @Override
    public PutObjectLegalHoldResponse putObjectLegalHold(PutObjectLegalHoldRequest putObjectLegalHoldRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putObjectLegalHold(putObjectLegalHoldRequest);
    }

    @Override
    public PutObjectLegalHoldResponse putObjectLegalHold(Consumer<PutObjectLegalHoldRequest.Builder> putObjectLegalHoldRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putObjectLegalHold(putObjectLegalHoldRequest);
    }

    @Override
    public PutObjectLockConfigurationResponse putObjectLockConfiguration(
        PutObjectLockConfigurationRequest putObjectLockConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putObjectLockConfiguration(putObjectLockConfigurationRequest);
    }

    @Override
    public PutObjectLockConfigurationResponse putObjectLockConfiguration(
        Consumer<PutObjectLockConfigurationRequest.Builder> putObjectLockConfigurationRequest
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putObjectLockConfiguration(putObjectLockConfigurationRequest);
    }

    @Override
    public PutObjectRetentionResponse putObjectRetention(PutObjectRetentionRequest putObjectRetentionRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putObjectRetention(putObjectRetentionRequest);
    }

    @Override
    public PutObjectRetentionResponse putObjectRetention(Consumer<PutObjectRetentionRequest.Builder> putObjectRetentionRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putObjectRetention(putObjectRetentionRequest);
    }

    @Override
    public PutObjectTaggingResponse putObjectTagging(PutObjectTaggingRequest putObjectTaggingRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.putObjectTagging(putObjectTaggingRequest);
    }

    @Override
    public PutObjectTaggingResponse putObjectTagging(Consumer<PutObjectTaggingRequest.Builder> putObjectTaggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putObjectTagging(putObjectTaggingRequest);
    }

    @Override
    public PutPublicAccessBlockResponse putPublicAccessBlock(PutPublicAccessBlockRequest putPublicAccessBlockRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putPublicAccessBlock(putPublicAccessBlockRequest);
    }

    @Override
    public PutPublicAccessBlockResponse putPublicAccessBlock(Consumer<PutPublicAccessBlockRequest.Builder> putPublicAccessBlockRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.putPublicAccessBlock(putPublicAccessBlockRequest);
    }

    @Override
    public RestoreObjectResponse restoreObject(RestoreObjectRequest restoreObjectRequest) throws ObjectAlreadyInActiveTierErrorException,
        AwsServiceException, SdkClientException, S3Exception {
        return delegate.restoreObject(restoreObjectRequest);
    }

    @Override
    public RestoreObjectResponse restoreObject(Consumer<RestoreObjectRequest.Builder> restoreObjectRequest)
        throws ObjectAlreadyInActiveTierErrorException, AwsServiceException, SdkClientException, S3Exception {
        return delegate.restoreObject(restoreObjectRequest);
    }

    @Override
    public UploadPartResponse uploadPart(UploadPartRequest uploadPartRequest, RequestBody requestBody) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.uploadPart(uploadPartRequest, requestBody);
    }

    @Override
    public UploadPartResponse uploadPart(Consumer<UploadPartRequest.Builder> uploadPartRequest, RequestBody requestBody)
        throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.uploadPart(uploadPartRequest, requestBody);
    }

    @Override
    public UploadPartResponse uploadPart(UploadPartRequest uploadPartRequest, Path sourcePath) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.uploadPart(uploadPartRequest, sourcePath);
    }

    @Override
    public UploadPartResponse uploadPart(Consumer<UploadPartRequest.Builder> uploadPartRequest, Path sourcePath) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.uploadPart(uploadPartRequest, sourcePath);
    }

    @Override
    public UploadPartCopyResponse uploadPartCopy(UploadPartCopyRequest uploadPartCopyRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.uploadPartCopy(uploadPartCopyRequest);
    }

    @Override
    public UploadPartCopyResponse uploadPartCopy(Consumer<UploadPartCopyRequest.Builder> uploadPartCopyRequest) throws AwsServiceException,
        SdkClientException, S3Exception {
        return delegate.uploadPartCopy(uploadPartCopyRequest);
    }

    @Override
    public WriteGetObjectResponseResponse writeGetObjectResponse(
        WriteGetObjectResponseRequest writeGetObjectResponseRequest,
        RequestBody requestBody
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.writeGetObjectResponse(writeGetObjectResponseRequest, requestBody);
    }

    @Override
    public WriteGetObjectResponseResponse writeGetObjectResponse(
        Consumer<WriteGetObjectResponseRequest.Builder> writeGetObjectResponseRequest,
        RequestBody requestBody
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.writeGetObjectResponse(writeGetObjectResponseRequest, requestBody);
    }

    @Override
    public WriteGetObjectResponseResponse writeGetObjectResponse(
        WriteGetObjectResponseRequest writeGetObjectResponseRequest,
        Path sourcePath
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.writeGetObjectResponse(writeGetObjectResponseRequest, sourcePath);
    }

    @Override
    public WriteGetObjectResponseResponse writeGetObjectResponse(
        Consumer<WriteGetObjectResponseRequest.Builder> writeGetObjectResponseRequest,
        Path sourcePath
    ) throws AwsServiceException, SdkClientException, S3Exception {
        return delegate.writeGetObjectResponse(writeGetObjectResponseRequest, sourcePath);
    }

    @Override
    public S3Utilities utilities() {
        return delegate.utilities();
    }

    @Override
    public S3Waiter waiter() {
        return delegate.waiter();
    }

    @Override
    public S3ServiceClientConfiguration serviceClientConfiguration() {
        return delegate.serviceClientConfiguration();
    }
}
