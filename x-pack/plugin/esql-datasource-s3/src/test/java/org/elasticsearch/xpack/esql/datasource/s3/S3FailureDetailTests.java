/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.IntelligentTieringAccessTier;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;

import org.elasticsearch.test.ESTestCase;

/**
 * Pins how a 403 is told apart from what S3 said: the IAM action a policy denial names, credentials S3 did not accept,
 * and an archived object. Only well-formed values are extracted, so nothing else from the store's text is echoed.
 */
public class S3FailureDetailTests extends ESTestCase {

    public void testDeniedActionIsTheActionAfterThePhrase() {
        assertEquals(
            "kms:Decrypt",
            S3FailureDetail.deniedAction(
                denied(
                    403,
                    "User: arn:aws:sts::123456789012:assumed-role/reader/session is not authorized to perform: kms:Decrypt "
                        + "on resource: arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555"
                )
            )
        );
        assertEquals(
            "s3-object-lambda:GetObject",
            S3FailureDetail.deniedAction(denied(403, "User: anonymous is not authorized to perform: s3-object-lambda:GetObject"))
        );
    }

    public void testDeniedActionIsNullWhenTheMessageNamesNone() {
        assertNull(S3FailureDetail.deniedAction(denied(403, "Access Denied")));
        assertNull(S3FailureDetail.deniedAction(denied(403, null)));
        assertNull(S3FailureDetail.deniedAction((S3Exception) S3Exception.builder().statusCode(403).message("Access Denied").build()));
    }

    public void testDeniedActionIsNullForAnythingButAnAction() {
        assertNull(S3FailureDetail.deniedAction(denied(403, "is not authorized to perform: arn:aws:kms:us-east-1:123456789012:key/1")));
        assertNull(S3FailureDetail.deniedAction(denied(403, "is not authorized to perform: <script>")));
        assertNull(S3FailureDetail.deniedAction(denied(400, "is not authorized to perform: kms:Decrypt")));
        assertNull(S3FailureDetail.deniedAction(denied(403, "is not authorized to perform: kms:D" + "e".repeat(200))));
    }

    public void testCredentialsRejectedOnlyForTheCodesThatSaySo() {
        assertTrue(S3FailureDetail.isCredentialsRejected(withCode(403, "InvalidAccessKeyId")));
        assertTrue(S3FailureDetail.isCredentialsRejected(withCode(403, "SignatureDoesNotMatch")));
        assertFalse(S3FailureDetail.isCredentialsRejected(withCode(403, "AccessDenied")));
        assertFalse(S3FailureDetail.isCredentialsRejected(withCode(400, "InvalidAccessKeyId")));
        assertFalse(S3FailureDetail.isCredentialsRejected((S3Exception) S3Exception.builder().statusCode(403).build()));
    }

    public void testArchivedIsKeyedOnTheErrorCode() {
        assertTrue(S3FailureDetail.isArchived(withCode(403, "InvalidObjectState")));
        assertFalse(S3FailureDetail.isArchived(withCode(403, "AccessDenied")));
        assertFalse(S3FailureDetail.isArchived((S3Exception) S3Exception.builder().statusCode(403).build()));
    }

    public void testArchivedTierRendersOnlyWhatTheSdkKnows() {
        assertEquals("storage class [GLACIER]", S3FailureDetail.archivedTier(archived(StorageClass.GLACIER, null)));
        assertEquals(
            "storage class [INTELLIGENT_TIERING] and access tier [ARCHIVE_ACCESS]",
            S3FailureDetail.archivedTier(archived(StorageClass.INTELLIGENT_TIERING, IntelligentTieringAccessTier.ARCHIVE_ACCESS))
        );
        assertEquals(
            "access tier [DEEP_ARCHIVE_ACCESS]",
            S3FailureDetail.archivedTier(archived(null, IntelligentTieringAccessTier.DEEP_ARCHIVE_ACCESS))
        );
        assertEquals(
            "",
            S3FailureDetail.archivedTier(
                InvalidObjectStateException.builder().storageClass("<not a storage class>").accessTier("ARN:aws").statusCode(403).build()
            )
        );
        assertEquals("", S3FailureDetail.archivedTier(withCode(403, "InvalidObjectState")));
    }

    private static S3Exception denied(int status, String errorMessage) {
        return (S3Exception) S3Exception.builder()
            .statusCode(status)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").errorMessage(errorMessage).build())
            .build();
    }

    private static S3Exception withCode(int status, String errorCode) {
        return (S3Exception) S3Exception.builder()
            .statusCode(status)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
            .build();
    }

    private static InvalidObjectStateException archived(StorageClass storageClass, IntelligentTieringAccessTier accessTier) {
        return InvalidObjectStateException.builder().storageClass(storageClass).accessTier(accessTier).statusCode(403).build();
    }
}
