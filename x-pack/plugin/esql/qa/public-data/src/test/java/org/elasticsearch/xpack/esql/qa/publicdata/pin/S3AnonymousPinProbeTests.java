/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

/** Offline pieces of the S3 probe: ListObjectsV2 XML scanning and s3:// URI parsing. */
public class S3AnonymousPinProbeTests extends ESTestCase {

    private static final String LISTING_XML = """
        <?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Name>clickhouse-public-datasets</Name>
          <Prefix>hits_compatible/athena_partitioned/</Prefix>
          <KeyCount>2</KeyCount>
          <MaxKeys>1000</MaxKeys>
          <IsTruncated>false</IsTruncated>
          <Contents>
            <Key>hits_compatible/athena_partitioned/hits_0.parquet</Key>
            <LastModified>2022-06-25T22:22:22.000Z</LastModified>
            <ETag>&quot;843c108848a3929260d44588b39ec1b6-6&quot;</ETag>
            <Size>122446530</Size>
            <StorageClass>STANDARD</StorageClass>
          </Contents>
          <Contents>
            <Key>hits_compatible/athena_partitioned/hits_1.parquet</Key>
            <LastModified>2022-06-25T22:22:23.000Z</LastModified>
            <ETag>&quot;deadbeefdeadbeefdeadbeefdeadbeef-1&quot;</ETag>
            <Size>133456789</Size>
            <StorageClass>STANDARD</StorageClass>
          </Contents>
        </ListBucketResult>
        """;

    public void testParseListing() {
        List<ObjectMetadata> objects = S3AnonymousPinProbe.parseListing(LISTING_XML);
        assertEquals(2, objects.size());
        assertEquals("hits_compatible/athena_partitioned/hits_0.parquet", objects.get(0).key());
        assertEquals("843c108848a3929260d44588b39ec1b6-6", objects.get(0).etag());
        assertEquals(122446530L, objects.get(0).sizeBytes());
        assertEquals("2022-06-25T22:22:22.000Z", objects.get(0).lastModified());
        assertEquals(133456789L, objects.get(1).sizeBytes());
    }

    public void testParseEmptyListing() {
        assertEquals(0, S3AnonymousPinProbe.parseListing("<ListBucketResult><KeyCount>0</KeyCount></ListBucketResult>").size());
    }

    public void testS3LocationParsing() {
        S3AnonymousPinProbe.S3Location location = S3AnonymousPinProbe.S3Location.parse("s3://bucket-name/some/deep/key.parquet");
        assertEquals("bucket-name", location.bucket());
        assertEquals("some/deep/key.parquet", location.key());

        S3AnonymousPinProbe.S3Location bare = S3AnonymousPinProbe.S3Location.parse("s3://bucket-name");
        assertEquals("bucket-name", bare.bucket());
        assertEquals("", bare.key());

        expectThrows(IllegalArgumentException.class, () -> S3AnonymousPinProbe.S3Location.parse("https://not-s3.example.org/x"));
    }

    public void testRetryClassification() {
        assertTrue(PinRetry.isRetryableStatus(429));
        assertTrue(PinRetry.isRetryableStatus(500));
        assertTrue(PinRetry.isRetryableStatus(503));
        assertFalse(PinRetry.isRetryableStatus(403));
        assertFalse(PinRetry.isRetryableStatus(404));
    }
}
