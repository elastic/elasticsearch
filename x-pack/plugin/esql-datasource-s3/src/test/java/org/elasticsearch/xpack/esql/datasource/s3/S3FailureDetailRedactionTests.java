/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class S3FailureDetailRedactionTests extends ESTestCase {

    private StoragePath pathWithSensitivePrefix() {
        // bucket/partition=2026-09-09/account-123456789012/us-west-2/file.parquet
        return StoragePath.of(
            "s3://my-bucket/tenant-partition=2026-09-09/account-123456789012/us-west-2/data.parquet"
        );
    }

    public void testRedactPathDropsSensitivePrefix() {
        String redacted = S3FailureDetail.redactPath(pathWithSensitivePrefix());
        assertEquals("s3://my-bucket/data.parquet", redacted);
        assertThat(redacted, not(containsString("123456789012")));
        assertThat(redacted, not(containsString("us-west-2")));
        assertThat(redacted, not(containsString("2026-09-09")));
        assertThat(redacted, not(containsString("tenant-partition")));
    }

    public void testRedactPathPreservesSchemeAndBucket() {
        StoragePath p = StoragePath.of("s3://my-bucket/data.parquet");
        assertEquals("s3://my-bucket/data.parquet", S3FailureDetail.redactPath(p));
    }

    public void testRedactPathNullSafe() {
        assertEquals("", S3FailureDetail.redactPath(null));
    }

    public void testRedactPrefixDropsSensitiveFolders() {
        String prefix = "tenant-partition=2026-09-09/account-123456789012/us-west-2/";
        assertEquals("", S3FailureDetail.redactPrefix(prefix));
        String prefixNoTrailing = "tenant-partition=2026-09-09/account-123456789012/us-west-2/data";
        assertEquals("data", S3FailureDetail.redactPrefix(prefixNoTrailing));
        assertThat(S3FailureDetail.redactPrefix(prefixNoTrailing), not(containsString("123456789012")));
        assertThat(S3FailureDetail.redactPrefix(prefixNoTrailing), not(containsString("us-west-2")));
    }

    public void testRedactPrefixSingleSegmentAndNull() {
        assertEquals("data.parquet", S3FailureDetail.redactPrefix("data.parquet"));
        assertEquals("", S3FailureDetail.redactPrefix(""));
        assertEquals("", S3FailureDetail.redactPrefix(null));
    }

    public void testLogKeepsFullDetail() {
        // 服务端日志版保留完整细节（脱敏只作用于用户可见消息）
        Exception plain = new Exception("boom");
        assertEquals("boom", S3FailureDetail.log(plain));
    }
}