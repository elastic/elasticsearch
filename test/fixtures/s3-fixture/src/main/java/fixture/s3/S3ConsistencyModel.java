/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.s3;

import static org.elasticsearch.test.ESTestCase.randomFrom;

/**
 * AWS S3 has weaker consistency for its multipart upload APIs than initially claimed (see support cases 10837136441 and 176070774900712)
 * but strong consistency of conditional writes based on the {@code If-Match} and {@code If-None-Match} headers. Other object storage
 * suppliers have decided instead to implement strongly-consistent multipart upload APIs and ignore the conditional writes headers. We
 * verify Elasticsearch's behaviour against both models.
 */
public enum S3ConsistencyModel {
    /**
     * The model implemented by AWS S3: multipart upload APIs are somewhat weak (e.g. aborts may return while the write operation is still
     * in flight) but conditional writes work as expected.
     */
    AWS_DEFAULT(true, false),

    /**
     * The alternative model verified by these tests: the multipart upload APIs are strongly consistent, but the {@code If-Match} and
     * {@code If-None-Match} headers are ignored and all writes are unconditional.
     */
    STRONG_MPUS(false, true);

    private final boolean conditionalWrites;
    private final boolean strongMultipartUploads;

    S3ConsistencyModel(boolean conditionalWrites, boolean strongMultipartUploads) {
        this.conditionalWrites = conditionalWrites;
        this.strongMultipartUploads = strongMultipartUploads;
    }

    public boolean hasStrongMultipartUploads() {
        return strongMultipartUploads;
    }

    public boolean hasConditionalWrites() {
        return conditionalWrites;
    }

    public static S3ConsistencyModel randomConsistencyModel() {
        return randomFrom(values());
    }
}
