/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import fixture.s3.BlobEntry;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.SuppressForbidden;

import java.util.function.BiPredicate;

import static fixture.aws.AwsCredentialsUtils.checkAuthorization;

/**
 * {@link S3HttpFixture} subclass that captures the underlying {@link S3HttpHandler} so the test JVM
 * can pre-populate blobs via {@link #seedBlob}. The standard fixture wraps its handler in an
 * anonymous class with no accessor, which prevents seeding from outside the cluster JVM.
 *
 * <p>Auth validation behaves exactly like the parent class for cluster traffic; the parent's
 * {@code user-agent} assertion is intentionally dropped here because this plugin (unlike
 * {@code repository-s3}) does not customize the AWS SDK's {@code User-Agent} header. Seeding
 * bypasses auth entirely because it writes directly into the in-memory blob map rather than over
 * HTTP.
 */
@SuppressForbidden(reason = "test fixture seeds blobs directly into the S3 handler's in-memory store")
public class SeedingS3HttpFixture extends S3HttpFixture {

    private final String bucket;
    private final BiPredicate<String, String> authorizationPredicate;
    private S3HttpHandler handler;

    public SeedingS3HttpFixture(String bucket, BiPredicate<String, String> authorizationPredicate) {
        super(
            true,
            null,
            bucket,
            "" /* no base path: external sources address objects by full key */,
            () -> S3ConsistencyModel.STRONG_MPUS,
            authorizationPredicate
        );
        this.bucket = bucket;
        this.authorizationPredicate = authorizationPredicate;
    }

    @Override
    protected HttpHandler createHandler() {
        handler = new S3HttpHandler(bucket, "", S3ConsistencyModel.STRONG_MPUS);
        return exchange -> {
            try {
                if (checkAuthorization(authorizationPredicate, exchange)) {
                    handler.handle(exchange);
                }
            } catch (Error e) {
                ExceptionsHelper.maybeDieOnAnotherThread(e);
                throw e;
            }
        };
    }

    /**
     * Pre-populates a blob in the fixture's in-memory store. Must be called after the fixture's
     * {@code @ClassRule} {@code before()} hook has run (i.e. inside {@code @BeforeClass} when the
     * fixture is wrapped in a {@link org.junit.rules.RuleChain}).
     */
    public void seedBlob(String key, byte[] content) {
        if (handler == null) {
            throw new IllegalStateException("S3 fixture has not been started yet; call seedBlob from @BeforeClass");
        }
        handler.blobs().put("/" + bucket + "/" + key, new BlobEntry(new BytesArray(content), "STANDARD"));
    }

}
