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
import org.elasticsearch.rest.RestStatus;

import java.util.function.BiPredicate;

import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsFixtureUtils.sendError;

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
 *
 * <p>Calling {@link #setCorrectRegion} switches the fixture into region-discovery mode: requests
 * signed for any region other than the configured one receive 400 AuthorizationHeaderMalformed
 * (HEAD bucket also carries {@code x-amz-bucket-region}), exactly as a custom-endpoint store like
 * Scaleway would. Correctly-signed requests proceed through normal auth and handler logic.
 */
@SuppressForbidden(reason = "test fixture seeds blobs directly into the S3 handler's in-memory store")
public class SeedingS3HttpFixture extends S3HttpFixture {

    private final String bucket;
    private final BiPredicate<String, String> authorizationPredicate;
    private S3HttpHandler handler;

    /**
     * When non-null, requests signed for a different region receive 400 AuthorizationHeaderMalformed
     * (or, for HEAD bucket, 400 + {@code x-amz-bucket-region}) before the normal auth check runs.
     */
    private volatile String correctRegion;

    public SeedingS3HttpFixture(String bucket, BiPredicate<String, String> authorizationPredicate) {
        super(
            true,
            null,
            () -> bucket,
            () -> "" /* no base path: external sources address objects by full key */,
            () -> S3ConsistencyModel.STRONG_MPUS,
            authorizationPredicate
        );
        this.bucket = bucket;
        this.authorizationPredicate = authorizationPredicate;
    }

    /**
     * Configures region-discovery mode. After this call, any S3 request signed for a region other
     * than {@code region} will receive 400 AuthorizationHeaderMalformed (HEAD bucket additionally
     * carries {@code x-amz-bucket-region: region}), simulating a custom endpoint that validates the
     * signing region. Must be called after the fixture has started (i.e. from {@code @BeforeClass}).
     */
    public void setCorrectRegion(String region) {
        this.correctRegion = region;
    }

    @Override
    protected HttpHandler createHandler() {
        handler = new S3HttpHandler(bucket, "", S3ConsistencyModel.STRONG_MPUS);
        return exchange -> {
            try {
                // Region-validating mode: simulate a custom endpoint that rejects wrong-region signing.
                final String cr = correctRegion;
                if (cr != null) {
                    String signingRegion = extractSigningRegion(exchange.getRequestHeaders().getFirst("Authorization"));
                    if (cr.equals(signingRegion) == false) {
                        if ("HEAD".equals(exchange.getRequestMethod()) && isHeadBucketPath(exchange.getRequestURI().getPath())) {
                            // HEAD bucket: include x-amz-bucket-region so the provider discovers the correct region.
                            exchange.getResponseHeaders().add("x-amz-bucket-region", cr);
                        }
                        sendError(
                            exchange,
                            RestStatus.BAD_REQUEST,
                            "AuthorizationHeaderMalformed",
                            "The authorization header is malformed; the region '" + signingRegion + "' is wrong; expecting '" + cr + "'"
                        );
                        return;
                    }
                }
                if (checkAuthorization(authorizationPredicate, exchange)) {
                    if (addressesAnotherBucket(exchange.getRequestURI().getPath())) {
                        // S3 answers a request for a bucket that does not exist with 404 NoSuchBucket;
                        // the shared handler answers 500, which would make a probe measure the fixture.
                        sendError(exchange, RestStatus.NOT_FOUND, "NoSuchBucket", "The specified bucket does not exist");
                        return;
                    }
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

    /**
     * True when the request addresses a bucket this fixture does not serve. The shared {@link S3HttpHandler}
     * answers those with a 500, which is not what S3 does — it returns 404 NoSuchBucket — so a probe pointed at
     * a missing bucket would otherwise be measuring the fixture rather than the product.
     */
    private boolean addressesAnotherBucket(String requestPath) {
        return requestPath.startsWith("/" + bucket + "/") == false && requestPath.equals("/" + bucket) == false;
    }

    private boolean isHeadBucketPath(String requestPath) {
        return requestPath.equals("/" + bucket) || requestPath.equals("/" + bucket + "/");
    }

    /**
     * Extracts the signing region from an AWS v4 Authorization header.
     * Header format: {@code AWS4-HMAC-SHA256 Credential=KEY/YYYYMMDD/REGION/SERVICE/aws4_request, ...}
     * Returns {@code null} when the header is absent or does not follow that format.
     */
    static String extractSigningRegion(String authorizationHeader) {
        if (authorizationHeader == null) {
            return null;
        }
        int credIdx = authorizationHeader.indexOf("Credential=");
        if (credIdx < 0) {
            return null;
        }
        // Skip "Credential=", then split on "/" to get [accessKey, date, region, service, ...]
        String[] parts = authorizationHeader.substring(credIdx + "Credential=".length()).split("/");
        return parts.length >= 3 ? parts[2] : null;
    }

}
