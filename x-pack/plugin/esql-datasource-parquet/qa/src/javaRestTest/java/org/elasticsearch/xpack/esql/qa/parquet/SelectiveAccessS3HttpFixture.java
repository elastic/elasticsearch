/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import fixture.s3.BlobEntry;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiPredicate;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static fixture.aws.AwsFixtureUtils.sendError;

/**
 * {@link S3HttpFixture} that accepts a mandatory reader key (full object access) and an optional
 * list-only key. When {@code listOnlyKey} is non-null, the list-only key is allowed to list the
 * bucket but receives 403 AccessDenied on any object read. When {@code listOnlyKey} is null the
 * fixture behaves as a plain seeding fixture: all requests authenticated with the reader key are
 * accepted and object reads are tracked.
 *
 * <p>This fixture is used by {@link FooterCacheScopeIT} to:
 * <ul>
 *   <li>Prove that a list-only credential is not answered from another data source's cached Parquet
 *       footer (credential-bypass bug).</li>
 *   <li>Act as the second, separate S3 store when testing endpoint-confusion bugs (listOnlyKey=null).</li>
 * </ul>
 */
@SuppressForbidden(reason = "test fixture seeds blobs directly into the S3 handler's in-memory store")
public class SelectiveAccessS3HttpFixture extends S3HttpFixture {

    private final String bucket;
    private final String readerKey;
    @Nullable
    private final String listOnlyKey;
    private final BiPredicate<String, String> readerPredicate;
    @Nullable
    private final BiPredicate<String, String> listOnlyPredicate;

    /** Access keys that signed object-read requests since the last call to {@link #getAndClearObjectReadKeys}. */
    private final CopyOnWriteArrayList<String> objectReadKeys = new CopyOnWriteArrayList<>();

    private S3HttpHandler handler;

    /**
     * @param bucket      bucket name served by this fixture
     * @param readerKey   access key that may read objects
     * @param listOnlyKey access key that may only list (null disables the second key tier)
     */
    public SelectiveAccessS3HttpFixture(String bucket, String readerKey, @Nullable String listOnlyKey) {
        super(true, null, () -> bucket, () -> "", () -> S3ConsistencyModel.STRONG_MPUS, (auth, token) -> true);
        this.bucket = bucket;
        this.readerKey = readerKey;
        this.listOnlyKey = listOnlyKey;
        this.readerPredicate = fixedAccessKey(readerKey, () -> "*", "s3");
        this.listOnlyPredicate = listOnlyKey != null ? fixedAccessKey(listOnlyKey, () -> "*", "s3") : null;
    }

    @Override
    protected HttpHandler createHandler() {
        handler = new S3HttpHandler(bucket, "", S3ConsistencyModel.STRONG_MPUS);
        return exchange -> {
            try {
                String auth = exchange.getRequestHeaders().getFirst("Authorization");
                String token = exchange.getRequestHeaders().getFirst("x-amz-security-token");
                boolean isReader = readerPredicate.test(auth, token);
                boolean isListOnly = listOnlyPredicate != null && listOnlyPredicate.test(auth, token);

                if (isReader == false && isListOnly == false) {
                    sendError(exchange, RestStatus.FORBIDDEN, "AccessDenied", "Access denied");
                    return;
                }

                if (addressesAnotherBucket(exchange.getRequestURI().getPath())) {
                    sendError(exchange, RestStatus.NOT_FOUND, "NoSuchBucket", "The specified bucket does not exist");
                    return;
                }

                String signingKey = isReader ? readerKey : listOnlyKey;

                if (isObjectRead(exchange)) {
                    objectReadKeys.add(signingKey);
                    if (isListOnly) {
                        sendError(exchange, RestStatus.FORBIDDEN, "AccessDenied", "Access denied reading object (key is list-only)");
                        return;
                    }
                }

                handler.handle(exchange);
            } catch (Error e) {
                ExceptionsHelper.maybeDieOnAnotherThread(e);
                throw e;
            }
        };
    }

    /**
     * Pre-populates a blob. Must be called after the fixture has started (i.e. from {@code @BeforeClass}).
     */
    public void seedBlob(String key, byte[] content) {
        if (handler == null) {
            throw new IllegalStateException("fixture not started; call seedBlob from @BeforeClass");
        }
        handler.blobs().put("/" + bucket + "/" + key, new BlobEntry(new BytesArray(content), "STANDARD"));
    }

    /**
     * Returns and clears the list of access keys that signed object-read requests since the last call.
     */
    public List<String> getAndClearObjectReadKeys() {
        List<String> snapshot = new ArrayList<>(objectReadKeys);
        objectReadKeys.clear();
        return snapshot;
    }

    /**
     * True when the request is a GET or HEAD to a specific object (not a bucket listing or
     * bucket-level HEAD).
     */
    private boolean isObjectRead(HttpExchange exchange) {
        String method = exchange.getRequestMethod();
        if ("GET".equals(method) == false && "HEAD".equals(method) == false) {
            return false;
        }
        String query = exchange.getRequestURI().getQuery();
        if (query != null && (query.contains("list-type") || query.contains("delimiter") || query.contains("prefix="))) {
            return false;
        }
        String path = exchange.getRequestURI().getPath();
        String bucketPrefix = "/" + bucket + "/";
        return path.startsWith(bucketPrefix) && path.length() > bucketPrefix.length();
    }

    private boolean addressesAnotherBucket(String requestPath) {
        return requestPath.startsWith("/" + bucket + "/") == false && requestPath.equals("/" + bucket) == false;
    }
}
