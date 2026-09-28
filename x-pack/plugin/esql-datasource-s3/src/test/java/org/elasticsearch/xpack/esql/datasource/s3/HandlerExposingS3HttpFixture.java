/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.core.SuppressForbidden;

import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;

/**
 * An {@link S3HttpFixture} that exposes its underlying {@link S3HttpHandler} so tests can seed
 * blobs and inspect the request log.
 */
@SuppressForbidden(reason = "overrides S3HttpFixture.createHandler which returns com.sun.net.httpserver.HttpHandler")
public final class HandlerExposingS3HttpFixture extends S3HttpFixture {

    private final String bucket;
    private final String accessKey;
    private S3HttpHandler handler;

    public HandlerExposingS3HttpFixture(String bucket, String accessKey) {
        super(true, () -> S3ConsistencyModel.STRONG_MPUS);
        this.bucket = bucket;
        this.accessKey = accessKey;
    }

    @Override
    protected HttpHandler createHandler() {
        handler = new S3HttpHandler(bucket, null, S3ConsistencyModel.STRONG_MPUS);
        var auth = fixedAccessKey(accessKey, () -> "us-east-1", "s3");
        return exchange -> {
            if (checkAuthorization(auth, exchange)) {
                handler.handle(exchange);
            }
        };
    }

    public S3HttpHandler handler() {
        return handler;
    }
}
