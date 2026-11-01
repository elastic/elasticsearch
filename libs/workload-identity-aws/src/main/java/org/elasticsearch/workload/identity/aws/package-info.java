/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Fully asynchronous, non-blocking implementations of the AWS SDK v2
 * {@link software.amazon.awssdk.identity.spi.IdentityProvider} contract for
 * {@link software.amazon.awssdk.identity.spi.AwsCredentialsIdentity}, intended for use with the AWS async
 * clients (for example {@code S3AsyncClient}).
 *
 * <p>Unlike the stock {@code StsAssumeRoleWithWebIdentityCredentialsProvider}, which only implements
 * the blocking {@code resolveCredentials()} and relies on a blocking cache, the providers here
 * implement {@code resolveIdentity()} to return a {@link java.util.concurrent.CompletableFuture}
 * backed by asynchronous I/O so that the calling thread is never parked while credentials are
 * fetched or refreshed.
 */
package org.elasticsearch.workload.identity.aws;
