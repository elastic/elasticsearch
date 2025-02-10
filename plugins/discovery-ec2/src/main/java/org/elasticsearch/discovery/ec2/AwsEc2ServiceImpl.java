/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.ec2.Ec2Client;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

class AwsEc2ServiceImpl implements AwsEc2Service {

    private static final Logger LOGGER = LogManager.getLogger(AwsEc2ServiceImpl.class);

    private final AtomicReference<LazyInitializable<AmazonEc2Reference, ElasticsearchException>> lazyClientReference =
        new AtomicReference<>();

    private Ec2Client buildClient(Ec2ClientSettings clientSettings) {
        final var credentials = buildCredentials(LOGGER, clientSettings);
        return buildClient(credentials, clientSettings);
    }

    // proxy for testing
    Ec2Client buildClient(AwsCredentialsProvider credentials, Ec2ClientSettings clientSettings) {
        // TODO NOMERGE protocol no longer supported? clientConfiguration.setProtocol(clientSettings.protocol);

        final var httpClientBuilder = ApacheHttpClient.builder();
        httpClientBuilder.socketTimeout(Duration.of(clientSettings.readTimeoutMillis, ChronoUnit.MILLIS));

        if (Strings.hasText(clientSettings.proxyHost)) {
            // TODO: remove this leniency, these settings should exist together and be validated
            final var uriBuilder = new URIBuilder();
            uriBuilder.setScheme(clientSettings.proxyScheme.getSchemeString())
                .setHost(clientSettings.proxyHost)
                .setPort(clientSettings.proxyPort);
            final URI proxyUri;
            try {
                proxyUri = uriBuilder.build();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }

            httpClientBuilder.proxyConfiguration(
                ProxyConfiguration.builder()
                    .endpoint(proxyUri)
                    .scheme(clientSettings.proxyScheme.getSchemeString())
                    .username(clientSettings.proxyUsername)
                    .password(clientSettings.proxyPassword)
                    .build()
            );
        }

        final var ec2ClientBuilder = Ec2Client.builder();
        ec2ClientBuilder.credentialsProvider(credentials);
        ec2ClientBuilder.httpClientBuilder(httpClientBuilder);

        // Increase the number of retries in case of 5xx API responses
        ec2ClientBuilder.overrideConfiguration(b -> b.retryStrategy(c -> c.maxAttempts(10)));

        if (Strings.hasText(clientSettings.endpoint)) {
            LOGGER.debug("using explicit ec2 endpoint [{}]", clientSettings.endpoint);
            final var endpoint = Endpoint.builder().url(URI.create(clientSettings.endpoint)).build();
            ec2ClientBuilder.endpointProvider(endpointParams -> CompletableFuture.completedFuture(endpoint));
        }
        return SocketAccess.doPrivileged(ec2ClientBuilder::build);
    }

    // TODO NOMERGE reinstate client settings

    // pkg private for tests
    static AwsCredentialsProvider buildCredentials(Logger logger, Ec2ClientSettings clientSettings) {
        final AwsCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            logger.debug("Using default provider chain");
            return DefaultCredentialsProvider.create();
        } else {
            logger.debug("Using basic key/secret credentials");
            return StaticCredentialsProvider.create(credentials);
        }
    }

    @Override
    public AmazonEc2Reference client() {
        final LazyInitializable<AmazonEc2Reference, ElasticsearchException> clientReference = this.lazyClientReference.get();
        if (clientReference == null) {
            throw new IllegalStateException("Missing ec2 client configs");
        }
        return clientReference.getOrCompute();
    }

    /**
     * Refreshes the settings for the AmazonEC2 client. The new client will be built using these new settings. The old client is usable
     * until released. On release it will be destroyed instead of being returned to the cache.
     */
    @Override
    public void refreshAndClearCache(Ec2ClientSettings clientSettings) {
        final LazyInitializable<AmazonEc2Reference, ElasticsearchException> newClient = new LazyInitializable<>(
            () -> new AmazonEc2Reference(buildClient(clientSettings)),
            AbstractRefCounted::incRef,
            AbstractRefCounted::decRef
        );
        final LazyInitializable<AmazonEc2Reference, ElasticsearchException> oldClient = this.lazyClientReference.getAndSet(newClient);
        if (oldClient != null) {
            oldClient.reset();
        }
    }

    @Override
    public void close() {
        final LazyInitializable<AmazonEc2Reference, ElasticsearchException> clientReference = this.lazyClientReference.getAndSet(null);
        if (clientReference != null) {
            clientReference.reset();
        }
    }

}
