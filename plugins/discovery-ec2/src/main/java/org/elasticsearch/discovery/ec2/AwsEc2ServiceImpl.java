/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.atomic.AtomicReference;

class AwsEc2ServiceImpl implements AwsEc2Service {

    private static final Logger LOGGER = LogManager.getLogger(AwsEc2ServiceImpl.class);

    private final AtomicReference<LazyInitializable<AmazonEc2Reference, ElasticsearchException>> lazyClientReference =
        new AtomicReference<>();

    private AmazonEC2 buildClient(Ec2ClientSettings clientSettings) {
        final AWSCredentialsProvider credentials = buildCredentials(LOGGER, clientSettings);
        final ClientConfiguration configuration = buildConfiguration(clientSettings);
        return buildClient(credentials, configuration, clientSettings.endpoint);
    }

    // proxy for testing
    AmazonEC2 buildClient(AWSCredentialsProvider credentials, ClientConfiguration configuration, String endpoint) {
        final AmazonEC2ClientBuilder builder = AmazonEC2ClientBuilder.standard()
            .withCredentials(credentials)
            .withClientConfiguration(configuration);
        if (Strings.hasText(endpoint)) {
            LOGGER.debug("using explicit ec2 endpoint [{}]", endpoint);
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null));
        }
        return SocketAccess.doPrivileged(builder::build);
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(Ec2ClientSettings clientSettings) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(clientSettings.protocol);
        if (Strings.hasText(clientSettings.proxyHost)) {
            // TODO: remove this leniency, these settings should exist together and be validated
            clientConfiguration.setProxyHost(clientSettings.proxyHost);
            clientConfiguration.setProxyPort(clientSettings.proxyPort);
            clientConfiguration.setProxyUsername(clientSettings.proxyUsername);
            clientConfiguration.setProxyPassword(clientSettings.proxyPassword);
        }
        // Increase the number of retries in case of 5xx API responses
        clientConfiguration.setMaxErrorRetry(10);
        clientConfiguration.setSocketTimeout(clientSettings.readTimeoutMillis);
        return clientConfiguration;
    }

    // pkg private for tests
    static AWSCredentialsProvider buildCredentials(Logger logger, Ec2ClientSettings clientSettings) {
        final AWSCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            logger.debug("Using default provider chain");
            return DefaultAWSCredentialsProviderChain.getInstance();
        } else {
            logger.debug("Using basic key/secret credentials");
            return new AWSStaticCredentialsProvider(credentials);
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
     * Refreshes the settings for the AmazonEC2 client. The new client will be build
     * using these new settings. The old client is usable until released. On release it
     * will be destroyed instead of being returned to the cache.
     */
    @Override
    public void refreshAndClearCache(Ec2ClientSettings clientSettings) {
        final LazyInitializable<AmazonEc2Reference, ElasticsearchException> newClient = new LazyInitializable<>(
            () -> new AmazonEc2Reference(buildClient(clientSettings)),
            clientReference -> clientReference.incRef(),
            clientReference -> clientReference.decRef()
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
        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

}
