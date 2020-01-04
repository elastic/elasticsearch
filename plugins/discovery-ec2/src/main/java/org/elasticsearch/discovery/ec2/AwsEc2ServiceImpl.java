/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.ec2;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;

import java.util.concurrent.atomic.AtomicReference;

class AwsEc2ServiceImpl implements AwsEc2Service {

    private static final Logger logger = LogManager.getLogger(AwsEc2ServiceImpl.class);

    private final AtomicReference<LazyInitializable<AmazonEc2Reference, ElasticsearchException>> lazyClientReference =
            new AtomicReference<>();

    private AmazonEC2 buildClient(Ec2ClientSettings clientSettings) {
        final AWSCredentialsProvider credentials = buildCredentials(logger, clientSettings);
        final ClientConfiguration configuration = buildConfiguration(clientSettings);
        final AmazonEC2 client = buildClient(credentials, configuration);
        if (Strings.hasText(clientSettings.endpoint)) {
            logger.debug("using explicit ec2 endpoint [{}]", clientSettings.endpoint);
            client.setEndpoint(clientSettings.endpoint);
        }
        return client;
    }

    // proxy for testing
    AmazonEC2 buildClient(AWSCredentialsProvider credentials, ClientConfiguration configuration) {
        final AmazonEC2 client = new AmazonEC2Client(credentials, configuration);
        return client;
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
                () -> new AmazonEc2Reference(buildClient(clientSettings)), clientReference -> clientReference.incRef(),
                clientReference -> clientReference.decRef());
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
