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

import java.util.Random;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

class AwsEc2ServiceImpl extends AbstractComponent implements AwsEc2Service {

    public static final String EC2_METADATA_URL = "http://169.254.169.254/latest/meta-data/";

    private volatile AmazonEC2Reference clientReference;
    private volatile EC2ClientSettings clientSettings;

    AwsEc2ServiceImpl(Settings settings) {
        super(settings);
    }

    private AmazonEC2 buildClient(EC2ClientSettings clientSettings) {
        final AWSCredentialsProvider credentials = buildCredentials(logger, clientSettings);
        final ClientConfiguration configuration = buildConfiguration(logger, clientSettings);
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
    static ClientConfiguration buildConfiguration(Logger logger, EC2ClientSettings clientSettings) {
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
        final Random rand = Randomness.get();
        final RetryPolicy retryPolicy = new RetryPolicy(
            RetryPolicy.RetryCondition.NO_RETRY_CONDITION,
            (originalRequest, exception, retriesAttempted) -> {
               // with 10 retries the max delay time is 320s/320000ms (10 * 2^5 * 1 * 1000)
               logger.warn("EC2 API request failed, retry again. Reason was:", exception);
               return 1000L * (long) (10d * Math.pow(2, retriesAttempted / 2.0d) * (1.0d + rand.nextDouble()));
            },
            10,
            false);
        clientConfiguration.setRetryPolicy(retryPolicy);
        clientConfiguration.setSocketTimeout(clientSettings.readTimeoutMillis);
        return clientConfiguration;
    }

    // pkg private for tests
    static AWSCredentialsProvider buildCredentials(Logger logger, EC2ClientSettings clientSettings) {
        final BasicAWSCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            logger.debug("Using either environment variables, system properties or instance profile credentials");
            return new DefaultAWSCredentialsProviderChain();
        } else {
            logger.debug("Using basic key/secret credentials");
            return new StaticCredentialsProvider(credentials);
        }
    }

    @Override
    public AmazonEC2Reference client() {
        if ((clientReference != null) && clientReference.tryIncRef()) {
            return clientReference;
        }
        synchronized (this) {
            if ((clientReference != null) && clientReference.tryIncRef()) {
                return clientReference;
            }
            if (clientSettings == null) {
                throw new IllegalArgumentException("Missing ec2 client configs.");
            }
            final AmazonEC2Reference clientReference = new AmazonEC2Reference(buildClient(clientSettings));
            clientReference.incRef();
            this.clientReference = clientReference;
            return clientReference;
        }
    }


    /**
     * Reloads the settings for the AmazonEC2 client. New clients will be build
     * using these. Old client is usable until released. On release it will be
     * destroyed instead of being returned to the cache.
     */
    @Override
    public synchronized EC2ClientSettings updateClientSettings(EC2ClientSettings clientSettings) {
        // shutdown all unused clients
        // others will shutdown on their respective release
        releaseCachedClient();
        final EC2ClientSettings prevSettings = this.clientSettings;
        this.clientSettings = clientSettings;
        return prevSettings;
    }

    @Override
    public synchronized void releaseCachedClient() {
        if (this.clientReference == null) {
            return;
        }
        // the client will shutdown when it will not be used anymore
        this.clientReference.decRef();
        // clear the cached client, it will be build lazily
        this.clientReference = null;
        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }
}
