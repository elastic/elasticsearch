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

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Lazy;

class AwsEc2ServiceImpl extends AbstractComponent implements AwsEc2Service {

    public static final String EC2_METADATA_URL = "http://169.254.169.254/latest/meta-data/";

    private final AtomicReference<LazyAmazonEc2Reference> lazyClientReference = new AtomicReference<>();

    AwsEc2ServiceImpl(Settings settings) {
        super(settings);
    }

    private AmazonEC2 buildClient(Ec2ClientSettings clientSettings) {
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
    static ClientConfiguration buildConfiguration(Logger logger, Ec2ClientSettings clientSettings) {
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
    static AWSCredentialsProvider buildCredentials(Logger logger, Ec2ClientSettings clientSettings) {
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
    public AmazonEc2Reference client() {
        final LazyAmazonEc2Reference clientReference = this.lazyClientReference.get();
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
        final LazyAmazonEc2Reference newClient = new LazyAmazonEc2Reference(() -> buildClient(clientSettings));
        final LazyAmazonEc2Reference oldClient = this.lazyClientReference.getAndSet(newClient);
        if (oldClient != null) {
            oldClient.clear();
        }
    }

    @Override
    public void close() {
        final LazyAmazonEc2Reference clientReference = this.lazyClientReference.getAndSet(null);
        if (clientReference != null) {
            clientReference.clear();
        }
        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

    private static class LazyAmazonEc2Reference extends Lazy<AmazonEc2Reference, Exception> {

        LazyAmazonEc2Reference(Supplier<AmazonEC2> supplier) {
            super(() -> {
                final AmazonEC2 client = Objects.requireNonNull(supplier.get());
                // wrap supplier result in a reference counted object with initial count set to 1
                final AmazonEc2Reference clientReference = new AmazonEc2Reference(client);
                clientReference.incRef();
                return clientReference;
            }, clientReference -> clientReference.decRef());
        }

        @Override
        public AmazonEc2Reference getOrCompute() {
            AmazonEc2Reference result;
            try {
                result = super.getOrCompute();
            } catch (final Exception e) {
                throw new ElasticsearchException("This is unexpected", e);
            }
            // the count will be decreased by {@codee AmazonEc2Reference#close}
            result.incRef();
            return result;
        }
    }

}
