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

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;

class AwsEc2ServiceImpl extends AbstractComponent implements AwsEc2Service, Closeable {

    public static final String EC2_METADATA_URL = "http://169.254.169.254/latest/meta-data/";

    private AmazonEC2Client client;

    AwsEc2ServiceImpl(Settings settings) {
        super(settings);
    }

    @Override
    public synchronized AmazonEC2 client() {
        if (client != null) {
            return client;
        }

        this.client = new AmazonEC2Client(buildCredentials(logger, settings), buildConfiguration(logger, settings));
        String endpoint = findEndpoint(logger, settings);
        if (endpoint != null) {
            client.setEndpoint(endpoint);
        }

        return this.client;
    }

    protected static AWSCredentialsProvider buildCredentials(Logger logger, Settings settings) {
        AWSCredentialsProvider credentials;

        try (SecureString key = ACCESS_KEY_SETTING.get(settings);
             SecureString secret = SECRET_KEY_SETTING.get(settings)) {
            if (key.length() == 0 && secret.length() == 0) {
                logger.debug("Using either environment variables, system properties or instance profile credentials");
                credentials = new DefaultAWSCredentialsProviderChain();
            } else {
                logger.debug("Using basic key/secret credentials");
                credentials = new StaticCredentialsProvider(new BasicAWSCredentials(key.toString(), secret.toString()));
            }
        }

        return credentials;
    }

    protected static ClientConfiguration buildConfiguration(Logger logger, Settings settings) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(PROTOCOL_SETTING.get(settings));

        if (PROXY_HOST_SETTING.exists(settings)) {
            String proxyHost = PROXY_HOST_SETTING.get(settings);
            Integer proxyPort = PROXY_PORT_SETTING.get(settings);
            try (SecureString proxyUsername = PROXY_USERNAME_SETTING.get(settings);
                 SecureString proxyPassword = PROXY_PASSWORD_SETTING.get(settings)) {

                clientConfiguration
                    .withProxyHost(proxyHost)
                    .withProxyPort(proxyPort)
                    .withProxyUsername(proxyUsername.toString())
                    .withProxyPassword(proxyPassword.toString());
            }
        }

        // Increase the number of retries in case of 5xx API responses
        final Random rand = Randomness.get();
        RetryPolicy retryPolicy = new RetryPolicy(
            RetryPolicy.RetryCondition.NO_RETRY_CONDITION,
            new RetryPolicy.BackoffStrategy() {
                @Override
                public long delayBeforeNextRetry(AmazonWebServiceRequest originalRequest,
                                                 AmazonClientException exception,
                                                 int retriesAttempted) {
                    // with 10 retries the max delay time is 320s/320000ms (10 * 2^5 * 1 * 1000)
                    logger.warn("EC2 API request failed, retry again. Reason was:", exception);
                    return 1000L * (long) (10d * Math.pow(2, retriesAttempted / 2.0d) * (1.0d + rand.nextDouble()));
                }
            },
            10,
            false);
        clientConfiguration.setRetryPolicy(retryPolicy);
        clientConfiguration.setSocketTimeout((int) READ_TIMEOUT_SETTING.get(settings).millis());

        return clientConfiguration;
    }

    protected static String findEndpoint(Logger logger, Settings settings) {
        String endpoint = null;
        if (ENDPOINT_SETTING.exists(settings)) {
            endpoint = ENDPOINT_SETTING.get(settings);
            logger.debug("using explicit ec2 endpoint [{}]", endpoint);
        }
        return endpoint;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.shutdown();
        }

        // Ensure that IdleConnectionReaper is shutdown
        IdleConnectionReaper.shutdown();
    }
}
