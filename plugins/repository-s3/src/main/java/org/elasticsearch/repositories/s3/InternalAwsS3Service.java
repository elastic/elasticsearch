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

package org.elasticsearch.repositories.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.CryptoMode;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;
import com.amazonaws.util.Base64;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;


import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


class InternalAwsS3Service extends AbstractLifecycleComponent implements AwsS3Service {

    // pkg private for tests
    static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    private final Map<String, S3ClientSettings> clientsSettings;

    private final Map<String, AmazonS3Client> clientsCache = new HashMap<>();

    InternalAwsS3Service(Settings settings, Map<String, S3ClientSettings> clientsSettings) {
        super(settings);
        this.clientsSettings = clientsSettings;
    }

    @Override
    public synchronized AmazonS3 client(Settings repositorySettings) {
        String clientName = CLIENT_NAME.get(repositorySettings);
        AmazonS3Client client = clientsCache.get(clientName);
        if (client != null) {
            return client;
        }

        S3ClientSettings clientSettings = clientsSettings.get(clientName);
        if (clientSettings == null) {
            throw new IllegalArgumentException("Unknown s3 client name [" + clientName + "]. Existing client configs: " +
                Strings.collectionToDelimitedString(clientsSettings.keySet(), ","));
        }

        logger.debug("creating S3 client with client_name [{}], endpoint [{}]", clientName, clientSettings.endpoint);

        AWSCredentialsProvider credentials = buildCredentials(logger, deprecationLogger, clientSettings, repositorySettings);
        ClientConfiguration configuration = buildConfiguration(clientSettings);

        if (S3Repository.CLIENT_SIDE_ENCRYPTION_SETTING.exists(repositorySettings) &&
            Boolean.parseBoolean(S3Repository.CLIENT_SIDE_ENCRYPTION_SETTING.get(repositorySettings).toString())) {

            logger.debug("Client side encryption is enabled");

            client = new AmazonS3EncryptionClient(
                credentials,
                new StaticEncryptionMaterialsProvider(buildClientSideEncryption(logger, repositorySettings)),
                configuration,
                new CryptoConfiguration(CryptoMode.EncryptionOnly));

        } else {
            client = new AmazonS3Client(credentials, configuration);
        }

        if (Strings.hasText(clientSettings.endpoint)) {
            client.setEndpoint(clientSettings.endpoint);
        }

        clientsCache.put(clientName, client);
        return client;
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(S3ClientSettings clientSettings) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
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

        clientConfiguration.setMaxErrorRetry(clientSettings.maxRetries);
        clientConfiguration.setUseThrottleRetries(clientSettings.throttleRetries);
        clientConfiguration.setSocketTimeout(clientSettings.readTimeoutMillis);

        return clientConfiguration;
    }

    // pkg private for tests
    static AWSCredentialsProvider buildCredentials(Logger logger, DeprecationLogger deprecationLogger,
                                                   S3ClientSettings clientSettings, Settings repositorySettings) {


        BasicAWSCredentials credentials = clientSettings.credentials;
        if (S3Repository.ACCESS_KEY_SETTING.exists(repositorySettings)) {
            if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings) == false) {
                throw new IllegalArgumentException("Repository setting [" + S3Repository.ACCESS_KEY_SETTING.getKey() +
                    " must be accompanied by setting [" + S3Repository.SECRET_KEY_SETTING.getKey() + "]");
            }
            try (SecureString key = S3Repository.ACCESS_KEY_SETTING.get(repositorySettings);
                 SecureString secret = S3Repository.SECRET_KEY_SETTING.get(repositorySettings)) {
                credentials = new BasicAWSCredentials(key.toString(), secret.toString());
            }
            // backcompat for reading keys out of repository settings
            deprecationLogger.deprecated("Using s3 access/secret key from repository settings. Instead " +
                "store these in named clients and the elasticsearch keystore for secure settings.");
        } else if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings)) {
            throw new IllegalArgumentException("Repository setting [" + S3Repository.SECRET_KEY_SETTING.getKey() +
                " must be accompanied by setting [" + S3Repository.ACCESS_KEY_SETTING.getKey() + "]");
        }
        if (credentials == null) {
            logger.debug("Using instance profile credentials");
            return new PrivilegedInstanceProfileCredentialsProvider();
        } else {
            logger.debug("Using basic key/secret credentials");
            return new StaticCredentialsProvider(credentials);
        }
    }

    static EncryptionMaterials buildClientSideEncryption(Logger logger, Settings repositorySettings) {

        EncryptionMaterials clientSideEncryptionMaterials = null;
        if (S3Repository.CLIENT_SIDE_ENCRYPTION_SETTING.exists(repositorySettings) &&
            Boolean.parseBoolean(S3Repository.CLIENT_SIDE_ENCRYPTION_SETTING.get(repositorySettings).toString())) {

            String symmetricKey = S3Repository.CLIENT_SYMMETRIC_KEY_SETTING.get(repositorySettings).toString();
            String publicKey = S3Repository.CLIENT_PUBLIC_KEY_SETTING.get(repositorySettings);
            String privateKey = S3Repository.CLIENT_PRIVATE_KEY_SETTING.get(repositorySettings).toString();

            if (Strings.isNullOrEmpty(symmetricKey) == false && (Strings.isNullOrEmpty(publicKey) == false ||
                Strings.isNullOrEmpty(privateKey) == false)) {
                throw new IllegalArgumentException("Client-side encryption: You can't specify a symmetric key " +
                    "AND a public/private key pair");
            }

            if (Strings.isNullOrEmpty(symmetricKey) == false || Strings.isNullOrEmpty(publicKey) == false ||
                Strings.isNullOrEmpty(privateKey) == false) {
                try {
                    // Check crypto
                    if (Cipher.getMaxAllowedKeyLength("AES") < 256) {
                        throw new IllegalArgumentException("Client-side encryption: Please install the Java " +
                            "Cryptography Extension");
                    }

                    // Transform the keys in a EncryptionMaterials
                    if (Strings.isNullOrEmpty(symmetricKey) == false) {
                        clientSideEncryptionMaterials = new EncryptionMaterials(new SecretKeySpec(
                            Base64.decode(symmetricKey), "AES"));
                    } else {
                        if (Strings.isNullOrEmpty(publicKey) || Strings.isNullOrEmpty(privateKey)) {
                            String missingKey = Strings.isNullOrEmpty(publicKey) ? "public key" : "private key";
                            throw new IllegalArgumentException("Client-side encryption: " + missingKey + " is missing");
                        }

                        clientSideEncryptionMaterials = new EncryptionMaterials(new KeyPair(
                            KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(Base64.decode(publicKey))),
                            KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(
                                Base64.decode(privateKey)))));
                    }

                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Client-side encryption: Error decoding your keys: " + e.getMessage());
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalArgumentException(e.getMessage());
                } catch (InvalidKeySpecException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            }
        }

        return clientSideEncryptionMaterials;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        for (AmazonS3Client client : clientsCache.values()) {
            client.shutdown();
        }

        // Ensure that IdleConnectionReaper is shutdown
        IdleConnectionReaper.shutdown();
    }

    static class PrivilegedInstanceProfileCredentialsProvider implements AWSCredentialsProvider {
        private final InstanceProfileCredentialsProvider credentials;

        private PrivilegedInstanceProfileCredentialsProvider() {
            this.credentials = new InstanceProfileCredentialsProvider();
        }

        @Override
        public AWSCredentials getCredentials() {
            return SocketAccess.doPrivileged(credentials::getCredentials);
        }

        @Override
        public void refresh() {
            SocketAccess.doPrivilegedVoid(credentials::refresh);
        }
    }
}
