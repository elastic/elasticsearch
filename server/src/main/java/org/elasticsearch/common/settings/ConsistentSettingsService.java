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

package org.elasticsearch.common.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public final class ConsistentSettingsService {
    private static final Logger logger = LogManager.getLogger(ConsistentSettingsService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final Collection<Setting<?>> secureSettingsCollection;
    private final SecretKeyFactory PBKDF2KeyFactory;

    public ConsistentSettingsService(Settings settings, ClusterService clusterService,
            Collection<Setting<?>> secureSettingsCollection) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.secureSettingsCollection = secureSettingsCollection;
        // this is used to compute the PBKDF2 hash (the published one)
        try {
            this.PBKDF2KeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("The \"PBKDF2WithHmacSHA512\" algorithm is required for consistent secure settings' hashes", e);
        }
    }

    public LocalNodeMasterListener newHashPublisher() {
        return new LocalNodeMasterListener() {

            // eagerly compute hashes to be published
            final Map<String, String> computedHashesOfConsistentSettings = computeHashesOfConsistentSecureSettings();

            @Override
            public void onMaster() {
                clusterService.submitStateUpdateTask("publish-secure-settings-hashes", new ClusterStateUpdateTask(Priority.URGENT) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final Map<String, String> publishedHashesOfConsistentSettings = currentState.metaData()
                                .hashesOfConsistentSettings();
                        if (computedHashesOfConsistentSettings.equals(publishedHashesOfConsistentSettings)) {
                            logger.debug("Nothing to publish. What is already published matches this master's view.");
                            return currentState;
                        } else {
                            return ClusterState.builder(currentState).metaData(MetaData.builder(currentState.metaData())
                                    .hashesOfConsistentSettings(computedHashesOfConsistentSettings)).build();
                        }
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error("unable to publish secure settings hashes", e);
                    }

                });
            }

            @Override
            public void offMaster() {
                logger.trace("I am no longer master, nothing to do");
            }

            @Override
            public String executorName() {
                return ThreadPool.Names.SAME;
            }
        };
    }

    public boolean areAllConsistent() {
        final ClusterState state = clusterService.state();
        final Map<String, String> publishedHashesOfConsistentSettings = state.metaData().hashesOfConsistentSettings();
        final AtomicBoolean allConsistent = new AtomicBoolean(true);
        forEachConcreteSecureSettingDo(concreteSecureSetting -> {
            final String publishedSaltAndHash = publishedHashesOfConsistentSettings.get(concreteSecureSetting.getKey());
            if (publishedSaltAndHash == null) {
                logger.warn("no published hash for consistent secure setting [{}]", concreteSecureSetting.getKey());
                if (state.nodes().isLocalNodeElectedMaster()) {
                    throw new IllegalStateException("Master node cannot validate consistent setting. No published hash for ["
                            + concreteSecureSetting.getKey() + "].");
                }
                allConsistent.set(false);
            } else {
                final String[] parts = publishedSaltAndHash.split(":");
                if (parts == null || parts.length != 2) {
                    throw new IllegalArgumentException("published hash [" + publishedSaltAndHash + " ] for secure setting ["
                            + concreteSecureSetting.getKey() + "] is invalid");
                }
                final String publishedSalt = parts[0];
                final String publishedHash = parts[1];
                final byte[] localHash = concreteSecureSetting.getSecretValueSHA256(settings);
                final byte[] computedSaltedHashBytes = computeSaltedPBKDF2Hash(localHash, publishedSalt.getBytes(StandardCharsets.UTF_8));
                final String computedSaltedHash = new String(Base64.getEncoder().encode(computedSaltedHashBytes), StandardCharsets.UTF_8);
                if (false == publishedHash.equals(computedSaltedHash)) {
                    logger.warn("the published hash [{}] of the consistent secure setting [{}] differs from the locally computed one [{}]",
                            publishedHash, concreteSecureSetting.getKey(), computedSaltedHash);
                    if (state.nodes().isLocalNodeElectedMaster()) {
                        throw new IllegalStateException("Master node cannot validate consistent setting. The published hash ["
                                + publishedHash + "] of the consistent secure setting [" + concreteSecureSetting.getKey()
                                + "] differs from the locally computed one [" + computedSaltedHash + "].");
                    }
                    allConsistent.set(false);
                }
            }
        });
        return allConsistent.get();
    }

    private void forEachConcreteSecureSettingDo(Consumer<SecureSetting<?>> secureSettingConsumer) {
        for (Setting<?> setting : secureSettingsCollection) {
            assert setting.isConsistent() : "[" + setting.getKey() + "] is not a consistent setting";
            if (setting instanceof Setting.AffixSetting<?>) {
                ((Setting.AffixSetting<?>)setting).getAllConcreteSettings(settings).forEach(concreteSetting -> {
                    assert concreteSetting instanceof SecureSetting<?> : "[" + concreteSetting.getKey() + "] is not a secure setting";
                    secureSettingConsumer.accept((SecureSetting<?>)concreteSetting);
                });
            } else if (setting instanceof SecureSetting<?>) {
                secureSettingConsumer.accept((SecureSetting<?>) setting);
            } else {
                assert false : "Unrecognized consistent secure setting [" + setting.getKey() + "]";
            }
        }
    }

    private Map<String, String> computeHashesOfConsistentSecureSettings() {
        final Map<String, String> result = new HashMap<>();
        forEachConcreteSecureSettingDo(concreteSecureSetting -> {
            final byte[] localHash = concreteSecureSetting.getSecretValueSHA256(settings);
            if (localHash != null) {
                final String salt = UUIDs.randomBase64UUID();
                final byte[] publicHash = computeSaltedPBKDF2Hash(localHash, salt.getBytes(StandardCharsets.UTF_8));
                final String encodedPublicHash = new String(Base64.getEncoder().encode(publicHash), StandardCharsets.UTF_8);
                result.put(concreteSecureSetting.getKey(), salt + ":" + encodedPublicHash);
            }
        });
        return result;
    }

    private byte[] computeSaltedPBKDF2Hash(byte[] bytes, byte[] salt) {
        final int iterations = 5000;
        final int keyLength = 512;
        char[] value = null;
        try {
            value = MessageDigests.toHexCharArray(bytes);
            final PBEKeySpec spec = new PBEKeySpec(value, salt, iterations, keyLength);
            final SecretKey key = PBKDF2KeyFactory.generateSecret(spec);
            return key.getEncoded();
        } catch (InvalidKeySpecException e) {
            throw new RuntimeException("Unexpected exception when computing PBKDF2 hash", e);
        } finally {
            if (value != null) {
                Arrays.fill(value, '0');
            }
        }
    }

}
