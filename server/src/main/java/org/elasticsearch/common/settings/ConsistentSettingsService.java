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

/**
 * Used to publish secure setting hashes in the cluster state and to validate those hashes against the local values of those same settings.
 * This is colloquially referred to as the secure setting consistency check. It will publish and verify hashes only for the collection
 * of settings passed in the constructor. The settings have to have the {@link Setting.Property#Consistent} property.
 */
public final class ConsistentSettingsService {
    private static final Logger logger = LogManager.getLogger(ConsistentSettingsService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final Collection<Setting<?>> secureSettingsCollection;
    private final Map<String, byte[]> digestsBySettingKey;
    private final SecretKeyFactory pbkdf2KeyFactory;

    public ConsistentSettingsService(Settings settings, ClusterService clusterService,
                                     Collection<Setting<?>> secureSettingsCollection) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.secureSettingsCollection = secureSettingsCollection;
        // eagerly compute digests because the keystore could be closed at a later time
        this.digestsBySettingKey = computeDigestOfConsistentSecureSettings();
        // this is used to compute the PBKDF2 hash (the published one)
        try {
            this.pbkdf2KeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("The \"PBKDF2WithHmacSHA512\" algorithm is required for consistent secure settings' hashes", e);
        }
    }

    /**
     * Returns a {@link LocalNodeMasterListener} that will publish hashes of all the settings passed in the constructor. These hashes are
     * published by the master node only. Note that this is not designed for {@link SecureSettings} implementations that are mutable.
     */
    public LocalNodeMasterListener newHashPublisher() {
        // eagerly compute salted hashes to be published
        final Map<String, String> hashesBySettingKey = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : this.digestsBySettingKey.entrySet()) {
            String salt = UUIDs.randomBase64UUID();
            byte[] publicHash = computeSaltedPBKDF2Hash(entry.getValue(), salt.getBytes(StandardCharsets.UTF_8));
            String encodedPublicHash = new String(Base64.getEncoder().encode(publicHash), StandardCharsets.UTF_8);
            hashesBySettingKey.put(entry.getKey(), salt + ":" + encodedPublicHash);
        }
        return new HashesPublisher(hashesBySettingKey, clusterService);
    }

    /**
     * Verifies that the hashes of consistent secure settings in the latest {@code ClusterState} verify for the values of those same
     * settings on the local node. The settings to be checked are passed in the constructor. Also, validates that a missing local
     * value is also missing in the published set, and vice-versa.
     */
    public boolean areAllConsistent() {
        ClusterState state = clusterService.state();
        Map<String, String> publishedHashesOfConsistentSettings = getPublishedHashesOfConsistentSettings();
        AtomicBoolean allConsistent = new AtomicBoolean(true);
        for (String localSettingName : digestsBySettingKey.keySet()) {
            if (false == publishedHashesOfConsistentSettings.containsKey(localSettingName)) {
                // setting missing on master but present locally
                logger.warn("no published hash for the consistent secure setting [{}] but it exists on the local node", localSettingName);
                if (state.nodes().isLocalNodeElectedMaster()) {
                    throw new IllegalStateException("Master node cannot validate consistent setting. No published hash for ["
                            + localSettingName + "] but setting exists.");
                }
                allConsistent.set(false);
            }
        }
        for (String publishedSettingName : publishedHashesOfConsistentSettings.keySet()) {
            boolean publishedMatches = false;
            for (Setting<?> secureSetting : secureSettingsCollection) {
                if (secureSetting.match(publishedSettingName)) {
                    publishedMatches = true;
                    break;
                }
            }
            if (publishedMatches && false == digestsBySettingKey.containsKey(publishedSettingName)) {
                logger.warn("the consistent secure setting [{}] does not exist on the local node but there is a published hash for it",
                        publishedSettingName);
                allConsistent.set(false);
            }
        }
        for (Map.Entry<String, String> publishedSaltAndHashForSetting : publishedHashesOfConsistentSettings.entrySet()) {
            String settingName = publishedSaltAndHashForSetting.getKey();
            String publishedSaltAndHash = publishedSaltAndHashForSetting.getValue();
            if (digestsBySettingKey.containsKey(settingName)) {
                String[] parts = publishedSaltAndHash.split(":");
                if (parts == null || parts.length != 2) {
                    throw new IllegalArgumentException("published hash [" + publishedSaltAndHash + " ] for secure setting ["
                            + settingName + "] is invalid");
                }
                String publishedSalt = parts[0];
                String publishedHash = parts[1];
                byte[] localDigest = digestsBySettingKey.get(settingName);
                byte[] computedSaltedHashBytes = computeSaltedPBKDF2Hash(localDigest, publishedSalt.getBytes(StandardCharsets.UTF_8));
                final String computedSaltedHash = new String(Base64.getEncoder().encode(computedSaltedHashBytes), StandardCharsets.UTF_8);
                if (false == publishedHash.equals(computedSaltedHash)) {
                    logger.warn("the published hash [{}] of the consistent secure setting [{}] differs from the locally computed one [{}]",
                            publishedHash, settingName, computedSaltedHash);
                    if (state.nodes().isLocalNodeElectedMaster()) {
                        throw new IllegalStateException("Master node cannot validate consistent setting. The published hash ["
                                + publishedHash + "] of the consistent secure setting [" + settingName
                                + "] differs from the locally computed one [" + computedSaltedHash + "].");
                    }
                    allConsistent.set(false);
                }
            }
        }
        return allConsistent.get();
    }

    public boolean isConsistent(SecureSetting<?> secureSetting) {
        for (String localSettingName : digestsBySettingKey.keySet()) {
            if (secureSetting.match(localSettingName)) {
                Map<String, String> publishedHashesOfConsistentSettings = getPublishedHashesOfConsistentSettings();
                if (false == publishedHashesOfConsistentSettings.containsKey(localSettingName)) {
                    logger.warn("no published hash for the consistent secure setting [{}] but it exists on the local node",
                            localSettingName);
                    return false;
                }
                String publishedSaltAndHash = publishedHashesOfConsistentSettings.get(localSettingName);
                String[] parts = publishedSaltAndHash.split(":");
                if (parts == null || parts.length != 2) {
                    throw new IllegalArgumentException("published hash [" + publishedSaltAndHash + " ] for secure setting ["
                            + localSettingName + "] is invalid");
                }
                String publishedSalt = parts[0];
                String publishedHash = parts[1];
                byte[] localDigest = digestsBySettingKey.get(localSettingName);
                byte[] computedSaltedHashBytes = computeSaltedPBKDF2Hash(localDigest, publishedSalt.getBytes(StandardCharsets.UTF_8));
                final String computedSaltedHash = new String(Base64.getEncoder().encode(computedSaltedHashBytes), StandardCharsets.UTF_8);
                if (false == publishedHash.equals(computedSaltedHash)) {
                    logger.warn("the published hash [{}] of the consistent secure setting [{}] differs from the locally computed one [{}]",
                            publishedHash, localSettingName, computedSaltedHash);
                    return false;
                }
                return true;
            }
        }
        throw new IllegalArgumentException("Invalid setting [" + secureSetting.getKey() + "] for consistency check.");
    }

    private Map<String, String> getPublishedHashesOfConsistentSettings() {
        ClusterState state = clusterService.state();
        if (state.metaData() == null || state.metaData().hashesOfConsistentSettings() == null) {
            throw new IllegalStateException("Hashes of consistent secure settings are not yet published by the master node. Cannot " +
                    "check consistency at this time");
        }
        return state.metaData().hashesOfConsistentSettings();
    }

    /**
     * Iterate over the passed in secure settings, expanding {@link Setting.AffixSetting} to concrete settings, in the scope of the local
     * settings.
     */
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

    private Map<String, byte[]> computeDigestOfConsistentSecureSettings() {
        Map<String, byte[]> digestsBySettingKey = new HashMap<>();
        forEachConcreteSecureSettingDo(concreteSecureSetting -> {
            byte[] localDigest = concreteSecureSetting.getSecretDigest(settings);
            if  (localDigest != null) {
                digestsBySettingKey.put(concreteSecureSetting.getKey(), localDigest);
            }
        });
        return digestsBySettingKey;
    }

    private byte[] computeSaltedPBKDF2Hash(byte[] bytes, byte[] salt) {
        final int iterations = 5000;
        final int keyLength = 512;
        char[] value = null;
        try {
            value = MessageDigests.toHexCharArray(bytes);
            final PBEKeySpec spec = new PBEKeySpec(value, salt, iterations, keyLength);
            final SecretKey key = pbkdf2KeyFactory.generateSecret(spec);
            return key.getEncoded();
        } catch (InvalidKeySpecException e) {
            throw new RuntimeException("Unexpected exception when computing PBKDF2 hash", e);
        } finally {
            if (value != null) {
                Arrays.fill(value, '0');
            }
        }
    }

    static final class HashesPublisher implements LocalNodeMasterListener {

        final Map<String, String> computedHashesOfConsistentSettings;
        final ClusterService clusterService;

        HashesPublisher(Map<String, String> computedHashesOfConsistentSettings, ClusterService clusterService) {
            this.computedHashesOfConsistentSettings = computedHashesOfConsistentSettings;
            this.clusterService = clusterService;
        }

        @Override
        public void onMaster() {
            clusterService.submitStateUpdateTask("publish-secure-settings-hashes", new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final Map<String, String> publishedHashesOfConsistentSettings = currentState.metaData()
                            .hashesOfConsistentSettings();
                    if (computedHashesOfConsistentSettings.equals(publishedHashesOfConsistentSettings)) {
                        logger.debug("Nothing to publish. What is already published matches this node's view.");
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
    }

}
