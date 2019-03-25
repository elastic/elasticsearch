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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class ConsistentSecureSettingsValidator implements ClusterStateApplier, LocalNodeMasterListener {
    private static final Logger logger = LogManager.getLogger(ConsistentSecureSettingsValidator.class);

    private final ClusterService clusterService;
    private final Map<String, char[]> localHashesOfConsistentSettings = new HashMap<>();
    private volatile Map<String, String> publicHashesOfConsistentSettings = new HashMap<>();
    private volatile boolean allSecureSettingsConsistent = true;

    public ConsistentSecureSettingsValidator(Settings settings, ClusterService clusterService, Set<SecureSetting<?>> consistentSecureSettings) {
        this.clusterService = clusterService;
        // eagerly compute hashes for the secure settings
        computeHashesOfLocalSecureSettingValues(settings, consistentSecureSettings);
    }

    private void computeHashesOfLocalSecureSettingValues(Settings settings, Set<SecureSetting<?>> consistentSecureSettings) {
        for (SecureSetting<?> secureSetting : consistentSecureSettings) {
            // compute unsalted hash to store in memory on the local node
            final char[] localHash = computeSecureSettingLocalHash(settings, secureSetting);
            localHashesOfConsistentSettings.put(secureSetting.getKey(), localHash);
            // salted hash (of the unsalted hash) suitable for cluster-wide publication
            final String publicHash = computePublicHash(localHash, UUIDs.randomBase64UUID());
            publicHashesOfConsistentSettings.put(secureSetting.getKey(), publicHash);
        }
    }

    public boolean allSecureSettingsConsistent() {
        return allSecureSettingsConsistent;
    }

    Map<String, String> publicHashesOfConsistentSettings() {
        return publicHashesOfConsistentSettings;
    }

    /**
     * Compute UNSALTED hash of a secure setting value for subsequent comparison with the master's reference. This values IS NOT to be
     * published in the cluster state, but rather compared (and stored) locally, to the SALTED hash value published in the cluster state by
     * the master node.
     */
    private static char[] computeSecureSettingLocalHash(Settings settings, SecureSetting<?> secureSetting) {
        final Object verbatimValue = secureSetting.get(settings);
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("\"SHA-512\" algorithm is required for consistent secure settings", e);
        }
        if (verbatimValue instanceof InputStream) {
            try (InputStream is = ((InputStream)verbatimValue)) {
                try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    Streams.copy(is, out);
                    digest.update(out.toByteArray());
                }
            } catch (IOException e) {
                throw new RuntimeException("Exception while reading the [" + secureSetting.getKey() + "] secure setting.", e);
            }
        } else if (verbatimValue instanceof SecureString) {
            try (SecureString secureString = ((SecureString)verbatimValue)) {
                digest.update(StandardCharsets.UTF_8.encode(CharBuffer.wrap(secureString.getChars())));
            }
        } else {
            throw new IllegalArgumentException("Unrecognized consistent secure setting [" + secureSetting.getKey() + "]");
        }
        // computing a base64 encoded unsalted SHA512 hash
        byte[] unencodedSHA512AsByteArray = null;
        byte[] SHA512EncodedBase64AsByteArray = null;
        try {
            unencodedSHA512AsByteArray = digest.digest();
            SHA512EncodedBase64AsByteArray = Base64.getEncoder().encode(unencodedSHA512AsByteArray);
            return CharArrays.utf8BytesToChars(SHA512EncodedBase64AsByteArray);
        } finally {
            if (unencodedSHA512AsByteArray != null) {
                Arrays.fill(unencodedSHA512AsByteArray, (byte) 0);
            }
            if (SHA512EncodedBase64AsByteArray != null) {
                Arrays.fill(SHA512EncodedBase64AsByteArray, (byte) 0);
            }
        }
    }

    private static byte[] computeSaltedHash(char[] value, byte[] salt) {
        final int iterations = 5000;
        final int keyLength = 512;
        try {
            final SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
            final PBEKeySpec spec = new PBEKeySpec(value, salt, iterations, keyLength);
            final SecretKey key = skf.generateSecret(spec);
            return key.getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("\"PBKDF2WithHmacSHA512\" algorithm is required for consistent secure settings", e);
        }
    }

    private static String computePublicHash(char[] localHash, String salt) {
        final byte[] saltedHashBytes = computeSaltedHash(localHash, salt.getBytes(StandardCharsets.UTF_8));
        final String base64SaltedHashString = new String(saltedHashBytes, StandardCharsets.UTF_8);
        return salt + ":" + base64SaltedHashString;
    }

    private boolean checkPublicHashAgainstLocalValue(String publishedSettingName, String publishedHashWithSalt) {
        logger.trace("published hash for secure setting [{}] is [{}]", publishedSettingName, publishedHashWithSalt);
        if (publishedHashWithSalt == null || false == publishedHashWithSalt.contains(":")) {
            logger.trace("published hash [{}] for secure setting [{}] is invalid", publishedHashWithSalt, publishedSettingName);
            return false;
        }
        final String[] parts = publishedHashWithSalt.split(":");
        if (parts == null || parts.length != 2) {
            logger.trace("published hash [{}] for secure setting [{}] is invalid", publishedHashWithSalt, publishedSettingName);
            return false;
        }
        final char[] localHash = localHashesOfConsistentSettings.get(publishedSettingName);
        if (localHash == null) {
            logger.trace("secure setting [{}] is missing on the local node", publishedSettingName);
            return false;
        }
        final String publishedSalt = parts[0];
        final String localHashWithSalt = computePublicHash(localHash, publishedSalt);
        logger.trace("local hash for secure setting [{}] is [{}]", publishedSettingName, localHashWithSalt);
        return publishedHashWithSalt.equals(localHashWithSalt);
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        boolean allConsistent = true;
        final Map<String, String> publishedHashesOfConsistentSettings = event.state().metaData().hashesOfConsistentSettings();
        if (event.localNodeMaster()) {
            logger.debug("local node is elected master, no hashes consistency check is required");
        } else if (publishedHashesOfConsistentSettings.equals(publicHashesOfConsistentSettings)) {
            logger.debug("hashes of secure settings are identical to the master's");
        } else {
            for (Map.Entry<String, String> publishedSettingAndHash : publishedHashesOfConsistentSettings.entrySet()) {
                if (false == checkPublicHashAgainstLocalValue(publishedSettingAndHash.getKey(), publishedSettingAndHash.getValue())) {
                    allConsistent = false;
                    if (logger.isDebugEnabled()) {
                        logger.debug("secure setting [{}] differs on the local node [{}] compared to master [{}].",
                                publishedSettingAndHash.getKey(), event.state().nodes().getLocalNodeId(),
                                event.state().nodes().getMasterNodeId());
                    } else {
                        break;
                    }
                }
            }
            // also verify that local node does not hold a superset of the master's
            for (String localSecureSettingName : localHashesOfConsistentSettings.keySet()) {
                if (false == publishedHashesOfConsistentSettings.containsKey(localSecureSettingName)) {
                    allConsistent = false;
                    if (logger.isTraceEnabled()) {
                        logger.trace("secure setting [{}] is missing on master, but it is present on the local node",
                                localSecureSettingName);
                    } else {
                        break;
                    }
                }
            }
            // public hashes of master and local node are equivalent but salts differ. Copy master's to local to speedup future checks
            if (allConsistent) {
                publicHashesOfConsistentSettings = publishedHashesOfConsistentSettings;
            }
        }
        allSecureSettingsConsistent = allConsistent;
    }

    @Override
    public void onMaster() {
        clusterService.submitStateUpdateTask("publish-secure-settings-hashes", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final Map<String, String> publishedHashesOfConsistentSettings = currentState.metaData().hashesOfConsistentSettings();
                final Map<String, String> publicHashesOfConsistentSettings = publicHashesOfConsistentSettings();
                if (publicHashesOfConsistentSettings.equals(publishedHashesOfConsistentSettings)) {
                    logger.debug("Nothing to publish, what is already published matches this master's view as well");
                    return currentState;
                } else {
                    return ClusterState.builder(currentState)
                            .metaData(MetaData.builder(currentState.metaData())
                                    .hashesOfConsistentSettings(publicHashesOfConsistentSettings))
                            .build();
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("unable to install publish secure settings hashes", e);
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
