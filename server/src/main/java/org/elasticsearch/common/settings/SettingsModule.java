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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * A module that binds the provided settings to the {@link Settings} interface.
 */
public class SettingsModule implements Module {
    private static final Logger logger = LogManager.getLogger(SettingsModule.class);

    private final Settings settings;
    private final Set<String> settingsFilterPattern = new HashSet<>();
    private final Map<String, Setting<?>> nodeSettings = new HashMap<>();
    private final Map<String, Setting<?>> indexSettings = new HashMap<>();
    private final Map<String, char[]> localHashesOfConsistentSettings = new HashMap<>();
    private final Map<String, String> publicHashesOfConsistentSettings = new HashMap<>();
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;

    public SettingsModule(Settings settings, Setting<?>... additionalSettings) {
        this(settings, Arrays.asList(additionalSettings), Collections.emptyList(), Collections.emptySet());
    }

    public SettingsModule(
            Settings settings,
            List<Setting<?>> additionalSettings,
            List<String> settingsFilter,
            Set<SettingUpgrader<?>> settingUpgraders) {
        this.settings = settings;
        for (Setting<?> setting : ClusterSettings.BUILT_IN_CLUSTER_SETTINGS) {
            registerSetting(setting);
        }
        for (Setting<?> setting : IndexScopedSettings.BUILT_IN_INDEX_SETTINGS) {
            registerSetting(setting);
        }

        for (Setting<?> setting : additionalSettings) {
            registerSetting(setting);
        }
        for (String filter : settingsFilter) {
            registerSettingsFilter(filter);
        }
        final Set<SettingUpgrader<?>> clusterSettingUpgraders = new HashSet<>();
        for (final SettingUpgrader<?> settingUpgrader : ClusterSettings.BUILT_IN_SETTING_UPGRADERS) {
            assert settingUpgrader.getSetting().hasNodeScope() : settingUpgrader.getSetting().getKey();
            final boolean added = clusterSettingUpgraders.add(settingUpgrader);
            assert added : settingUpgrader.getSetting().getKey();
        }
        for (final SettingUpgrader<?> settingUpgrader : settingUpgraders) {
            assert settingUpgrader.getSetting().hasNodeScope() : settingUpgrader.getSetting().getKey();
            final boolean added = clusterSettingUpgraders.add(settingUpgrader);
            assert added : settingUpgrader.getSetting().getKey();
        }
        this.indexScopedSettings = new IndexScopedSettings(settings, new HashSet<>(this.indexSettings.values()));
        this.clusterSettings = new ClusterSettings(settings, new HashSet<>(this.nodeSettings.values()), clusterSettingUpgraders);
        Settings indexSettings = settings.filter((s) -> (s.startsWith("index.") &&
            // special case - we want to get Did you mean indices.query.bool.max_clause_count
            // which means we need to by-pass this check for this setting
            // TODO remove in 6.0!!
            "index.query.bool.max_clause_count".equals(s) == false)
            && clusterSettings.get(s) == null);
        if (indexSettings.isEmpty() == false) {
            try {
                String separator = IntStream.range(0, 85).mapToObj(s -> "*").collect(Collectors.joining("")).trim();
                StringBuilder builder = new StringBuilder();
                builder.append(System.lineSeparator());
                builder.append(separator);
                builder.append(System.lineSeparator());
                builder.append("Found index level settings on node level configuration.");
                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                int count = 0;
                for (String word : ("Since elasticsearch 5.x index level settings can NOT be set on the nodes configuration like " +
                    "the elasticsearch.yaml, in system properties or command line arguments." +
                    "In order to upgrade all indices the settings must be updated via the /${index}/_settings API. " +
                    "Unless all settings are dynamic all indices must be closed in order to apply the upgrade" +
                    "Indices created in the future should use index templates to set default values."
                ).split(" ")) {
                    if (count + word.length() > 85) {
                        builder.append(System.lineSeparator());
                        count = 0;
                    }
                    count += word.length() + 1;
                    builder.append(word).append(" ");
                }

                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                builder.append("Please ensure all required values are updated on all indices by executing: ");
                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                builder.append("curl -XPUT 'http://localhost:9200/_all/_settings?preserve_existing=true' -d '");
                try (XContentBuilder xContentBuilder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    xContentBuilder.prettyPrint();
                    xContentBuilder.startObject();
                    indexSettings.toXContent(xContentBuilder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
                    xContentBuilder.endObject();
                    builder.append(Strings.toString(xContentBuilder));
                }
                builder.append("'");
                builder.append(System.lineSeparator());
                builder.append(separator);
                builder.append(System.lineSeparator());

                logger.warn(builder.toString());
                throw new IllegalArgumentException("node settings must not contain any index level settings");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // by now we are fully configured, lets check node level settings for unregistered index settings
        clusterSettings.validate(settings, true);
        this.settingsFilter = new SettingsFilter(settingsFilterPattern);
     }

    @Override
    public void configure(Binder binder) {
        binder.bind(Settings.class).toInstance(settings);
        binder.bind(SettingsFilter.class).toInstance(settingsFilter);
        binder.bind(ClusterSettings.class).toInstance(clusterSettings);
        binder.bind(IndexScopedSettings.class).toInstance(indexScopedSettings);
    }

    /**
     * Registers a new setting. This method should be used by plugins in order to expose any custom settings the plugin defines.
     * Unless a setting is registered the setting is unusable. If a setting is never the less specified the node will reject
     * the setting during startup.
     */
    private void registerSetting(Setting<?> setting) {
        if (setting.isFiltered()) {
            if (settingsFilterPattern.contains(setting.getKey()) == false) {
                registerSettingsFilter(setting.getKey());
            }
        }
        if (setting.hasNodeScope() || setting.hasIndexScope()) {
            if (setting.hasNodeScope()) {
                Setting<?> existingSetting = nodeSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                nodeSettings.put(setting.getKey(), setting);
                if (setting.isConsistent()) {
                    if (setting instanceof Setting.AffixSetting<?>) {
                        ((Setting.AffixSetting<?>)setting).getAllConcreteSettings(this.settings).forEach(concreteSetting -> {
                            if (concreteSetting instanceof SecureSetting<?>) {
                                buildSecureSettingHashes((SecureSetting<?>) concreteSetting);
                            } else {
                                throw new RuntimeException("Unrecognized consistent setting [" + setting.getKey() + "]");
                            }
                        });
                    } else if (setting instanceof SecureSetting<?>) {
                        buildSecureSettingHashes((SecureSetting<?>) setting);
                    } else {
                        throw new RuntimeException("Unrecognized consistent setting [" + setting.getKey() + "]");
                    }
                }
            } else if (setting.hasIndexScope()) {
                Setting<?> existingSetting = indexSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                indexSettings.put(setting.getKey(), setting);
            }
        } else {
            throw new IllegalArgumentException("No scope found for setting [" + setting.getKey() + "]");
        }
    }

    private void buildSecureSettingHashes(SecureSetting<?> secureConcreteSetting) {
        // compute unsalted hash to store in memory on the local node
        final char[] localHash = computeSecureSettingLocalHash(secureConcreteSetting);
        localHashesOfConsistentSettings.put(secureConcreteSetting.getKey(), localHash);
        // compute and store a salted hash to store on the cluster state, so that other nodes can cross check against it
        publicHashesOfConsistentSettings.put(secureConcreteSetting.getKey(), computePublicHash(localHash));
    }

    /**
     * Compute UNSALTED hash of a secure setting value for subsequent comparison with the master's reference. This values IS NOT to be
     * published in the cluster state, but rather compared (and stored), on the local node, to the SALTED hash value published in the
     * cluster state by the master node.
     */
    private char[] computeSecureSettingLocalHash(SecureSetting<?> secureSetting) {
        final Object verbatimValue = secureSetting.get(this.settings);
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

    public static byte[] computeSaltedHash(char[] value, byte[] salt) {
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

    public static String computePublicHash(char[] value) {
        final String saltString = UUIDs.randomBase64UUID();
        final byte[] saltedHashBytes = computeSaltedHash(value, saltString.getBytes(StandardCharsets.UTF_8));
        final String base64SaltedHashString = new String(saltedHashBytes, StandardCharsets.UTF_8);
        return saltString + ":" + base64SaltedHashString;
    }

    /**
     * Registers a settings filter pattern that allows to filter out certain settings that for instance contain sensitive information
     * or if a setting is for internal purposes only. The given pattern must either be a valid settings key or a simple regexp pattern.
     */
    private void registerSettingsFilter(String filter) {
        if (SettingsFilter.isValidPattern(filter) == false) {
            throw new IllegalArgumentException("filter [" + filter +"] is invalid must be either a key or a regex pattern");
        }
        if (settingsFilterPattern.contains(filter)) {
            throw new IllegalArgumentException("filter [" + filter + "] has already been registered");
        }
        settingsFilterPattern.add(filter);
    }

    public Settings getSettings() {
        return settings;
    }

    public IndexScopedSettings getIndexScopedSettings() {
        return indexScopedSettings;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public SettingsFilter getSettingsFilter() {
        return settingsFilter;
    }

}
