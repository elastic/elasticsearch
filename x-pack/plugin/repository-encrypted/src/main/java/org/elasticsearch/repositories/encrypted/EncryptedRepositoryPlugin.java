/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ConsistentSettingsService;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public final class EncryptedRepositoryPlugin extends Plugin implements RepositoryPlugin {

    static final Logger logger = LogManager.getLogger(EncryptedRepositoryPlugin.class);
    static final String REPOSITORY_TYPE_NAME = "encrypted";
    static final String CIPHER_ALGO = "AES";
    static final String RAND_ALGO = "SHA1PRNG";
    static final Setting.AffixSetting<SecureString> ENCRYPTION_PASSWORD_SETTING = Setting.affixKeySetting("repository.encrypted.",
            "password", key -> SecureSetting.secureString(key, null, Setting.Property.Consistent));
    static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity());

    protected static XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    public EncryptedRepositoryPlugin(Settings settings) {
        if (false == EncryptedRepositoryPlugin.getLicenseState().isEncryptedSnapshotAllowed()) {
            logger.warn("Encrypted snapshot repositories are not allowed for the current license." +
                    "Snapshotting to any encrypted repository is not permitted and will fail.",
                    LicenseUtils.newComplianceException(EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME + " snapshot repository"));
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ENCRYPTION_PASSWORD_SETTING);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(final Environment env, final NamedXContentRegistry registry,
                                                           final ClusterService clusterService) {
        // cache all the passwords for encrypted repositories while keystore-based secure passwords are still readable
        final Map<String, char[]> cachedRepositoryPasswords = new HashMap<>();
        for (String repositoryName : ENCRYPTION_PASSWORD_SETTING.getNamespaces(env.settings())) {
            Setting<SecureString> encryptionPasswordSetting = ENCRYPTION_PASSWORD_SETTING
                    .getConcreteSettingForNamespace(repositoryName);
            SecureString encryptionPassword = encryptionPasswordSetting.get(env.settings());
            cachedRepositoryPasswords.put(repositoryName, encryptionPassword.getChars());
        }
        return Collections.singletonMap(REPOSITORY_TYPE_NAME, new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetaData metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetaData metaData, Function<String, Repository.Factory> typeLookup) throws Exception {
                if (false == EncryptedRepositoryPlugin.getLicenseState().isEncryptedSnapshotAllowed()) {
                    logger.warn("Encrypted snapshot repositories are not allowed for the current license." +
                                    "Snapshots to the [" + metaData.name() + "] encrypted repository are not permitted and will fail.",
                            LicenseUtils.newComplianceException(EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME + " snapshot repository"));
                }
                String delegateType = DELEGATE_TYPE.get(metaData.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException(DELEGATE_TYPE.getKey() + " must be set");
                }
                if (REPOSITORY_TYPE_NAME.equals(delegateType)) {
                    throw new IllegalArgumentException("Cannot encrypt an already encrypted repository. " + DELEGATE_TYPE.getKey() +
                            " must not be equal to " + REPOSITORY_TYPE_NAME);
                }
                if (false == cachedRepositoryPasswords.containsKey(metaData.name())) {
                    throw new IllegalArgumentException(
                            ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(metaData.name()).getKey() + " must be set");
                }
                Repository.Factory factory = typeLookup.apply(delegateType);
                Repository delegatedRepository = factory.create(new RepositoryMetaData(metaData.name(),
                        delegateType, metaData.settings()));
                if (false == (delegatedRepository instanceof BlobStoreRepository) || delegatedRepository instanceof EncryptedRepository) {
                    throw new IllegalArgumentException("Unsupported type " + DELEGATE_TYPE.getKey());
                }
                char[] repositoryPassword = cachedRepositoryPasswords.get(metaData.name());
                PasswordBasedEncryption metadataEncryption = new PasswordBasedEncryption(repositoryPassword,
                        SecureRandom.getInstance(RAND_ALGO));
                return new EncryptedRepository(metaData, registry, clusterService, (BlobStoreRepository) delegatedRepository,
                        repositoryPassword);
            }
        });
    }
}
