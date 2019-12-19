/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class EncryptedRepositoryPlugin extends Plugin implements RepositoryPlugin {

    static final Setting.AffixSetting<SecureString> ENCRYPTION_PASSWORD_SETTING = Setting.affixKeySetting("repository.encrypted.",
            "password", key -> SecureSetting.secureString(key, null));
    static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity());
    static final String TYPE = "encrypted";

    private final Map<String, char[]> cachedRepositoryPasswords = new HashMap<>();

    public EncryptedRepositoryPlugin(Settings settings) {
        // cache the passwords for all encrypted repositories during *plugin* instantiation
        // the keystore-based secure passwords are not readable on repository instantiation
        for (String repositoryName : ENCRYPTION_PASSWORD_SETTING.getNamespaces(settings)) {
            Setting<SecureString> encryptionPasswordSetting = ENCRYPTION_PASSWORD_SETTING
                    .getConcreteSettingForNamespace(repositoryName);
            SecureString encryptionPassword = encryptionPasswordSetting.get(settings);
            cachedRepositoryPasswords.put(repositoryName, encryptionPassword.getChars());
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ENCRYPTION_PASSWORD_SETTING);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(final Environment env, final NamedXContentRegistry registry,
                                                           final ClusterService clusterService) {
        return Collections.singletonMap(TYPE, new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetaData metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetaData metaData, Function<String, Repository.Factory> typeLookup) throws Exception {
                String delegateType = DELEGATE_TYPE.get(metaData.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException(DELEGATE_TYPE.getKey() + " must be set");
                }
                if (TYPE.equals(delegateType)) {
                    throw new IllegalArgumentException("Cannot encrypt an already encrypted repository. " + DELEGATE_TYPE.getKey() +
                            " must not be equal to " + TYPE);
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
                char[] masterPassword = cachedRepositoryPasswords.get(metaData.name());
                return new EncryptedRepository(metaData, registry, clusterService, (BlobStoreRepository) delegatedRepository,
                        masterPassword);
            }
        });
    }
}
