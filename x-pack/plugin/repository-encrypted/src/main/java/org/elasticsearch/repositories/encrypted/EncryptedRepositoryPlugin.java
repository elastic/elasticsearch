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
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class EncryptedRepositoryPlugin extends Plugin implements RepositoryPlugin {

    static final String REPOSITORY_TYPE_NAME = "encrypted";
    static final String CIPHER_ALGO = "AES";
    static final int KEK_PBKDF2_ITER = 61616; // funny "uncommon" iter count larger than 60k
    static final String KEK_PBKDF2_ALGO = "PBKDF2WithHmacSHA512";
    static final int KEK_KEY_SIZE_IN_BITS = 256;
    static final String DEFAULT_KEK_SALT = "the AES key encryption key, which is generated from the repository password using " +
            "PBKDF2, is never stored on disk, therefore the salt parameter of PBKDF2, used to protect against offline cracking of the key" +
            " using rainbow tables is not required. A hardcoded salt value does not compromise security.";
    static final Setting.AffixSetting<SecureString> ENCRYPTION_PASSWORD_SETTING = Setting.affixKeySetting("repository.encrypted.",
            "password", key -> SecureSetting.secureString(key, null));
    static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity());
    static final Setting<String> KEK_PBKDF2_SALT = new Setting<>("kek_salt", DEFAULT_KEK_SALT, Function.identity());

    private final Map<String, char[]> cachedRepositoryPasswords = new HashMap<>();

    public EncryptedRepositoryPlugin(Settings settings) {
        // cache the passwords for all encrypted repositories during plugin instantiation
        // the keystore-based secure passwords are not readable on repository instantiation
        for (String repositoryName : ENCRYPTION_PASSWORD_SETTING.getNamespaces(settings)) {
            Setting<SecureString> encryptionPasswordSetting = ENCRYPTION_PASSWORD_SETTING
                    .getConcreteSettingForNamespace(repositoryName);
            SecureString encryptionPassword = encryptionPasswordSetting.get(settings);
            cachedRepositoryPasswords.put(repositoryName, encryptionPassword.getChars());
        }
    }

    SecretKey generateKeyEncryptionKey(char[] password, byte[] salt) throws NoSuchAlgorithmException, InvalidKeySpecException {
        PBEKeySpec keySpec = new PBEKeySpec(password, salt, KEK_PBKDF2_ITER, KEK_KEY_SIZE_IN_BITS);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEK_PBKDF2_ALGO);
        SecretKey secretKey = keyFactory.generateSecret(keySpec);
        SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), CIPHER_ALGO);
        return secret;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ENCRYPTION_PASSWORD_SETTING);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(final Environment env, final NamedXContentRegistry registry,
                                                           final ClusterService clusterService) {
        return Collections.singletonMap(REPOSITORY_TYPE_NAME, new Repository.Factory() {

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
                String kekSalt = KEK_PBKDF2_SALT.get(metaData.settings());
                SecretKey keyEncryptionKey = generateKeyEncryptionKey(repositoryPassword, kekSalt.getBytes(StandardCharsets.UTF_8));
                return new EncryptedRepository(metaData, registry, clusterService, (BlobStoreRepository) delegatedRepository,
                        keyEncryptionKey);
            }
        });
    }
}
