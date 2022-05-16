/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Build;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class EncryptedRepositoryPlugin extends Plugin implements RepositoryPlugin {

    static final LicensedFeature.Momentary ENCRYPTED_SNAPSHOT_FEATURE = LicensedFeature.momentary(
        null,
        "encrypted-snapshot",
        License.OperationMode.PLATINUM
    );

    private static final Boolean ENCRYPTED_REPOSITORY_FEATURE_FLAG_REGISTERED;
    static {
        final String property = System.getProperty("es.encrypted_repository_feature_flag_registered");
        if (Build.CURRENT.isSnapshot() && property != null) {
            throw new IllegalArgumentException("es.encrypted_repository_feature_flag_registered is only supported in non-snapshot builds");
        }
        if ("true".equals(property)) {
            ENCRYPTED_REPOSITORY_FEATURE_FLAG_REGISTERED = true;
        } else if ("false".equals(property)) {
            ENCRYPTED_REPOSITORY_FEATURE_FLAG_REGISTERED = false;
        } else if (property == null) {
            ENCRYPTED_REPOSITORY_FEATURE_FLAG_REGISTERED = null;
        } else {
            throw new IllegalArgumentException(
                "expected es.encrypted_repository_feature_flag_registered to be unset or [true|false] but was [" + property + "]"
            );
        }
    }

    static final Logger logger = LogManager.getLogger(EncryptedRepositoryPlugin.class);
    static final String REPOSITORY_TYPE_NAME = "encrypted";
    // TODO add at least hdfs, and investigate supporting all `BlobStoreRepository` implementations
    static final List<String> SUPPORTED_ENCRYPTED_TYPE_NAMES = Arrays.asList("fs", "gcs", "azure", "s3");
    static final Setting.AffixSetting<SecureString> ENCRYPTION_PASSWORD_SETTING = Setting.affixKeySetting(
        "repository.encrypted.",
        "password",
        key -> SecureSetting.secureString(key, null)
    );
    static final Setting<String> DELEGATE_TYPE_SETTING = Setting.simpleString("delegate_type", "");
    static final Setting<String> PASSWORD_NAME_SETTING = Setting.simpleString("password_name", "");

    // "protected" because it is overloaded for tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ENCRYPTION_PASSWORD_SETTING);
    }

    // public for testing
    // Checks if the plugin is currently disabled because we're running a release build or the feature flag is turned off
    public static boolean isDisabled() {
        return false == Build.CURRENT.isSnapshot()
            && (ENCRYPTED_REPOSITORY_FEATURE_FLAG_REGISTERED == null || ENCRYPTED_REPOSITORY_FEATURE_FLAG_REGISTERED == false);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry registry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings
    ) {
        if (isDisabled()) {
            return Map.of();
        }

        // load all the passwords from the keystore in memory because the keystore is not readable when the repository is created
        final Map<String, SecureString> repositoryPasswordsMapBuilder = new HashMap<>();
        for (String passwordName : ENCRYPTION_PASSWORD_SETTING.getNamespaces(env.settings())) {
            Setting<SecureString> passwordSetting = ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(passwordName);
            repositoryPasswordsMapBuilder.put(passwordName, passwordSetting.get(env.settings()));
            logger.debug("Loaded repository password [{}] from the node keystore", passwordName);
        }
        final Map<String, SecureString> repositoryPasswordsMap = Map.copyOf(repositoryPasswordsMapBuilder);
        return Collections.singletonMap(REPOSITORY_TYPE_NAME, new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetadata metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetadata metadata, Function<String, Repository.Factory> typeLookup) throws Exception {
                final String delegateType = DELEGATE_TYPE_SETTING.get(metadata.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException("Repository setting [" + DELEGATE_TYPE_SETTING.getKey() + "] must be set");
                }
                if (REPOSITORY_TYPE_NAME.equals(delegateType)) {
                    throw new IllegalArgumentException(
                        "Cannot encrypt an already encrypted repository. ["
                            + DELEGATE_TYPE_SETTING.getKey()
                            + "] must not be equal to ["
                            + REPOSITORY_TYPE_NAME
                            + "]"
                    );
                }
                final Repository.Factory factory = typeLookup.apply(delegateType);
                if (null == factory || false == SUPPORTED_ENCRYPTED_TYPE_NAMES.contains(delegateType)) {
                    throw new IllegalArgumentException(
                        "Unsupported delegate repository type [" + delegateType + "] for setting [" + DELEGATE_TYPE_SETTING.getKey() + "]"
                    );
                }
                final String repositoryPasswordName = PASSWORD_NAME_SETTING.get(metadata.settings());
                if (Strings.hasLength(repositoryPasswordName) == false) {
                    throw new IllegalArgumentException("Repository setting [" + PASSWORD_NAME_SETTING.getKey() + "] must be set");
                }
                final SecureString repositoryPassword = repositoryPasswordsMap.get(repositoryPasswordName);
                if (repositoryPassword == null) {
                    throw new IllegalArgumentException(
                        "Secure setting ["
                            + ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryPasswordName).getKey()
                            + "] must be set"
                    );
                }
                final Repository delegatedRepository = factory.create(
                    new RepositoryMetadata(metadata.name(), delegateType, metadata.settings())
                );
                if (false == (delegatedRepository instanceof BlobStoreRepository) || delegatedRepository instanceof EncryptedRepository) {
                    throw new IllegalArgumentException("Unsupported delegate repository type [" + DELEGATE_TYPE_SETTING.getKey() + "]");
                }
                if (false == ENCRYPTED_SNAPSHOT_FEATURE.check(getLicenseState())) {
                    logger.warn(
                        new ParameterizedMessage(
                            "Encrypted snapshots are not allowed for the currently installed license [{}]."
                                + " Snapshots to the [{}] encrypted repository are not permitted."
                                + " All the other operations, including restore, work without restrictions.",
                            getLicenseState().getOperationMode().description(),
                            metadata.name()
                        ),
                        LicenseUtils.newComplianceException("encrypted snapshots")
                    );
                }
                return createEncryptedRepository(
                    metadata,
                    registry,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    (BlobStoreRepository) delegatedRepository,
                    () -> getLicenseState(),
                    repositoryPassword
                );
            }
        });
    }

    // protected for tests
    protected EncryptedRepository createEncryptedRepository(
        RepositoryMetadata metadata,
        NamedXContentRegistry registry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        BlobStoreRepository delegatedRepository,
        Supplier<XPackLicenseState> licenseStateSupplier,
        SecureString repoPassword
    ) throws GeneralSecurityException {
        return new EncryptedRepository(
            metadata,
            registry,
            clusterService,
            bigArrays,
            recoverySettings,
            delegatedRepository,
            licenseStateSupplier,
            repoPassword
        );
    }
}
