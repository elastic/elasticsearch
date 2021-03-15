/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.xpack.core.security.support.AESKeyUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.repositories.encrypted.EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING;

public final class RepositoryPasswords {
    static final Setting<String> PASSWORD_NAME_SETTING = Setting.simpleString("password_name", "");
    static final Setting<String> PASSWORD_CHANGE_TO_NAME_SETTING = Setting.simpleString("change_password_to_name", "");
    static final Setting<String> PASSWORD_CHANGE_FROM_NAME_SETTING = Setting.simpleString("change_password_from_name", "");
    // TODO these are not really "settings"
    //  we need to find a better way to put these in the cluster state in relation to a repository
    public static final Setting.AffixSetting<String> PASSWORD_HASH_SETTING =
            Setting.prefixKeySetting("password_hash.", key -> Setting.simpleString(key));

    private final String repositoryName;
    // all the repository password *values* pulled from the local node's keystore
    private final Map<String, SecureString> localRepositoryPasswordsMap;
    // password "hashes" that agree with the password values on the local node (to skip verification)
    private final Map<String, String> repositoryPasswordValidatedHashesMap;

    public RepositoryPasswords(Map<String, SecureString> localRepositoryPasswordsMap, RepositoryMetadata repositoryMetadata) {
        this.repositoryName = repositoryMetadata.name();
        this.localRepositoryPasswordsMap = Map.copyOf(localRepositoryPasswordsMap);
        this.repositoryPasswordValidatedHashesMap = new ConcurrentHashMap<>(localRepositoryPasswordsMap.size());
        settingsUpdate(repositoryMetadata.settings());
    }

    public void updateRepositoryUuidInMetadata(
            ClusterService clusterService,
            ActionListener<Void> listener) {
//        if (clusterService.localNode().isMasterNode()) {
//            listener.onResponse(null);
//            return;
//        }

        AESKeyUtils.generatePasswordBasedKey()

        final RepositoriesMetadata currentReposMetadata
                = clusterService.state().metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        final RepositoryMetadata repositoryMetadata = currentReposMetadata.repository(repositoryName);
        if (repositoryMetadata == null) {
            listener.onFailure(null);
            return;
        }



        clusterService.submitStateUpdateTask("update encrypted repository password hashes [" + repositoryName + "]",
                new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final RepositoriesMetadata currentReposMetadata
                                = currentState.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);

                        final RepositoryMetadata repositoryMetadata = currentReposMetadata.repository(repositoryName);
                        if (repositoryMetadata == null) {
                            return listener.onFailure(null);
                        } else {
                            final RepositoriesMetadata newReposMetadata = currentReposMetadata.withUuid(repositoryName, repositoryUuid);
                            final Metadata.Builder metadata
                                    = Metadata.builder(currentState.metadata()).putCustom(RepositoriesMetadata.TYPE, newReposMetadata);
                            return ClusterState.builder(currentState).metadata(metadata).build();
                        }
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(null);
                    }
                });
    }

    /** This method is used to change the in-use password of the snapshot repository.
     * Changing the currently in-use password uses a transitory state where both the old and the new passwords are simultaneously set.
     */
    public void settingsUpdate(Settings repositorySettings) {
        String passwordName = PASSWORD_NAME_SETTING.get(repositorySettings);
        if (Strings.hasLength(passwordName) == false) {
            throw new IllegalArgumentException("Repository setting [" + PASSWORD_NAME_SETTING.getKey() + "] must be set");
        }
        if (false == localRepositoryPasswordsMap.containsKey(passwordName)) {
            throw new IllegalArgumentException(
                    "Secure setting ["
                            + ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(passwordName).getKey()
                            + "] must be set"
            );
        }
        String newPasswordName = NEW_PASSWORD_NAME_SETTING.get(repositorySettings);
        String passwordChangeLock = PASSWORD_CHANGE_LOCK_SETTING.get(repositorySettings);
        if (Strings.hasLength(newPasswordName) && false == Strings.hasLength(passwordChangeLock)) {
            throw new IllegalArgumentException("Repository setting [" + NEW_PASSWORD_NAME_SETTING.getKey() + "] is set" +
                    " but [" + PASSWORD_CHANGE_LOCK_SETTING.getKey() + "] is not, but they must be set together.");
        }
        if (false == Strings.hasLength(newPasswordName) && Strings.hasLength(passwordChangeLock)) {
            throw new IllegalArgumentException("Repository setting [" + PASSWORD_CHANGE_LOCK_SETTING.getKey() + "] is set" +
                    " but [" + NEW_PASSWORD_NAME_SETTING.getKey() + "] is not, but they must be set together.");
        }
        if (Strings.hasLength(newPasswordName) && Strings.hasLength(passwordChangeLock)) {
            // a password change is in progress
            if (false == repositoryPasswordsMap.containsKey(newPasswordName)) {
                throw new IllegalArgumentException(
                        "Secure setting ["
                                + ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(newPasswordName).getKey()
                                + "] not set"
                );
            }
            this.newPasswordName = newPasswordName;
            this.passwordChangeLock = passwordChangeLock;
        } else {
            this.newPasswordName = null;
            this.passwordChangeLock = null;
        }
        this.currentPasswordName = passwordName;
    }
}
