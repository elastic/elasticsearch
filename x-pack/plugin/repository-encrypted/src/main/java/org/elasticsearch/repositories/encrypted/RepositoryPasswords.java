/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.support.AESKeyUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.repositories.encrypted.EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING;
import static org.elasticsearch.repositories.encrypted.EncryptedRepositoryPlugin.logger;

public final class RepositoryPasswords {
    static final Setting<String> PASSWORD_NAME_SETTING = Setting.simpleString("password_name", "");
    static final Setting<String> PASSWORD_CHANGE_FROM_NAME_SETTING = Setting.simpleString("change_password_from_name", "");
    static final Setting<String> PASSWORD_CHANGE_TO_NAME_SETTING = Setting.simpleString("change_password_to_name", "");
    // TODO these are not really "settings"
    //  we need to find a better way to put these in the cluster state in relation to a repository
    public static final Setting.AffixSetting<String> PASSWORD_HASH_SETTING =
            Setting.prefixKeySetting("password_hash.", key -> Setting.simpleString(key));

    // all the repository password *values* pulled from the local node's keystore
    private final Map<String, SecureString> localRepositoryPasswordsMap;
    private final ThreadPool threadPool;
    private final Map<String, ListenableFuture<String>> localRepositoryPasswordHashesMap;
    private final Map<String, ListenableFuture<Boolean>> verifiedRepositoryPasswordHashesLru;

    public RepositoryPasswords(Map<String, SecureString> localRepositoryPasswordsMap, ThreadPool threadPool) {
        this.localRepositoryPasswordsMap = Map.copyOf(localRepositoryPasswordsMap);
        this.threadPool = threadPool;
        this.localRepositoryPasswordHashesMap = new ConcurrentHashMap<>(localRepositoryPasswordsMap.size());
        this.verifiedRepositoryPasswordHashesLru = new LinkedHashMap<>(3 * localRepositoryPasswordsMap.size(),
                0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, ListenableFuture<Boolean>> eldest) {
                return size() > 4 * localRepositoryPasswordsMap.size();
            }
        };
    }

    public Map<String, String> getPasswordHashes(RepositoryMetadata repositoryMetadata) {
        return PASSWORD_HASH_SETTING.getAsMap(repositoryMetadata.settings());
    }

    public boolean containsPasswordHashes(RepositoryMetadata repositoryMetadata) {
        return false == getPasswordHashes(repositoryMetadata).isEmpty();
    }

    public boolean containsRequiredPasswordHashes(RepositoryMetadata repositoryMetadata) {
        final Map<String, String> passwordHashes = getPasswordHashes(repositoryMetadata);
        final String passwordName = PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        if (Strings.hasLength(passwordName) == false) {
            throw new IllegalArgumentException("Repository setting [" + PASSWORD_NAME_SETTING.getKey() + "] must be set");
        }
        if (false == passwordHashes.containsKey(passwordName)) {
            return false;
        }
        final String fromPasswordName = PASSWORD_CHANGE_FROM_NAME_SETTING.get(repositoryMetadata.settings());
        final String toPasswordName = PASSWORD_CHANGE_TO_NAME_SETTING.get(repositoryMetadata.settings());
        if (Strings.hasLength(fromPasswordName) && false == Strings.hasLength(toPasswordName)) {
            throw new IllegalArgumentException("Repository setting [" + PASSWORD_CHANGE_FROM_NAME_SETTING.getKey() + "] is set" +
                    " but [" + PASSWORD_CHANGE_TO_NAME_SETTING.getKey() + "] is not, but they must be set together.");
        }
        if (false == Strings.hasLength(toPasswordName) && Strings.hasLength(fromPasswordName)) {
            throw new IllegalArgumentException("Repository setting [" + PASSWORD_CHANGE_FROM_NAME_SETTING.getKey() + "] is set" +
                    " but [" + PASSWORD_CHANGE_TO_NAME_SETTING.getKey() + "] is not, but they must be set together.");
        }
        if (Strings.hasLength(fromPasswordName) && false == passwordHashes.containsKey(fromPasswordName)) {
            return false;
        }
        if (Strings.hasLength(toPasswordName) && false == passwordHashes.containsKey(toPasswordName)) {
            return false;
        }
        return true;
    }

    public SecureString currentLocalPassword(RepositoryMetadata repositoryMetadata) {
        return localPasswords(repositoryMetadata).get(PASSWORD_NAME_SETTING.get(repositoryMetadata.settings()));
    }

    // password verification is not forked on a different thread because it is currently only called on IO threads
    public boolean verifyPublishedPasswordHashes(RepositoryMetadata repositoryMetadata) throws ExecutionException, InterruptedException {
        final Map<String, SecureString> localPasswords = localPasswords(repositoryMetadata);
        final Map<String, String> publishedPasswordHashes = getPasswordHashes(repositoryMetadata);
        for (Map.Entry<String, SecureString> localPassword : localPasswords.entrySet()) {
            logger.trace(() -> new ParameterizedMessage("Verifying hash for password [{}] of repository [{}]", localPassword.getKey(),
                    repositoryMetadata.name()));
            String publishedPasswordHash = publishedPasswordHashes.get(localPassword.getKey());
            if (publishedPasswordHash == null) {
                logger.debug(() -> new ParameterizedMessage("Missing hash for password [{}] of repository [{}]", localPassword.getKey(),
                        repositoryMetadata.name()));
                // the metadata names a password for which no hash has been published
                return false;
            } else {
                // cache the verified hash for a given named password
                boolean verifyResult =
                        this.verifiedRepositoryPasswordHashesLru.computeIfAbsent(localPassword.getKey() + publishedPasswordHash,
                                k -> AESKeyUtils.verifySaltedPasswordHash(localPassword.getValue(),
                                        publishedPasswordHash, EsExecutors.newDirectExecutorService())).get();
                if (false == verifyResult) {
                    logger.debug(() -> new ParameterizedMessage("Mismatch hash for password [{}] of repository [{}]",
                            localPassword.getKey(), repositoryMetadata.name()));
                    return false;
                }
            }
        }
        return true;
    }

    public void computePasswordHashes(RepositoryMetadata repositoryMetadata, ActionListener<Collection<Tuple<String, String>>> listener) {
        final Map<String, SecureString> localPasswords;
        try {
            localPasswords = localPasswords(repositoryMetadata);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final GroupedActionListener<Tuple<String, String>> passwordHashesGroupListener =
                new GroupedActionListener<>(ActionListener.wrap(passwordHashes -> {
                    if (isCryptoThread()) {
                        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> listener.onResponse(passwordHashes));
                    } else {
                        listener.onResponse(passwordHashes);
                    }
                }, e -> {
                    if (isCryptoThread()) {
                        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> listener.onFailure(e));
                    } else {
                        listener.onFailure(e);
                    }
                }), localPasswords.size());
        for (Map.Entry<String, SecureString> localPassword : localPasswords.entrySet()) {
            this.localRepositoryPasswordHashesMap.computeIfAbsent(localPassword.getKey(),
                    passwordName -> AESKeyUtils.computeSaltedPasswordHash(localPassword.getValue(),
                            threadPool.executor(SecurityField.SECURITY_CRYPTO_THREAD_POOL_NAME)))
                    .addListener(passwordHashesGroupListener.map(passwordHash -> new Tuple<>(localPassword.getKey(), passwordHash)),
                            EsExecutors.newDirectExecutorService());
        }
    }

    public Collection<Tuple<String, String>> computePasswordHashes(RepositoryMetadata repositoryMetadata) throws ExecutionException,
            InterruptedException {
        Map<String, SecureString> localPasswords = localPasswords(repositoryMetadata);
        Collection<Tuple<String, String>> passwordsHashes = new ArrayList<>();
        for (Map.Entry<String, SecureString> localPassword : localPasswords.entrySet()) {
            String passwordHash = this.localRepositoryPasswordHashesMap.computeIfAbsent(localPassword.getKey(),
                    passwordName -> AESKeyUtils.computeSaltedPasswordHash(localPassword.getValue(),
                            EsExecutors.newDirectExecutorService())).get();
            passwordsHashes.add(new Tuple<>(localPassword.getKey(), passwordHash));
        }
        return passwordsHashes;
    }

    public RepositoryMetadata withPasswordHashes(RepositoryMetadata repositoryMetadata,
                                                 Collection<Tuple<String, String>> passwordHashesToPublish) {
        if (false == getPasswordHashes(repositoryMetadata).isEmpty()) {
            throw new IllegalArgumentException("Will not overwrite password hashes");
        }
        Settings.Builder newSettingsBuilder = Settings.builder();
        newSettingsBuilder.put(repositoryMetadata.settings());
        for (Tuple<String, String> passwordNameAndHash : passwordHashesToPublish) {
            String passwordName = passwordNameAndHash.v1();
            String passwordHash = passwordNameAndHash.v2();
            newSettingsBuilder.put(PASSWORD_HASH_SETTING.getConcreteSettingForNamespace(passwordName).getKey(), passwordHash);
        }
        RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(repositoryMetadata.name(),
                repositoryMetadata.uuid(), repositoryMetadata.type(),
                    newSettingsBuilder.build(), repositoryMetadata.generation(), repositoryMetadata.pendingGeneration());
        if (getPasswordHashes(repositoryMetadata).isEmpty()) {
            throw new IllegalStateException("Inconsistency error when updating repository password hashes");
        }
        return newRepositoryMetadata;
    }

    private Map<String, SecureString> localPasswords(RepositoryMetadata repositoryMetadata) {
        Settings settings = repositoryMetadata.settings();
        Map<String, SecureString> localPasswordsSubset = new HashMap<>(3);
        String passwordName = PASSWORD_NAME_SETTING.get(settings);
        if (Strings.hasLength(passwordName) == false) {
            throw new IllegalArgumentException("Repository setting [" + PASSWORD_NAME_SETTING.getKey() + "] must be set");
        }
        SecureString password = localRepositoryPasswordsMap.get(passwordName);
        if (null == password) {
            throw new IllegalArgumentException(
                    "Secure setting ["
                            + ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(passwordName).getKey()
                            + "] must be set"
            );
        }
        localPasswordsSubset.put(passwordName, password);
        String fromPasswordName = PASSWORD_CHANGE_FROM_NAME_SETTING.get(settings);
        String toPasswordName = PASSWORD_CHANGE_TO_NAME_SETTING.get(settings);
        if (Strings.hasLength(fromPasswordName) && false == Strings.hasLength(toPasswordName)) {
            throw new IllegalArgumentException("Repository setting [" + PASSWORD_CHANGE_FROM_NAME_SETTING.getKey() + "] is set" +
                    " but [" + PASSWORD_CHANGE_TO_NAME_SETTING.getKey() + "] is not, but they must be set together.");
        }
        if (false == Strings.hasLength(toPasswordName) && Strings.hasLength(fromPasswordName)) {
            throw new IllegalArgumentException("Repository setting [" + PASSWORD_CHANGE_FROM_NAME_SETTING.getKey() + "] is set" +
                    " but [" + PASSWORD_CHANGE_TO_NAME_SETTING.getKey() + "] is not, but they must be set together.");
        }
        if (Strings.hasLength(fromPasswordName)) {
            SecureString fromPassword = localRepositoryPasswordsMap.get(fromPasswordName);
            if (null == fromPassword) {
                throw new IllegalArgumentException(
                        "Secure setting ["
                                + ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(fromPasswordName).getKey()
                                + "] must be set"
                );
            }
            localPasswordsSubset.put(fromPasswordName, fromPassword);
        }
        if (Strings.hasLength(toPasswordName)) {
            SecureString toPassword = localRepositoryPasswordsMap.get(toPasswordName);
            if (null == toPassword) {
                throw new IllegalArgumentException(
                        "Secure setting ["
                                + ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(toPasswordName).getKey()
                                + "] must be set"
                );
            }
            localPasswordsSubset.put(toPasswordName, toPassword);
        }
        return localPasswordsSubset;
    }

    private static boolean isCryptoThread() {
        return Thread.currentThread().getName().contains("[" + SecurityField.SECURITY_CRYPTO_THREAD_POOL_NAME + "]");
    }
}
