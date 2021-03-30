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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.support.AESKeyUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public final class RepositoryPasswords {
    static final Setting<String> CURRENT_PASSWORD_NAME_SETTING = Setting.simpleString("password_name", "");
    static final Setting<String> CHANGE_FROM_PASSWORD_NAME_SETTING = Setting.simpleString("change_from_password_name", "");
    static final Setting<String> CHANGE_TO_PASSWORD_NAME_SETTING = Setting.simpleString("change_to_password_name", "");
    // TODO these are not really "settings"
    // we need to find a better way to put these in the cluster state in relation to a repository
    public static final Setting.AffixSetting<String> PASSWORDS_HASH_SETTING = Setting.prefixKeySetting(
        "passwords_hash.",
        key -> Setting.simpleString(key)
    );

    static final Logger logger = LogManager.getLogger(RepositoryPasswords.class);

    // all the repository password *values* pulled from the local node's keystore
    private final Map<String, SecureString> localRepositoryPasswordsMap;
    private final ThreadPool threadPool;
    private final Map<String, ListenableFuture<String>> localRepositoryPasswordsHashMap;
    private final Map<String, ListenableFuture<Boolean>> verifiedRepositoryPasswordsHashLru;

    public RepositoryPasswords(Map<String, SecureString> localRepositoryPasswordsMap, ThreadPool threadPool) {
        this.localRepositoryPasswordsMap = Map.copyOf(localRepositoryPasswordsMap);
        this.threadPool = threadPool;
        this.localRepositoryPasswordsHashMap = new ConcurrentHashMap<>(localRepositoryPasswordsMap.size());
        this.verifiedRepositoryPasswordsHashLru = new LinkedHashMap<>(3 * localRepositoryPasswordsMap.size(), 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, ListenableFuture<Boolean>> eldest) {
                return size() > 4 * localRepositoryPasswordsMap.size();
            }
        };
    }

    public Map<String, String> getPasswordsHash(RepositoryMetadata repositoryMetadata) {
        return PASSWORDS_HASH_SETTING.getAsMap(repositoryMetadata.settings());
    }

    public boolean containsPasswordsHash(RepositoryMetadata repositoryMetadata) {
        return false == getPasswordsHash(repositoryMetadata).isEmpty();
    }

    public boolean equalsIgnorePasswordSettings(RepositoryMetadata repositoryMetadata1, RepositoryMetadata repositoryMetadata2) {
        if (false == repositoryMetadata1.type().equals(repositoryMetadata2.type())) {
            return false;
        }
        Predicate<String> passwordSettingsPredicate = settingName -> settingName.equals(CURRENT_PASSWORD_NAME_SETTING.getKey())
            || settingName.equals(CHANGE_FROM_PASSWORD_NAME_SETTING.getKey())
            || settingName.equals(CHANGE_TO_PASSWORD_NAME_SETTING.getKey())
            || PASSWORDS_HASH_SETTING.match(settingName);
        return repositoryMetadata1.settings()
            .filter(passwordSettingsPredicate.negate())
            .equals(repositoryMetadata2.settings().filter(passwordSettingsPredicate.negate()));
    }

    public SecureString currentLocalPassword(RepositoryMetadata repositoryMetadata) {
        return localPasswords(repositoryMetadata).get(CURRENT_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings()));
    }

    public List<SecureString> passwordsForDekWrapping(RepositoryMetadata repositoryMetadata) {
        Map<String, SecureString> localPasswords = localPasswords(repositoryMetadata);
        String currentPasswordName = CURRENT_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        SecureString currentPassword = localPasswords.get(currentPasswordName);
        String fromPasswordName = CHANGE_FROM_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        if (currentPasswordName.equals(fromPasswordName)) {
            // in-progress password change away from the current password
            String toPasswordName = CHANGE_TO_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
            SecureString toPassword = localPasswords.get(toPasswordName);
            return List.of(currentPassword, toPassword);
        } else {
            return List.of(currentPassword);
        }
    }

    public void updateWithHashForCurrentPassword(RepositoryMetadata repositoryMetadata,
                                                 ActionListener<RepositoryMetadata> listener) {
        String currentPasswordName = CURRENT_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        computePasswordHash(currentPasswordName, threadPool.generic(), ActionListener.wrap(currentPasswordHash -> {
            Settings.Builder newSettingsBuilder = Settings.builder();
            newSettingsBuilder.put(repositoryMetadata.settings());
            newSettingsBuilder.put(PASSWORDS_HASH_SETTING.getConcreteSettingForNamespace(currentPasswordName).getKey(),
                    currentPasswordHash);
            RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(
                    repositoryMetadata.name(),
                    repositoryMetadata.uuid(),
                    repositoryMetadata.type(),
                    newSettingsBuilder.build(),
                    repositoryMetadata.generation(),
                    repositoryMetadata.pendingGeneration()
            );
            listener.onResponse(newRepositoryMetadata);
        }, listener::onFailure));
    }

    public boolean isPasswordChangeInProgress(RepositoryMetadata repositoryMetadata) {
        return localPasswords(repositoryMetadata).size() > 1;
    }

    public Map<String, String> computePasswordsHashForBlobWrite(RepositoryMetadata repositoryMetadata) throws ExecutionException,
            InterruptedException {
        final String currentPasswordName = CURRENT_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        final String fromPasswordName = CHANGE_FROM_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        final String toPasswordName = CHANGE_TO_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        // only the passwords that can be used to encrypt blobs
        final Set<String> hashesToCompute;
        if (isPasswordChangeInProgress(repositoryMetadata) && currentPasswordName.equals(fromPasswordName)) {
            hashesToCompute = Set.of(currentPasswordName, toPasswordName);
        } else {
            hashesToCompute = Set.of(currentPasswordName);
        }
        final StepListener<Map<String, String>> waitStep = new StepListener<>();
        computePasswordsHash(hashesToCompute, EsExecutors.newDirectExecutorService(), waitStep);
        return waitStep.asFuture().get();
    }

    public void verifyPublishedPasswordsHashForBlobWrite(RepositoryMetadata repositoryMetadata, ActionListener<Void> listener) {
        verifyPublishedPasswordsHashForBlobWrite(repositoryMetadata, threadPool.generic(), listener);
    }

    public void verifyPublishedPasswordsHashForBlobWrite(RepositoryMetadata repositoryMetadata) throws ExecutionException,
            InterruptedException {
        StepListener<Void> stepWait = new StepListener<>();
        verifyPublishedPasswordsHashForBlobWrite(repositoryMetadata, stepWait);
        stepWait.asFuture().get();
    }

    private void verifyPublishedPasswordsHashForBlobWrite(RepositoryMetadata repositoryMetadata,
                                                          ExecutorService executor,
                                                          ActionListener<Void> listener) {
        final Map<String, String> publishedPasswordsHash = getPasswordsHash(repositoryMetadata);
        final String currentPasswordName = CURRENT_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        final String fromPasswordName = CHANGE_FROM_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        final String toPasswordName = CHANGE_TO_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        // only the passwords that can be used to encrypt blobs
        final Map<String, String> hashesToVerify;
        try {
            if (isPasswordChangeInProgress(repositoryMetadata) && currentPasswordName.equals(fromPasswordName)) {
                hashesToVerify = Map.of(currentPasswordName, publishedPasswordsHash.get(currentPasswordName), toPasswordName,
                        publishedPasswordsHash.get(toPasswordName));
            } else {
                hashesToVerify = Map.of(currentPasswordName, publishedPasswordsHash.get(currentPasswordName));
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        verifyPasswordsHash(hashesToVerify, executor, ActionListener.wrap(verifyResult -> {
            if (false == verifyResult) {
                listener.onFailure(new IllegalArgumentException("Local repository passwords are different"));
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    public void verifyPublishedPasswordsHashForChange(RepositoryMetadata repositoryMetadata, ActionListener<Void> listener) {
        final Map<String, String> publishedPasswordsHash = getPasswordsHash(repositoryMetadata);
        final String fromPasswordName = CHANGE_FROM_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        final String toPasswordName = CHANGE_TO_PASSWORD_NAME_SETTING.get(repositoryMetadata.settings());
        final Map<String, String> hashesToVerify = Map.of(fromPasswordName, publishedPasswordsHash.get(fromPasswordName),
                toPasswordName, publishedPasswordsHash.get(toPasswordName));
        verifyPasswordsHash(hashesToVerify, threadPool.generic(), ActionListener.wrap(verifyResult -> {
            if (false == verifyResult) {
                listener.onFailure(new IllegalArgumentException("Local passwords different from when password change started"));
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    public boolean verifyPasswordsHash(Map<String, String> passwordsHash) throws ExecutionException, InterruptedException {
        StepListener<Boolean> waitStep = new StepListener<>();
        verifyPasswordsHash(passwordsHash, EsExecutors.newDirectExecutorService(), waitStep);
        return waitStep.asFuture().get();
    }

    private void verifyPasswordsHash(Map<String, String> passwordsHash, ExecutorService executor, ActionListener<Boolean> listener) {
        if (passwordsHash == null || passwordsHash.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("No passwords hash to verify"));
            return;
        }
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicInteger hashesToVerifyCount = new AtomicInteger(0);
        for (Map.Entry<String, String> passwordNameAndHash : passwordsHash.entrySet()) {
            if (false == Strings.hasLength(passwordNameAndHash.getValue())) {
                listener.onFailure(new IllegalStateException("Null or empty hash for [" + passwordNameAndHash.getKey() + "]"));
                return;
            }
            SecureString password = localRepositoryPasswordsMap.get(passwordNameAndHash.getKey());
            if (password == null) {
                logger.debug(() -> new ParameterizedMessage("Missing local password [{}] to verify",
                        passwordNameAndHash.getKey()));
                listener.onFailure(new IllegalArgumentException("Missing local password [" + passwordNameAndHash.getKey() +
                        "] to verify"));
                return;
            } else {
                hashesToVerifyCount.incrementAndGet();
            }
        }
        for (Map.Entry<String, String> passwordNameAndHash : passwordsHash.entrySet()) {
            SecureString password = localRepositoryPasswordsMap.get(passwordNameAndHash.getKey());
            this.verifiedRepositoryPasswordsHashLru.computeIfAbsent(
                    passwordNameAndHash.getKey() + passwordNameAndHash.getValue(),
                    k -> AESKeyUtils.verifySaltedPasswordHash(
                            password,
                            passwordNameAndHash.getValue(),
                            threadPool.executor(SecurityField.SECURITY_CRYPTO_THREAD_POOL_NAME)
                    )
            ).addListener(ActionListener.wrap(hashVerify -> {
                if (hashesToVerifyCount.decrementAndGet() == 0 && hashVerify && done.compareAndSet(false, true)) {
                    if (isCryptoThread()) {
                        executor.execute(() -> listener.onResponse(true));
                    } else {
                        listener.onResponse(true);
                    }
                } else if (false == hashVerify && done.compareAndSet(false, true)) {
                    if (isCryptoThread()) {
                        executor.execute(() -> listener.onResponse(false));
                    } else {
                        listener.onResponse(false);
                    }
                }
            }, e -> {
                if (done.compareAndSet(false, true)) {
                    if (isCryptoThread()) {
                        executor.execute(() -> listener.onFailure(e));
                    } else {
                        listener.onFailure(e);
                    }
                }
            }), EsExecutors.newDirectExecutorService());
        }
    }

    // also ensures settings integrity, but does not validate hashes and does not ensure that referenced passwords exist
    private Map<String, SecureString> localPasswords(RepositoryMetadata repositoryMetadata) {
        Settings settings = repositoryMetadata.settings();
        Map<String, SecureString> localPasswordsSubset = new HashMap<>(3);
        Set<String> passwordsHash = new HashSet<>(getPasswordsHash(repositoryMetadata).keySet());
        String passwordName = CURRENT_PASSWORD_NAME_SETTING.get(settings);
        if (Strings.hasLength(passwordName) == false) {
            throw new IllegalArgumentException("Missing repository setting [" + CURRENT_PASSWORD_NAME_SETTING.getKey() + "]");
        }
        SecureString password = localRepositoryPasswordsMap.get(passwordName);
        passwordsHash.remove(passwordName);
        localPasswordsSubset.put(passwordName, password);
        String fromPasswordName = CHANGE_FROM_PASSWORD_NAME_SETTING.get(settings);
        String toPasswordName = CHANGE_TO_PASSWORD_NAME_SETTING.get(settings);
        if (Strings.hasLength(fromPasswordName) && false == Strings.hasLength(toPasswordName)) {
            throw new IllegalArgumentException(
                "Repository setting ["
                    + CHANGE_FROM_PASSWORD_NAME_SETTING.getKey()
                    + "] is set but ["
                    + CHANGE_TO_PASSWORD_NAME_SETTING.getKey()
                    + "] is not, yet they must be set together."
            );
        }
        if (false == Strings.hasLength(fromPasswordName) && Strings.hasLength(toPasswordName)) {
            throw new IllegalArgumentException(
                "Repository setting ["
                    + CHANGE_FROM_PASSWORD_NAME_SETTING.getKey()
                    + "] is not set but ["
                    + CHANGE_TO_PASSWORD_NAME_SETTING.getKey()
                    + "] is set, yet they must be set together."
            );
        }
        if (Strings.hasLength(fromPasswordName)) {
            SecureString fromPassword = localRepositoryPasswordsMap.get(fromPasswordName);
            if (false == passwordsHash.remove(fromPasswordName)) {
                throw new IllegalArgumentException("Missing password hash for [" + fromPasswordName + "]");
            }
            localPasswordsSubset.put(fromPasswordName, fromPassword);
        }
        if (Strings.hasLength(toPasswordName)) {
            SecureString toPassword = localRepositoryPasswordsMap.get(toPasswordName);
            if (false == passwordsHash.remove(toPasswordName)) {
                throw new IllegalArgumentException("Missing password hash for [" + toPasswordName + "]");
            }
            localPasswordsSubset.put(toPasswordName, toPassword);
        }
        if (false == passwordsHash.isEmpty()) {
            throw new IllegalArgumentException("Unexpected extra passwords hash ["
                    + Strings.collectionToCommaDelimitedString(passwordsHash)
                    + "]");
        }
        return localPasswordsSubset;
    }

    private void computePasswordsHash(Set<String> passwordsName, ExecutorService executor, ActionListener<Map<String, String>> listener) {
        if (passwordsName == null || passwordsName.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("Null or empty passwords set to compute hashes for"));
            return;
        }
        final AtomicBoolean done = new AtomicBoolean(false);
        final ConcurrentHashMap<String, String> computedHashes = new ConcurrentHashMap<>(passwordsName.size());
        for (String passwordName : passwordsName) {
            computePasswordHash(passwordName, EsExecutors.newDirectExecutorService(), ActionListener.wrap(computedHash -> {
                computedHashes.put(passwordName, computedHash);
                if (computedHashes.size() == passwordsName.size() && done.compareAndSet(false, true)) {
                    if (isCryptoThread()) {
                        executor.execute(() -> listener.onResponse(computedHashes));
                    } else {
                        listener.onResponse(computedHashes);
                    }
                }
            }, e -> {
                if (done.compareAndSet(false, true)) {
                    if (isCryptoThread()) {
                        executor.execute(() -> listener.onFailure(e));
                    } else {
                        listener.onFailure(e);
                    }
                }
            }));
        }
    }

    private void computePasswordHash(String passwordName, ExecutorService executor, ActionListener<String> listener) {
        SecureString password = this.localRepositoryPasswordsMap.get(passwordName);
        if (null == password) {
            listener.onFailure(new IllegalArgumentException("Missing local password [" + passwordName + "]"));
            return;
        }
        this.localRepositoryPasswordsHashMap.computeIfAbsent(
                passwordName,
                k -> AESKeyUtils.computeSaltedPasswordHash(
                        password,
                        threadPool.executor(SecurityField.SECURITY_CRYPTO_THREAD_POOL_NAME)
                )
        ).addListener(listener, executor);
    }

    private static boolean isCryptoThread() {
        return Thread.currentThread().getName().contains("[" + SecurityField.SECURITY_CRYPTO_THREAD_POOL_NAME + "]");
    }
}
