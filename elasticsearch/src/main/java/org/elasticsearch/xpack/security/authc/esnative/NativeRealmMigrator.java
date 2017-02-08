/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.common.GroupedActionListener;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;

/**
 * Performs migration steps for the {@link NativeRealm} and {@link ReservedRealm}.
 * When upgrading an Elasticsearch/X-Pack installation from a previous version, this class is responsible for ensuring that user/role
 * data stored in the security index is converted to a format that is appropriate for the newly installed version.
 *
 * @see SecurityTemplateService
 */
public class NativeRealmMigrator {

    private final NativeUsersStore nativeUsersStore;
    private final XPackLicenseState licenseState;
    private final Logger logger;

    public NativeRealmMigrator(Settings settings, NativeUsersStore nativeUsersStore, XPackLicenseState licenseState) {
        this.nativeUsersStore = nativeUsersStore;
        this.licenseState = licenseState;
        this.logger = Loggers.getLogger(getClass(), settings);
    }

    /**
     * Special care must be taken because this upgrade happens <strong>before</strong> the security-mapping is updated.
     * We do it in that order because the version of the security-mapping controls the behaviour of the
     * reserved and native realm
     *
     * @param listener A listener for the results of the upgrade. Calls {@link ActionListener#onFailure(Exception)} if a problem occurs,
     *                 {@link ActionListener#onResponse(Object) onResponse(true)} if an upgrade is performed, or
     *                 {@link ActionListener#onResponse(Object) onResponse(false)} if no upgrade was required.
     * @see SecurityTemplateService#securityIndexMappingAndTemplateSufficientToRead(ClusterState, Logger)
     * @see NativeUsersStore#canWrite
     * @see NativeUsersStore#mappingVersion
     */
    public void performUpgrade(@Nullable Version previousVersion, ActionListener<Boolean> listener) {
        try {
            List<BiConsumer<Version, ActionListener<Void>>> tasks = collectUpgradeTasks(previousVersion);
            if (tasks.isEmpty()) {
                listener.onResponse(false);
            } else {
                final GroupedActionListener<Void> countDownListener = new GroupedActionListener<>(
                        ActionListener.wrap(r -> listener.onResponse(true), listener::onFailure),
                        tasks.size(),
                        Collections.emptyList()
                );
                logger.info("Performing {} security migration task(s)", tasks.size());
                tasks.forEach(t -> t.accept(previousVersion, countDownListener));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private List<BiConsumer<Version, ActionListener<Void>>> collectUpgradeTasks(@Nullable Version previousVersion) {
        List<BiConsumer<Version, ActionListener<Void>>> tasks = new ArrayList<>();
        if (shouldDisableLogstashUser(previousVersion)) {
            tasks.add(this::doDisableLogstashUser);
        }
        if (shouldConvertDefaultPasswords(previousVersion)) {
            tasks.add(this::doConvertDefaultPasswords);
        }
        return tasks;
    }

    /**
     * If we're upgrading from a security version where the {@link LogstashSystemUser} did not exist, then we mark the user as disabled.
     * Otherwise the user will exist with a default password, which is desirable for an <em>out-of-the-box</em> experience in fresh installs
     * but problematic for already-locked-down upgrades.
     */
    private boolean shouldDisableLogstashUser(@Nullable Version previousVersion) {
        return previousVersion != null && previousVersion.before(LogstashSystemUser.DEFINED_SINCE);
    }

    private void doDisableLogstashUser(@Nullable Version previousVersion, ActionListener<Void> listener) {
        logger.info("Upgrading security from version [{}] - new reserved user [{}] will default to disabled",
                previousVersion, LogstashSystemUser.NAME);
        // Only clear the cache is authentication is allowed by the current license
        // otherwise the license management checks will prevent it from completing successfully.
        final boolean clearCache = licenseState.isAuthAllowed();
        nativeUsersStore.ensureReservedUserIsDisabled(LogstashSystemUser.NAME, clearCache, listener);
    }

    /**
     * Old versions of X-Pack security would assign the default password content to a user if it was enabled/disabled before the password
     * was explicitly set to another value. If upgrading from one of those versions, then we want to change those users to be flagged as
     * having a "default password" (which is stored as blank) so that {@link ReservedRealm#ACCEPT_DEFAULT_PASSWORD_SETTING} does the
     * right thing.
     */
    private boolean shouldConvertDefaultPasswords(@Nullable Version previousVersion) {
        return previousVersion != null && previousVersion.before(Version.V_6_0_0_alpha1_UNRELEASED);
    }

    @SuppressWarnings("unused")
    private void doConvertDefaultPasswords(@Nullable Version previousVersion, ActionListener<Void> listener) {
        nativeUsersStore.getAllReservedUserInfo(ActionListener.wrap(
                users -> {
                    final List<String> toConvert = users.entrySet().stream()
                            .filter(entry -> hasOldStyleDefaultPassword(entry.getValue()))
                            .map(entry -> entry.getKey())
                            .collect(Collectors.toList());
                    if (toConvert.isEmpty()) {
                        listener.onResponse(null);
                    } else {
                        GroupedActionListener countDownListener = new GroupedActionListener(
                                ActionListener.wrap((r) -> listener.onResponse(null), listener::onFailure),
                                toConvert.size(), Collections.emptyList()
                        );
                        toConvert.forEach(username -> {
                            logger.debug(
                                    "Upgrading security from version [{}] - marking reserved user [{}] as having default password",
                                    previousVersion, username);
                            resetReservedUserPassword(username, countDownListener);
                        });
                    }
                }, listener::onFailure)
        );
    }

    /**
     * Determines whether the supplied {@link NativeUsersStore.ReservedUserInfo} has its password set to be the default password, without
     * having the {@link NativeUsersStore.ReservedUserInfo#hasDefaultPassword} flag set.
     */
    private boolean hasOldStyleDefaultPassword(NativeUsersStore.ReservedUserInfo userInfo) {
        return userInfo.hasDefaultPassword == false && Hasher.BCRYPT.verify(ReservedRealm.DEFAULT_PASSWORD_TEXT, userInfo.passwordHash);
    }

    /**
     * Sets a reserved user's password back to blank, so that the default password functionality applies.
     */
    void resetReservedUserPassword(String username, ActionListener<Void> listener) {
        final ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(username);
        request.passwordHash(new char[0]);
        nativeUsersStore.changePassword(request, true, listener);
    }

}
