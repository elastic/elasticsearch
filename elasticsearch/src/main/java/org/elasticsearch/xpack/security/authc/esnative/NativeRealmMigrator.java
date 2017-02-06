/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;

/**
 * Performs migration steps for the {@link NativeRealm} and {@link ReservedRealm}.
 * When upgrading an Elasticsearch/X-Pack installation from a previous version, this class is responsible for ensuring that user/role
 * data stored in the security index is converted to a format that is appropriate for the newly installed version.
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
            if (shouldDisableLogstashUser(previousVersion)) {
                logger.info("Upgrading security from version [{}] - new reserved user [{}] will default to disabled",
                        previousVersion, LogstashSystemUser.NAME);
                // Only clear the cache is authentication is allowed by the current license
                // otherwise the license management checks will prevent it from completing successfully.
                final boolean clearCache = licenseState.isAuthAllowed();
                nativeUsersStore.ensureReservedUserIsDisabled(LogstashSystemUser.NAME, clearCache, new ActionListener<Void>() {
                            @Override
                            public void onResponse(Void aVoid) {
                                listener.onResponse(true);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
            } else {
                listener.onResponse(false);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * If we're upgrading from a security version where the {@link LogstashSystemUser} did not exist, then we mark the user as disabled.
     * Otherwise the user will exist with a default password, which is desirable for an <em>out-of-the-box</em> experience in fresh installs
     * but problematic for already-locked-down upgrades.
     */
    private boolean shouldDisableLogstashUser(@Nullable Version previousVersion) {
        return previousVersion != null && previousVersion.before(LogstashSystemUser.DEFINED_SINCE);
    }

}
