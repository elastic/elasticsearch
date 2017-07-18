/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.elasticsearch.xpack.security.support.IndexLifecycleManager;
import org.elasticsearch.xpack.security.user.BeatsSystemUser;
import org.elasticsearch.xpack.security.user.BuiltinUserInfo;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.security.user.User;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_INDEX_NAME;
import static org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.INDEX_TYPE;
import static org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.RESERVED_USER_TYPE;

/**
 * Performs migration steps for the {@link NativeRealm} and {@link ReservedRealm}.
 * When upgrading an Elasticsearch/X-Pack installation from a previous version, this class is responsible for ensuring that user/role
 * data stored in the security index is converted to a format that is appropriate for the newly installed version.
 */
public class NativeRealmMigrator implements IndexLifecycleManager.IndexDataMigrator {

    private final XPackLicenseState licenseState;
    private final Logger logger;
    private InternalClient client;
    private final BuiltinUserInfo[] builtinUsers = new BuiltinUserInfo[] {
            LogstashSystemUser.USER_INFO,
            BeatsSystemUser.USER_INFO
    };

    public NativeRealmMigrator(Settings settings, XPackLicenseState licenseState, InternalClient internalClient) {
        this.licenseState = licenseState;
        this.logger = Loggers.getLogger(getClass(), settings);
        this.client = internalClient;
    }

    /**
     * Special care must be taken because this upgrade happens <strong>before</strong> the security-mapping is updated.
     * We do it in that order because the version of the security-mapping controls the behaviour of the
     * reserved and native realm
     *
     * @param listener A listener for the results of the upgrade. Calls {@link ActionListener#onFailure(Exception)} if a problem occurs,
     *                 {@link ActionListener#onResponse(Object) onResponse(true)} if an upgrade is performed, or
     *                 {@link ActionListener#onResponse(Object) onResponse(false)} if no upgrade was required.
     * @see SecurityLifecycleService#securityIndexMappingAndTemplateSufficientToRead(ClusterState, Logger)
     * @see SecurityLifecycleService#isSecurityIndexWriteable
     * @see IndexLifecycleManager#mappingVersion
     */
    @Override
    public void performUpgrade(@Nullable Version previousVersion, ActionListener<Boolean> listener) {
        try {
            List<BiConsumer<Version, ActionListener<Void>>> tasks = collectUpgradeTasks(previousVersion);
            if (tasks.isEmpty()) {
                listener.onResponse(false);
            } else {
                final GroupedActionListener<Void> countDownListener = new GroupedActionListener<>(
                    ActionListener.wrap(r -> listener.onResponse(true), listener::onFailure), tasks.size(), emptyList()
                );
                logger.info("Performing {} security migration task(s) from version {}", tasks.size(), previousVersion);
                tasks.forEach(t -> t.accept(previousVersion, countDownListener));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private List<BiConsumer<Version, ActionListener<Void>>> collectUpgradeTasks(@Nullable Version previousVersion) {
        List<BiConsumer<Version, ActionListener<Void>>> tasks = new ArrayList<>();
        for (BuiltinUserInfo info : builtinUsers) {
            if (isNewUser(previousVersion, info)) {
                tasks.add((v,l) -> createUserAsDisabled(info, v, l));
            }
        }

        if (shouldConvertDefaultPasswords(previousVersion)) {
            tasks.add(this::doConvertDefaultPasswords);
        }
        return tasks;
    }

    /**
     * If we're upgrading from a security version where the new user did not exist, then we mark the user as disabled.
     * Otherwise the user will exist with a default password, which is desirable for an <em>out-of-the-box</em> experience in fresh
     * installs but problematic for already-locked-down upgrades.
     */
    private boolean isNewUser(@Nullable Version previousVersion, BuiltinUserInfo info) {
        return previousVersion != null
                && previousVersion.before(info.getDefinedSince())
                && previousVersion.onOrAfter(Version.V_5_0_0);
    }

    private void createUserAsDisabled(BuiltinUserInfo info, @Nullable Version previousVersion, ActionListener<Void> listener) {
        logger.info("Upgrading security from version [{}] - new reserved user [{}] will default to disabled",
                previousVersion, info.getName());
        // Only clear the cache is authentication is allowed by the current license
        // otherwise the license management checks will prevent it from completing successfully.
        final boolean clearCache = licenseState.isAuthAllowed();
        final String userName = info.getName();
        client.prepareGet(SECURITY_INDEX_NAME, INDEX_TYPE, getIdForUser(userName)).execute(
            ActionListener.wrap(getResponse -> {
                if (getResponse.isExists()) {
                    // the document exists - we shouldn't do anything
                    listener.onResponse(null);
                } else {
                    client.prepareIndex(SECURITY_INDEX_NAME, INDEX_TYPE, getIdForUser(userName))
                          .setSource(Requests.INDEX_CONTENT_TYPE, User.Fields.ENABLED.getPreferredName(), false,
                                     User.Fields.PASSWORD.getPreferredName(), "",
                                     User.Fields.TYPE.getPreferredName(), RESERVED_USER_TYPE)
                          .setCreate(true)
                          .execute(ActionListener.wrap(r -> {
                              if (clearCache) {
                                  new SecurityClient(client).prepareClearRealmCache()
                                        .usernames(userName)
                                        .execute(ActionListener.wrap(re -> listener.onResponse(null), listener::onFailure));
                              } else {
                                  listener.onResponse(null);
                              }
                          }, listener::onFailure));
                }
            }, listener::onFailure));
    }

    /**
     * Old versions of X-Pack security would assign the default password content to a user if it was enabled/disabled before the
     * password was explicitly set to another value. If upgrading from one of those versions, then we want to change those users to be
     * flagged as having a "default password" (which is stored as blank) so that {@link ReservedRealm#ACCEPT_DEFAULT_PASSWORD_SETTING}
     * does the right thing.
     */
    private boolean shouldConvertDefaultPasswords(@Nullable Version previousVersion) {
        return previousVersion != null
                && previousVersion.before(Version.V_6_0_0_alpha1)
                && previousVersion.onOrAfter(Version.V_5_0_0);
    }

    @SuppressWarnings("unused")
    private void doConvertDefaultPasswords(@Nullable Version previousVersion, ActionListener<Void> listener) {
        client.prepareSearch(SECURITY_INDEX_NAME)
            .setQuery(QueryBuilders.termQuery(User.Fields.TYPE.getPreferredName(), RESERVED_USER_TYPE))
            .setFetchSource(true)
            .execute(ActionListener.wrap(searchResponse -> {
                assert searchResponse.getHits().getTotalHits() <= 10 :
                    "there are more than 10 reserved users we need to change this to retrieve them all!";
                Set<String> toConvert = new HashSet<>();
                for (SearchHit searchHit : searchResponse.getHits()) {
                    Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                    if (hasOldStyleDefaultPassword(sourceMap)) {
                        toConvert.add(searchHit.getId());
                    }
                }

                if (toConvert.isEmpty()) {
                    listener.onResponse(null);
                } else {
                    GroupedActionListener<UpdateResponse> countDownListener = new GroupedActionListener<>(
                        ActionListener.wrap((r) -> listener.onResponse(null), listener::onFailure), toConvert.size(), emptyList()
                    );
                    toConvert.forEach(username -> {
                        logger.debug("Upgrading security from version [{}] - marking reserved user [{}] as having default password",
                                     previousVersion, username);
                        client.prepareUpdate(SECURITY_INDEX_NAME, INDEX_TYPE, getIdForUser(username))
                              .setDoc(User.Fields.PASSWORD.getPreferredName(), "",
                                      User.Fields.TYPE.getPreferredName(), RESERVED_USER_TYPE)
                              .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                              .execute(countDownListener);
                    });
                }
            }, listener::onFailure));
    }

    /**
     * Determines whether the supplied source as a {@link Map} has its password explicitly set to be the default password
     */
    public static boolean hasOldStyleDefaultPassword(Map<String, Object> userSource) {
        // TODO we should store the hash as something other than a string... bytes?
        final String passwordHash = (String) userSource.get(User.Fields.PASSWORD.getPreferredName());
        if (passwordHash == null) {
            throw new IllegalStateException("passwordHash should never be null");
        } else if (passwordHash.isEmpty()) {
            // we know empty is the new style
            return false;
        }

        try (SecureString secureString = new SecureString(passwordHash.toCharArray())) {
            return Hasher.BCRYPT.verify(ReservedRealm.EMPTY_PASSWORD_TEXT, secureString.getChars());
        }
    }

    /**
     * Gets the document's id field for the given user name.
     */
    private static String getIdForUser(final String userName) {
        return RESERVED_USER_TYPE + "-" + userName;
    }
}
