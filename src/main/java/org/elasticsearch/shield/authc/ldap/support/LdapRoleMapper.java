/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.sdk.DN;
import com.unboundid.ldap.sdk.LDAPException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.dn;
import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.relativeName;

/**
 * This class loads and monitors the file defining the mappings of LDAP DNs to internal ES Roles.
 */
public class LdapRoleMapper {

    public static final String DEFAULT_FILE_NAME = "role_mapping.yml";
    public static final String ROLE_MAPPING_FILE_SETTING = "files.role_mapping";
    public static final String USE_UNMAPPED_GROUPS_AS_ROLES_SETTING = "unmapped_groups_as_roles";

    protected final ESLogger logger;
    protected final RealmConfig config;

    private final String realmType;
    private final Path file;
    private final boolean useUnmappedGroupsAsRoles;
    protected volatile ImmutableMap<DN, Set<String>> dnRoles;

    private CopyOnWriteArrayList<RefreshListener> listeners;

    public LdapRoleMapper(String realmType, RealmConfig config, ResourceWatcherService watcherService, @Nullable RefreshListener listener) {
        this.realmType = realmType;
        this.config = config;
        this.logger = config.logger(getClass());

        useUnmappedGroupsAsRoles = config.settings().getAsBoolean(USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, false);
        file = resolveFile(config.settings(), config.env());
        dnRoles = parseFileLenient(file, logger, realmType, config.name());
        FileWatcher watcher = new FileWatcher(file.getParent().toFile());
        watcher.addListener(new FileListener());
        watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        listeners = new CopyOnWriteArrayList<>();
        if (listener != null) {
            listeners.add(listener);
        }
    }

    public synchronized void addListener(RefreshListener listener) {
        listeners.add(listener);
    }

    public static Path resolveFile(Settings settings, Environment env) {
        String location = settings.get(ROLE_MAPPING_FILE_SETTING);
        if (location == null) {
            return ShieldPlugin.resolveConfigFile(env, DEFAULT_FILE_NAME);
        }
        return Paths.get(location);
    }

    /**
     * Internally in this class, we try to load the file, but if for some reason we can't, we're being more lenient by
     * logging the error and skipping/removing all mappings. This is aligned with how we handle other auto-loaded files
     * in shield.
     */
    public static ImmutableMap<DN, Set<String>> parseFileLenient(Path path, ESLogger logger, String realmType, String realmName) {
        try {
            return parseFile(path, logger, realmType, realmName);
        } catch (Throwable t) {
            logger.error("failed to parse role mappings file [{}]. skipping/removing all mappings...", t, path.toAbsolutePath());
            return ImmutableMap.of();
        }
    }

    public static ImmutableMap<DN, Set<String>> parseFile(Path path, ESLogger logger, String realmType, String realmName) {

        logger.trace("reading realm [{}/{}] role mappings file [{}]...", realmType, realmName, path.toAbsolutePath());

        if (!Files.exists(path)) {
            return ImmutableMap.of();
        }

        try (FileInputStream in = new FileInputStream(path.toFile())) {
            Settings settings = ImmutableSettings.builder()
                    .loadFromStream(path.toString(), in)
                    .build();

            Map<DN, Set<String>> dnToRoles = new HashMap<>();
            Set<String> roles = settings.names();
            for (String role : roles) {
                for (String ldapDN : settings.getAsArray(role)) {
                    try {
                        DN dn = new DN(ldapDN);
                        Set<String> dnRoles = dnToRoles.get(dn);
                        if (dnRoles == null) {
                            dnRoles = new HashSet<>();
                            dnToRoles.put(dn, dnRoles);
                        }
                        dnRoles.add(role);
                    } catch (LDAPException e) {
                        logger.error("invalid DN [{}] found in [{}] role mappings [{}] for realm [{}/{}]. skipping... ", e, ldapDN, realmType, path.toAbsolutePath(), realmType, realmName);
                    }
                }

            }

            if (dnToRoles.isEmpty()){
                logger.warn("no mappings found in role mappings file [{}] for realm [{}/{}]", path.toAbsolutePath(), realmType, realmName);
            }

            return ImmutableMap.copyOf(dnToRoles);

        } catch (IOException e) {
            throw new ShieldSettingsException("could not read realm [" + realmType + "/" + realmName + "] role mappings file [" + path.toAbsolutePath() + "]", e);
        }
    }

    int mappingsCount() {
        return dnRoles.size();
    }

    /**
     * This will map the groupDN's to ES Roles
     */
    public Set<String> resolveRoles(String userDnString, List<String> groupDns) {
        Set<String> roles = new HashSet<>();
        for (String groupDn : groupDns) {
            DN groupLdapName = dn(groupDn);
            if (dnRoles.containsKey(groupLdapName)) {
                roles.addAll(dnRoles.get(groupLdapName));
            } else if (useUnmappedGroupsAsRoles) {
                roles.add(relativeName(groupLdapName));
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("the roles [{}], are mapped from these [{}] groups [{}] for realm [{}/{}]", roles, realmType, groupDns, realmType, config.name());
        }

        DN userDn = dn(userDnString);
        Set<String> rolesMappedToUserDn = dnRoles.get(userDn);
        if (rolesMappedToUserDn != null) {
            roles.addAll(rolesMappedToUserDn);
            if (logger.isDebugEnabled()) {
                logger.debug("the roles [{}], are mapped from the user [{}] for realm [{}/{}]", roles, realmType, userDnString, realmType, config.name());
            }
        }
        return roles;
    }

    public void notifyRefresh() {
        for (RefreshListener listener : listeners) {
            listener.onRefresh();
        }
    }

    private class FileListener extends FileChangesListener {
        @Override
        public void onFileCreated(File file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(File file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(File file) {
            if (file.equals(LdapRoleMapper.this.file.toFile())) {
                logger.info("role mappings file [{}] changed for realm [{}/{}]. updating mappings...", file.getAbsolutePath(), realmType, config.name());
                dnRoles = parseFileLenient(file.toPath(), logger, realmType, config.name());
                notifyRefresh();
            }
        }
    }

}
