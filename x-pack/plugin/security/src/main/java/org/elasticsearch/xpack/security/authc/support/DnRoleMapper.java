/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support;

import com.unboundid.ldap.sdk.DN;
import com.unboundid.ldap.sdk.LDAPException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.elasticsearch.xpack.security.PrivilegedFileWatcher;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.support.mapper.AbstractRoleMapperClearRealmCache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.security.AccessController.doPrivileged;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.dn;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.relativeName;

/**
 * This class loads and monitors the file defining the mappings of DNs to internal ES Roles.
 */
public class DnRoleMapper extends AbstractRoleMapperClearRealmCache {
    private static final Logger logger = LogManager.getLogger(DnRoleMapper.class);

    protected final RealmConfig config;

    private final Path file;
    private final boolean useUnmappedGroupsAsRoles;
    private volatile Map<String, List<String>> dnRoles;

    public DnRoleMapper(RealmConfig config, ResourceWatcherService watcherService) {
        this.config = config;
        useUnmappedGroupsAsRoles = config.getSetting(DnRoleMapperSettings.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING);
        file = resolveFile(config);
        dnRoles = doPrivileged(
            (PrivilegedAction<Map<String, List<String>>>) () -> parseFileLenient(file, logger, config.type(), config.name())
        );
        FileWatcher watcher = new PrivilegedFileWatcher(file.getParent());
        watcher.addListener(new FileListener());
        try {
            watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to start file watcher for role mapping file [" + file.toAbsolutePath() + "]", e);
        }
    }

    public static Path resolveFile(RealmConfig realmConfig) {
        String location = realmConfig.getSetting(DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING);
        return Security.resolveSecuredConfigFile(realmConfig.env(), location);
    }

    /**
     * Internally in this class, we try to load the file, but if for some reason we can't, we're being more lenient by
     * logging the error and skipping/removing all mappings. This is aligned with how we handle other auto-loaded files
     * in security.
     */
    public static Map<String, List<String>> parseFileLenient(Path path, Logger logger, String realmType, String realmName) {
        try {
            return parseFile(path, logger, realmType, realmName, false);
        } catch (Exception e) {
            logger.error(
                () -> format("failed to parse role mappings file [%s]. skipping/removing all mappings...", path.toAbsolutePath()),
                e
            );
            return emptyMap();
        }
    }

    public static Map<String, List<String>> parseFile(Path path, Logger logger, String realmType, String realmName, boolean strict) {

        logger.trace("reading realm [{}/{}] role mappings file [{}]...", realmType, realmName, path.toAbsolutePath());

        if (Files.exists(path) == false) {
            final String message = org.elasticsearch.core.Strings.format(
                "Role mapping file [%s] for realm [%s] does not exist.",
                path.toAbsolutePath(),
                realmName
            );
            if (strict) {
                throw new ElasticsearchException(message);
            } else {
                logger.warn(message + " Role mapping will be skipped.");
                return emptyMap();
            }
        }

        try {
            // create this here so it's in an allowed stack frame
            var file = Files.newInputStream(path);
            Settings settings = Settings.builder().loadFromStream(path.getFileName().toString(), file, false).build();

            Map<DN, Set<String>> dnToRoles = new HashMap<>();
            Set<String> roles = settings.names();
            for (String role : roles) {
                for (String providedDn : settings.getAsList(role)) {
                    try {
                        DN dn = new DN(providedDn);
                        Set<String> dnRoles = dnToRoles.get(dn);
                        if (dnRoles == null) {
                            dnRoles = new HashSet<>();
                            dnToRoles.put(dn, dnRoles);
                        }
                        dnRoles.add(role);
                    } catch (LDAPException e) {
                        String message = Strings.format(
                            "invalid DN [%s] found in [%s] role mappings [%s] for realm [%s/%s].",
                            providedDn,
                            realmType,
                            path.toAbsolutePath(),
                            realmType,
                            realmName
                        );
                        if (strict) {
                            throw new ElasticsearchException(message, e);
                        } else {
                            logger.error(message + " skipping...", e);
                        }
                    }
                }

            }

            logger.debug(
                "[{}] role mappings found in file [{}] for realm [{}/{}]",
                dnToRoles.size(),
                path.toAbsolutePath(),
                realmType,
                realmName
            );
            Map<String, List<String>> normalizedMap = dnToRoles.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toNormalizedString(), entry -> List.copyOf(entry.getValue())));
            return unmodifiableMap(normalizedMap);
        } catch (IOException | SettingsException e) {
            throw new ElasticsearchException(
                "could not read realm [" + realmType + "/" + realmName + "] role mappings file [" + path.toAbsolutePath() + "]",
                e
            );
        }
    }

    int mappingsCount() {
        return dnRoles.size();
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        try {
            listener.onResponse(resolveRoles(user.getDn(), user.getGroups()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * This will map the groupDN's to ES Roles
     */
    public Set<String> resolveRoles(String userDnString, Collection<String> groupDns) {
        Set<String> roles = new HashSet<>();
        for (String groupDnString : groupDns) {
            DN groupDn = dn(groupDnString);
            String normalizedGroupDn = groupDn.toNormalizedString();
            if (dnRoles.containsKey(normalizedGroupDn)) {
                roles.addAll(dnRoles.get(normalizedGroupDn));
            } else if (useUnmappedGroupsAsRoles) {
                roles.add(relativeName(groupDn));
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug(
                "the roles [{}], are mapped from these [{}] groups [{}] using file [{}] for realm [{}/{}]",
                roles,
                config.type(),
                groupDns,
                file.getFileName(),
                config.type(),
                config.name()
            );
        }

        String normalizedUserDn = dn(userDnString).toNormalizedString();
        List<String> rolesMappedToUserDn = dnRoles.get(normalizedUserDn);
        if (rolesMappedToUserDn != null) {
            roles.addAll(rolesMappedToUserDn);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(
                "the roles [{}], are mapped from the user [{}] using file [{}] for realm [{}/{}]",
                (rolesMappedToUserDn == null) ? Collections.emptySet() : rolesMappedToUserDn,
                normalizedUserDn,
                file.getFileName(),
                config.type(),
                config.name()
            );
        }
        return roles;
    }

    private class FileListener implements FileChangesListener {
        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(Path file) {
            if (file.equals(DnRoleMapper.this.file)) {
                final Map<String, List<String>> previousDnRoles = dnRoles;
                dnRoles = doPrivileged(
                    (PrivilegedAction<Map<String, List<String>>>) () -> parseFileLenient(file, logger, config.type(), config.name())
                );

                if (previousDnRoles.equals(dnRoles) == false) {
                    logger.info(
                        "role mappings file [{}] changed for realm [{}/{}]. updating mappings...",
                        file.toAbsolutePath(),
                        config.type(),
                        config.name()
                    );
                    clearRealmCachesOnLocalNode();
                }
            }
        }
    }
}
