/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class loads and monitors the file defining the mappings of LDAP Group DNs to internal ES Roles.
 */
public class LdapGroupToRoleMapper extends AbstractComponent {

    public static final String ROLE_MAPPING_DEFAULT_FILE_NAME = ".role_mapping";
    public static final String ROLE_MAPPING_FILE_SETTING = "files.role_mapping";
    public static final String USE_UNMAPPED_GROUPS_AS_ROLES_SETTING = "unmapped_groups_as_roles";

    private final Path file;
    private final Listener listener;
    private final boolean useUnmappedGroupsAsRoles;
    private volatile ImmutableMap<LdapName, Set<String>> groupRoles;

    @Inject
    public LdapGroupToRoleMapper(Settings settings, Environment env, ResourceWatcherService watcherService) {
        this(settings, env, watcherService, Listener.NOOP);
    }

    LdapGroupToRoleMapper(Settings settings, Environment env, ResourceWatcherService watcherService, Listener listener) {
        super(settings);
        useUnmappedGroupsAsRoles = componentSettings.getAsBoolean(USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, false);
        file = resolveFile(componentSettings, env);
        groupRoles = parseFile(file, logger);
        FileWatcher watcher = new FileWatcher(file.getParent().toFile());
        watcher.addListener(new FileListener());
        watcherService.add(watcher);
        this.listener = listener;
    }

    public static ImmutableMap<LdapName, Set<String>> parseFile(Path path, ESLogger logger)  {
        if (!Files.exists(path)) {
            return ImmutableMap.of();
        }

        try (FileInputStream in = new FileInputStream( path.toFile() )){
            Settings settings = ImmutableSettings.builder()
                    .loadFromStream(path.toString(), in)
                    .build();

            Map<LdapName, Set<String>> groupToRoles = new HashMap<>();
            Set<String> roles = settings.names();
            for(String role: roles){
                for(String ldapDN: settings.getAsArray(role)){
                    try {
                        LdapName group = new LdapName(ldapDN);
                        Set<String> groupRoles = groupToRoles.get(group);
                        if (groupRoles == null){
                            groupRoles = new HashSet<>();
                            groupToRoles.put(group, groupRoles);
                        }
                        groupRoles.add(role);
                    } catch (InvalidNameException e) {
                        logger.error("Invalid group DN [{}] found in ldap group to role mappings [{}]. Skipping... ", e, ldapDN, path);
                    }
                }

            }
            return ImmutableMap.copyOf(groupToRoles);

        } catch (IOException e) {
            throw new ElasticsearchException("unable to load ldap role mapper file [" + path.toAbsolutePath() + "]", e);
        }
    }

    public static Path resolveFile(Settings settings, Environment env) {
        String location = settings.get(ROLE_MAPPING_FILE_SETTING);
        if (location == null) {
            return env.configFile().toPath().resolve(ROLE_MAPPING_DEFAULT_FILE_NAME);
        }
        return Paths.get(location);
    }

    /**
     * This will map the groupDN's to ES Roles
     */
    public Set<String> mapRoles(List<String> groupDns) {
        Set<String>roles = new HashSet<>();
        for(String groupDn: groupDns){
            LdapName groupLdapName = LdapUtils.ldapName(groupDn);
            if (this.groupRoles.containsKey(groupLdapName)) {
                roles.addAll(this.groupRoles.get(groupLdapName));
            } else if (useUnmappedGroupsAsRoles) {
                roles.add(getRelativeName(groupLdapName));
            }
        }
        return roles;
    }

    String getRelativeName(LdapName groupLdapName) {
        return (String) groupLdapName.getRdn(groupLdapName.size() - 1).getValue();
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
            if (file.equals(LdapGroupToRoleMapper.this.file.toFile())) {
                groupRoles = parseFile(file.toPath(), logger);
                listener.onRefresh();
            }
        }
    }

    public static interface Listener {

        static final Listener NOOP = new Listener() {
            @Override
            public void onRefresh() {
            }
        };

        void onRefresh();
    }
}
