/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.ldap.AbstractGroupToRoleMapper;
import org.elasticsearch.shield.authc.support.ldap.AbstractGroupToRoleMapperTests;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;

public class LdapGroupToRoleMapperTests extends AbstractGroupToRoleMapperTests {

    private final String[] starkGroupDns = new String[] {
            //groups can be named by different attributes, depending on the directory,
            //we don't care what it is named by
            "cn=shield,ou=marvel,o=superheros",
            "cn=avengers,ou=marvel,o=superheros",
            "group=genius, dc=mit, dc=edu",
            "groupName = billionaire , ou = acme",
            "gid = playboy , dc = example , dc = com",
            "groupid=philanthropist,ou=groups,dc=unitedway,dc=org"
    };

    @Override
    protected AbstractGroupToRoleMapper createMapper(Path file, ResourceWatcherService watcherService) {
        Settings ldapSettings = ImmutableSettings.builder()
                .put("files.role_mapping", file.toAbsolutePath())
                .build();
        RealmConfig config = new RealmConfig("ldap-group-mapper-test", ldapSettings, settings, env);
        return new LdapGroupToRoleMapper(config, watcherService);
    }

    @Test
    public void testYaml() throws IOException {
        File file = this.getResource("../support/ldap/role_mapping.yml");
        Settings ldapSettings = ImmutableSettings.settingsBuilder()
                .put(LdapGroupToRoleMapper.ROLE_MAPPING_FILE_SETTING, file.getCanonicalPath())
                .build();
        RealmConfig config = new RealmConfig("ldap1", ldapSettings);

        AbstractGroupToRoleMapper mapper = new LdapGroupToRoleMapper(config, new ResourceWatcherService(settings, threadPool));

        Set<String> roles = mapper.mapRoles( Arrays.asList(starkGroupDns) );

        //verify
        assertThat(roles, hasItems("shield", "avenger"));
    }

    @Test
    public void testRelativeDN() {
        Settings ldapSettings = ImmutableSettings.builder()
                .put(AbstractGroupToRoleMapper.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, true)
                .build();
        RealmConfig config = new RealmConfig("ldap1", ldapSettings);

        AbstractGroupToRoleMapper mapper = new LdapGroupToRoleMapper(config, new ResourceWatcherService(settings, threadPool));

        Set<String> roles = mapper.mapRoles(Arrays.asList(starkGroupDns));
        assertThat(roles, hasItems("genius", "billionaire", "playboy", "philanthropist", "shield", "avengers"));
    }

}
