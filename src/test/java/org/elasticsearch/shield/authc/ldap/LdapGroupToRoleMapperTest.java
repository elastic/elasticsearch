/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.support.ldap.AbstractGroupToRoleMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;

public class LdapGroupToRoleMapperTest extends ElasticsearchTestCase {

    private final String tonyStarkDN = "cn=tstark,ou=marvel,o=superheros";
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
    private final String roleShield = "shield";
    private final String roleAvenger = "avenger";
    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = new ThreadPool("test");
    }

    @After
    public void shutdown() {
        threadPool.shutdownNow();
    }


    @Test
    public void testYaml() throws IOException {
        File file = this.getResource("../support/ldap/role_mapping.yml");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put(LdapGroupToRoleMapper.ROLE_MAPPING_FILE_SETTING, file.getCanonicalPath())
                .build();

        AbstractGroupToRoleMapper mapper = new LdapGroupToRoleMapper(settings,
                "ldap1",
                new Environment(settings),
                new ResourceWatcherService(settings, threadPool));

        Set<String> roles = mapper.mapRoles( Arrays.asList(starkGroupDns) );

        //verify
        assertThat(roles, hasItems(roleShield, roleAvenger));
    }

    @Test
    public void testRelativeDN() {
        Settings settings = ImmutableSettings.builder()
                .put(AbstractGroupToRoleMapper.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, true)
                .build();

        AbstractGroupToRoleMapper mapper = new LdapGroupToRoleMapper(settings,
                "ldap1",
                new Environment(settings),
                new ResourceWatcherService(settings, threadPool));

        Set<String> roles = mapper.mapRoles(Arrays.asList(starkGroupDns));
        assertThat(roles, hasItems("genius", "billionaire", "playboy", "philanthropist", "shield", "avengers"));
    }
}
