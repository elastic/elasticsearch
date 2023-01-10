/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Strings;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * This tests the mapping of multiple groups to a role in a file based role-mapping
 */
public class MultiGroupMappingIT extends AbstractAdLdapRealmTestCase {

    @BeforeClass
    public static void setRoleMappingType() {
        final String extraContent = """
            MarvelCharacters:
              - "CN=SHIELD,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com"
              - "CN=Avengers,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com"
              - "CN=Gods,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com"
              - "CN=Philanthropists,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com"
              - "cn=SHIELD,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com"
              - "cn=Avengers,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com"
              - "cn=Gods,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com"
              - "cn=Philanthropists,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com"
            """;
        roleMappings = CollectionUtils.appendToCopy(roleMappings, new RoleMappingEntry(extraContent, null));
    }

    @Override
    protected String configRoles() {
        return Strings.format("""
            %s
            MarvelCharacters:
              cluster: [ NONE ]
              indices:
                - names: 'marvel_comics'
                  privileges: [ all ]
            """, super.configRoles());
    }

    public void testGroupMapping() throws IOException {
        String asgardian = "odin";
        String securityPhilanthropist = realmConfig.loginWithCommonName ? "Bruce Banner" : "hulk";
        String security = realmConfig.loginWithCommonName ? "Phil Coulson" : "phil";
        String securityAsgardianPhilanthropist = "thor";
        String noGroupUser = "jarvis";

        assertAccessAllowed(asgardian, "marvel_comics");
        assertAccessAllowed(securityAsgardianPhilanthropist, "marvel_comics");
        assertAccessAllowed(securityPhilanthropist, "marvel_comics");
        assertAccessAllowed(security, "marvel_comics");
        assertAccessDenied(noGroupUser, "marvel_comics");
    }
}
