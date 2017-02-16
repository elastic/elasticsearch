/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration.ldap;

import org.elasticsearch.test.junit.annotations.Network;

import java.io.IOException;

/**
 * This tests the mapping of multiple groups to a role
 */
@Network
public class MultiGroupMappingTests extends AbstractAdLdapRealmTestCase {

    @Override
    protected String configRoles() {
        return super.configRoles() +
                "\n" +
                "MarvelCharacters:\n" +
                "  cluster: [ NONE ]\n" +
                "  indices:\n" +
                "    - names: 'marvel_comics'\n" +
                "      privileges: [ all ]\n";
    }

    @Override
    protected String configRoleMappings(RealmConfig realm) {
        return "MarvelCharacters:  \n" +
                "  - \"CN=SHIELD,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\"\n" +
                "  - \"CN=Avengers,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\"\n" +
                "  - \"CN=Gods,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\"\n" +
                "  - \"CN=Philanthropists,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\"\n" +
                "  - \"cn=SHIELD,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com\"\n" +
                "  - \"cn=Avengers,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com\"\n" +
                "  - \"cn=Gods,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com\"\n" +
                "  - \"cn=Philanthropists,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com\"";
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
