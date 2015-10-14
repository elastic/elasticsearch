/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration.ldap;

import org.elasticsearch.test.junit.annotations.Network;

import java.io.IOException;

/**
 * This tests the group to role mappings from LDAP sources provided by the super class - available from super.realmConfig.
 * The super class will provide appropriate group mappings via configGroupMappings()
 */
@Network
public class GroupMappingTests extends AbstractAdLdapRealmTestCase {
    public void testAuthcAuthz() throws IOException {
        String avenger = realmConfig.loginWithCommonName ? "Natasha Romanoff" : "blackwidow";
        assertAccessAllowed(avenger, "avengers");
    }

    public void testGroupMapping() throws IOException {
        String asgardian = "odin";
        String shieldPhilanthropist = realmConfig.loginWithCommonName ? "Bruce Banner" : "hulk";
        String shield = realmConfig.loginWithCommonName ? "Phil Coulson" : "phil";
        String shieldAsgardianPhilanthropist = "thor";
        String noGroupUser = "jarvis";

        assertAccessAllowed(asgardian, ASGARDIAN_INDEX);
        assertAccessAllowed(shieldAsgardianPhilanthropist, ASGARDIAN_INDEX);
        assertAccessDenied(shieldPhilanthropist, ASGARDIAN_INDEX);
        assertAccessDenied(shield, ASGARDIAN_INDEX);
        assertAccessDenied(noGroupUser, ASGARDIAN_INDEX);

        assertAccessAllowed(shieldPhilanthropist, PHILANTHROPISTS_INDEX);
        assertAccessAllowed(shieldAsgardianPhilanthropist, PHILANTHROPISTS_INDEX);
        assertAccessDenied(asgardian, PHILANTHROPISTS_INDEX);
        assertAccessDenied(shield, PHILANTHROPISTS_INDEX);
        assertAccessDenied(noGroupUser, PHILANTHROPISTS_INDEX);

        assertAccessAllowed(shield, SHIELD_INDEX);
        assertAccessAllowed(shieldPhilanthropist, SHIELD_INDEX);
        assertAccessAllowed(shieldAsgardianPhilanthropist, SHIELD_INDEX);
        assertAccessDenied(asgardian, SHIELD_INDEX);
        assertAccessDenied(noGroupUser, SHIELD_INDEX);
    }
}
