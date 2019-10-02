/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

/**
 * This tests the group to role mappings from LDAP sources provided by the super class - available from super.realmConfig.
 * The super class will provide appropriate group mappings via configGroupMappings()
 */
@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/35738")
public class GroupMappingIT extends AbstractAdLdapRealmTestCase {

    public void testAuthcAuthz() throws IOException {
        String avenger = realmConfig.loginWithCommonName ? "Natasha Romanoff" : "blackwidow";
        assertAccessAllowed(avenger, "avengers");
    }

    public void testGroupMapping() throws IOException {
        String asgardian = "odin";
        String securityPhilanthropist = realmConfig.loginWithCommonName ? "Bruce Banner" : "hulk";
        String securityMappedUser = realmConfig.loginWithCommonName ? "Phil Coulson" : "phil";
        String securityAsgardianPhilanthropist = "thor";
        String noGroupUser = "jarvis";

        assertAccessAllowed(asgardian, ASGARDIAN_INDEX);
        assertAccessAllowed(securityAsgardianPhilanthropist, ASGARDIAN_INDEX);
        assertAccessDenied(securityPhilanthropist, ASGARDIAN_INDEX);
        assertAccessDenied(securityMappedUser, ASGARDIAN_INDEX);
        assertAccessDenied(noGroupUser, ASGARDIAN_INDEX);

        assertAccessAllowed(securityPhilanthropist, PHILANTHROPISTS_INDEX);
        assertAccessAllowed(securityAsgardianPhilanthropist, PHILANTHROPISTS_INDEX);
        assertAccessDenied(asgardian, PHILANTHROPISTS_INDEX);
        assertAccessDenied(securityMappedUser, PHILANTHROPISTS_INDEX);
        assertAccessDenied(noGroupUser, PHILANTHROPISTS_INDEX);

        assertAccessAllowed(securityMappedUser, SECURITY_INDEX);
        assertAccessAllowed(securityPhilanthropist, SECURITY_INDEX);
        assertAccessAllowed(securityAsgardianPhilanthropist, SECURITY_INDEX);
        assertAccessDenied(asgardian, SECURITY_INDEX);
        assertAccessDenied(noGroupUser, SECURITY_INDEX);
    }
}
