/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPInterface;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

public abstract class GroupsResolverTestCase extends ESTestCase {

    LDAPConnection ldapConnection;

    protected abstract String ldapUrl();

    protected abstract String bindDN();

    protected abstract String bindPassword();

    protected abstract String trustPath();

    @Before
    public void setUpLdapConnection() throws Exception {
        Path trustPath = getDataPath(trustPath());
        this.ldapConnection = LdapTestUtils.openConnection(ldapUrl(), bindDN(), bindPassword(), trustPath);
    }

    @After
    public void tearDownLdapConnection() throws Exception {
        if (ldapConnection != null) {
            ldapConnection.close();
        }
    }

    protected static List<String> resolveBlocking(GroupsResolver resolver, LDAPInterface ldapConnection, String dn, TimeValue timeLimit,
                                                  Logger logger, Collection<Attribute> attributes) {
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        resolver.resolve(ldapConnection, dn, timeLimit, logger, attributes, future);
        return future.actionGet();
    }
}
