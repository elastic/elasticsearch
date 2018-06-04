/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchScope;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;

import java.nio.file.Files;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.login.LoginException;

public class SimpleKdcLdapServerTests extends KerberosTestCase {

    public void testPrincipalCreationAndSearchOnLdap() throws Exception {
        simpleKdcLdapServer.createPrincipal(workDir.resolve("p1p2.keytab"), "p1", "p2");
        assertTrue(Files.exists(workDir.resolve("p1p2.keytab")));
        LDAPConnection ldapConn = AccessController.doPrivileged(new PrivilegedExceptionAction<LDAPConnection>() {

            @Override
            public LDAPConnection run() throws Exception {
                return new LDAPConnection("localhost", simpleKdcLdapServer.getLdapListenPort());
            }
        });
        assertTrue(ldapConn.isConnected());
        SearchResult sr = ldapConn.search("dc=example,dc=com", SearchScope.SUB, "(uid=p1)");
        assertEquals(1, sr.getEntryCount());
    }

    public void testClientServiceMutualAuthentication() throws PrivilegedActionException, GSSException, LoginException {
        final String serviceUserName = randomFrom(serviceUserNames);
        // Client login and init token preparation
        final String clientUserName = randomFrom(clientUserNames);
        final SpnegoClient spnegoClient =
                new SpnegoClient(principalName(clientUserName), new SecureString("pwd".toCharArray()), principalName(serviceUserName));
        final String base64KerbToken = spnegoClient.getBase64TicketForSpnegoHeader();
        assertNotNull(base64KerbToken);
        final KerberosAuthenticationToken kerbAuthnToken = new KerberosAuthenticationToken(base64KerbToken);

        // Service Login
        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        // Handle Authz header which contains base64 token
        final Tuple<String, String> userNameOutToken = new KerberosTicketValidator().validateTicket(principalName(serviceUserName),
                GSSName.NT_USER_NAME, (String) kerbAuthnToken.credentials(), config);
        assertNotNull(userNameOutToken);
        assertEquals(principalName(clientUserName), userNameOutToken.v1());

        // Authenticate service on client side.
        final String outToken = spnegoClient.handleResponse(userNameOutToken.v2());
        assertNull(outToken);
        assertTrue(spnegoClient.isEstablished());

        // Close
        spnegoClient.close();
    }
}
