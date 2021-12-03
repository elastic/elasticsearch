/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchScope;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.ietf.jgss.GSSException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedActionException;
import java.text.ParseException;
import java.util.Base64;

import javax.security.auth.login.LoginException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SimpleKdcLdapServerTests extends KerberosTestCase {

    public void testPrincipalCreationAndSearchOnLdap() throws Exception {
        simpleKdcLdapServer.createPrincipal(workDir.resolve("p1p2.keytab"), "p1", "p2");
        assertTrue(Files.exists(workDir.resolve("p1p2.keytab")));
        try (
            LDAPConnection ldapConn = LdapUtils.privilegedConnect(
                () -> new LDAPConnection("localhost", simpleKdcLdapServer.getLdapListenPort())
            );
        ) {
            assertThat(ldapConn.isConnected(), is(true));
            SearchResult sr = ldapConn.search("dc=example,dc=com", SearchScope.SUB, "(krb5PrincipalName=p1@EXAMPLE.COM)");
            assertThat(sr.getSearchEntries(), hasSize(1));
            assertThat(sr.getSearchEntries().get(0).getDN(), equalTo("uid=p1,dc=example,dc=com"));
        }
    }

    public void testClientServiceMutualAuthentication() throws PrivilegedActionException, GSSException, LoginException, ParseException {
        final String serviceUserName = randomFrom(serviceUserNames);
        // Client login and init token preparation
        final String clientUserName = randomFrom(clientUserNames);
        try (
            SpnegoClient spnegoClient = new SpnegoClient(
                principalName(clientUserName),
                new SecureString("spnego-test-password".toCharArray()),
                principalName(serviceUserName),
                randomFrom(KerberosTicketValidator.SUPPORTED_OIDS)
            );
        ) {
            final String base64KerbToken = spnegoClient.getBase64EncodedTokenForSpnegoHeader();
            assertThat(base64KerbToken, is(notNullValue()));
            final KerberosAuthenticationToken kerbAuthnToken = new KerberosAuthenticationToken(Base64.getDecoder().decode(base64KerbToken));

            // Service Login
            final Environment env = TestEnvironment.newEnvironment(globalSettings);
            final Path keytabPath = getKeytabPath(env);
            // Handle Authz header which contains base64 token
            final PlainActionFuture<Tuple<String, String>> future = new PlainActionFuture<>();
            new KerberosTicketValidator().validateTicket((byte[]) kerbAuthnToken.credentials(), keytabPath, true, future);
            assertThat(future.actionGet(), is(notNullValue()));
            assertThat(future.actionGet().v1(), equalTo(principalName(clientUserName)));

            // Authenticate service on client side.
            final String outToken = spnegoClient.handleResponse(future.actionGet().v2());
            assertThat(outToken, is(nullValue()));
            assertThat(spnegoClient.isEstablished(), is(true));
        }
    }
}
