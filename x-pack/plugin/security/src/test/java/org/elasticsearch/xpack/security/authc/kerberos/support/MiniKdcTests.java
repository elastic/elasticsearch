/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTestCase;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTicketValidator;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivilegedActionException;

import javax.security.auth.login.LoginException;

/**
 * Tests MiniKdc and framework around it.
 */
public class MiniKdcTests extends KerberosTestCase {

    private Settings settings;
    private String serviceUserName;
    private String clientUserName;
    private Settings globalSettings;
    private Path dir;

    @Before
    public void setup() throws Exception {
        dir = createTempDir();
        globalSettings = Settings.builder().put("path.home", dir).build();
        serviceUserName = "HTTP/" + randomAlphaOfLength(10);
        Path ktabPathForService = createPrincipalKeyTab(dir, serviceUserName);
        settings = buildKerberosRealmSettings(ktabPathForService.toString());
        clientUserName = "client-" + randomAlphaOfLength(10);
        createPrincipal(clientUserName, "pwd".toCharArray());
    }

    @After
    public void cleanup() throws IOException {
    }

    public void testClientServiceMutualAuthentication() throws PrivilegedActionException, GSSException, LoginException {
        // Client login and init token preparation
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
