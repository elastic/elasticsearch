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
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import javax.security.auth.login.LoginException;

public class KerberosTicketValidatorTests extends KerberosTestCase {

    private Settings settings;
    private List<String> serviceUserNames = new ArrayList<>();
    private String clientUserName;
    private Settings globalSettings;
    private Path dir;
    private KerberosTicketValidator kerberosTicketValidator = new KerberosTicketValidator();
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        dir = createTempDir();
        globalSettings = Settings.builder().put("path.home", dir).build();
        serviceUserNames.clear();
        serviceUserNames.add("HTTP/" + randomAlphaOfLength(10));
        serviceUserNames.add("HTTP/" + randomAlphaOfLength(10));
        Path ktabPathForService = createPrincipalKeyTab(dir, serviceUserNames.toArray(new String[0]));
        settings = buildKerberosRealmSettings(ktabPathForService.toString());
        clientUserName = "client-" + randomAlphaOfLength(5);
        createPrincipal(clientUserName, "pwd".toCharArray());
    }

    public void testKerbTicketGeneratedForDifferentServerFailsValidation() throws Exception {
        createPrincipalKeyTab(dir, "differentServer");

        // Client login and init token preparation
        final SpnegoClient spnegoClient =
                new SpnegoClient(principalName(clientUserName), new SecureString("pwd".toCharArray()), principalName("differentServer"));
        final String base64KerbToken = spnegoClient.getBase64TicketForSpnegoHeader();
        assertNotNull(base64KerbToken);

        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        thrown.expect(new GSSExceptionMatcher(GSSException.FAILURE));
        kerberosTicketValidator.validateTicket("*", GSSName.NT_USER_NAME, base64KerbToken, config);
    }

    public void testInvalidKerbTicketFailsValidation() throws Exception {
        final String base64KerbToken = Base64.getEncoder().encodeToString(randomByteArrayOfLength(5));

        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        thrown.expect(new GSSExceptionMatcher(GSSException.DEFECTIVE_TOKEN));
        kerberosTicketValidator.validateTicket("*", GSSName.NT_USER_NAME, base64KerbToken, config);
    }

    public void testWhenKeyTabDoesNotExistFailsValidation() throws LoginException, GSSException {
        final String base64KerbToken = Base64.getEncoder().encodeToString(randomByteArrayOfLength(5));
        settings = buildKerberosRealmSettings("random-non-existing.keytab".toString());
        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(Matchers.equalTo("configured service key tab file does not exist for "
                + RealmSettings.getFullSettingKey(config, KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH)));
        kerberosTicketValidator.validateTicket("*", GSSName.NT_USER_NAME, base64KerbToken, config);
    }

    public void testWhenKeyTabWithInvalidContentFailsValidation()
            throws LoginException, GSSException, IOException, PrivilegedActionException {
        // Client login and init token preparation
        final SpnegoClient spnegoClient = new SpnegoClient(principalName(clientUserName), new SecureString("pwd".toCharArray()),
                principalName(randomFrom(serviceUserNames)));
        final String base64KerbToken = spnegoClient.getBase64TicketForSpnegoHeader();
        assertNotNull(base64KerbToken);

        final Path ktabPath = writeKeyTab(dir, "invalid.keytab", "not - a - valid - key - tab");
        settings = buildKerberosRealmSettings(ktabPath.toString());
        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        thrown.expect(new GSSExceptionMatcher(GSSException.FAILURE));
        kerberosTicketValidator.validateTicket("*", GSSName.NT_USER_NAME, base64KerbToken, config);
    }

    public void testValidKebrerosTicket() throws PrivilegedActionException, GSSException, LoginException {
        // Client login and init token preparation
        final SpnegoClient spnegoClient = new SpnegoClient(principalName(clientUserName), new SecureString("pwd".toCharArray()),
                principalName(randomFrom(serviceUserNames)));
        final String base64KerbToken = spnegoClient.getBase64TicketForSpnegoHeader();
        assertNotNull(base64KerbToken);

        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        final Tuple<String, String> userNameOutToken =
                kerberosTicketValidator.validateTicket("*", GSSName.NT_HOSTBASED_SERVICE, base64KerbToken, config);
        assertNotNull(userNameOutToken);
        assertEquals(principalName(clientUserName), userNameOutToken.v1());
        assertNotNull(userNameOutToken.v2());

        spnegoClient.handleResponse(userNameOutToken.v2());
        assertTrue(spnegoClient.isEstablished());
        spnegoClient.close();
    }

    class GSSExceptionMatcher extends BaseMatcher<GSSException> {
        private int expectedErrorCode;

        GSSExceptionMatcher(int expectedErrorCode) {
            this.expectedErrorCode = expectedErrorCode;
        }

        @Override
        public boolean matches(Object item) {
            if (item instanceof GSSException) {
                GSSException gssException = (GSSException) item;
                if (gssException.getMajor() == expectedErrorCode) {
                    if (gssException.getMajorString().equals(new GSSException(expectedErrorCode).getMajorString())) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
        }
    }
}
