/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.ietf.jgss.GSSException;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivilegedActionException;
import java.util.Base64;

import javax.security.auth.login.LoginException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class KerberosTicketValidatorTests extends KerberosTestCase {

    private KerberosTicketValidator kerberosTicketValidator = new KerberosTicketValidator();

    public void testKerbTicketGeneratedForDifferentServerFailsValidation() throws Exception {
        createPrincipalKeyTab(workDir, "differentServer");

        // Client login and init token preparation
        final String clientUserName = randomFrom(clientUserNames);
        try (SpnegoClient spnegoClient =
                new SpnegoClient(principalName(clientUserName), new SecureString("pwd".toCharArray()), principalName("differentServer"));) {
            final String base64KerbToken = spnegoClient.getBase64EncodedTokenForSpnegoHeader();
            assertThat(base64KerbToken, is(notNullValue()));

            final Environment env = TestEnvironment.newEnvironment(globalSettings);
            final Path keytabPath = env.configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(settings));
            final GSSException gssException = expectThrows(GSSException.class,
                    () -> kerberosTicketValidator.validateTicket(Base64.getDecoder().decode(base64KerbToken), keytabPath, true));
            assertThat(gssException.getMajor(), equalTo(GSSException.FAILURE));
        }
    }

    public void testInvalidKerbTicketFailsValidation() throws Exception {
        final String base64KerbToken = Base64.getEncoder().encodeToString(randomByteArrayOfLength(5));

        final Environment env = TestEnvironment.newEnvironment(globalSettings);
        final Path keytabPath = env.configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(settings));
        final GSSException gssException = expectThrows(GSSException.class,
                () -> kerberosTicketValidator.validateTicket(Base64.getDecoder().decode(base64KerbToken), keytabPath, true));
        assertThat(gssException.getMajor(), equalTo(GSSException.DEFECTIVE_TOKEN));
    }

    public void testWhenKeyTabWithInvalidContentFailsValidation()
            throws LoginException, GSSException, IOException, PrivilegedActionException {
        // Client login and init token preparation
        final String clientUserName = randomFrom(clientUserNames);
        try (SpnegoClient spnegoClient = new SpnegoClient(principalName(clientUserName), new SecureString("pwd".toCharArray()),
                principalName(randomFrom(serviceUserNames)));) {
            final String base64KerbToken = spnegoClient.getBase64EncodedTokenForSpnegoHeader();
            assertThat(base64KerbToken, is(notNullValue()));

            final Path ktabPath = writeKeyTab(workDir.resolve("invalid.keytab"), "not - a - valid - key - tab");
            settings = buildKerberosRealmSettings(ktabPath.toString());
            final Environment env = TestEnvironment.newEnvironment(globalSettings);
            final Path keytabPath = env.configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(settings));
            final GSSException gssException = expectThrows(GSSException.class,
                    () -> kerberosTicketValidator.validateTicket(Base64.getDecoder().decode(base64KerbToken), keytabPath, true));
            assertThat(gssException.getMajor(), equalTo(GSSException.FAILURE));
        }
    }

    public void testValidKebrerosTicket() throws PrivilegedActionException, GSSException, LoginException {
        // Client login and init token preparation
        final String clientUserName = randomFrom(clientUserNames);
        try (SpnegoClient spnegoClient = new SpnegoClient(principalName(clientUserName), new SecureString("pwd".toCharArray()),
                principalName(randomFrom(serviceUserNames)));) {
            final String base64KerbToken = spnegoClient.getBase64EncodedTokenForSpnegoHeader();
            assertThat(base64KerbToken, is(notNullValue()));

            final Environment env = TestEnvironment.newEnvironment(globalSettings);
            final Path keytabPath = env.configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(settings));
            final Tuple<String, String> userNameOutToken =
                    kerberosTicketValidator.validateTicket(Base64.getDecoder().decode(base64KerbToken), keytabPath, true);
            assertThat(userNameOutToken, is(notNullValue()));
            assertThat(userNameOutToken.v1(), equalTo(principalName(clientUserName)));
            assertThat(userNameOutToken.v2(), is(notNullValue()));

            final String outToken = spnegoClient.handleResponse(userNameOutToken.v2());
            assertThat(outToken, is(nullValue()));
            assertThat(spnegoClient.isEstablished(), is(true));
        }
    }

}
