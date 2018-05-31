/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.io.IOException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * This class is used as a Spnego client and handles spnego interactions.<br>
 * Not thread safe
 */
public class SpnegoClient {
    public static final Oid SPNEGO_OID = getSpnegoOid();

    private static Oid getSpnegoOid() {
        Oid oid = null;
        try {
            oid = new Oid("1.3.6.1.5.5.2");
        } catch (GSSException gsse) {
            ExceptionsHelper.convertToRuntime(gsse);
        }
        return oid;
    }

    public static final String CRED_CONF_NAME = "PasswordConf";
    private static final String SUN_KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";
    private final GSSManager gssManager = GSSManager.getInstance();
    private final LoginContext loginContext;
    private final GSSContext gssContext;
    private boolean isEstablished;

    public SpnegoClient(final String userPrincipalName, final SecureString password, final String servicePrincipalName)
            throws PrivilegedActionException, GSSException {
        final GSSName gssUserPrincipalName = gssManager.createName(userPrincipalName, GSSName.NT_USER_NAME);
        final GSSName gssServicePrincipalName = gssManager.createName(servicePrincipalName, GSSName.NT_USER_NAME);
        loginContext = AccessController
                .doPrivileged((PrivilegedExceptionAction<LoginContext>) () -> loginUsingPassword(userPrincipalName, password));
        final GSSCredential userCreds =
                KerberosTestCase.doAsWrapper(loginContext.getSubject(), (PrivilegedExceptionAction<GSSCredential>) () -> gssManager
                        .createCredential(gssUserPrincipalName, GSSCredential.DEFAULT_LIFETIME, SPNEGO_OID, GSSCredential.INITIATE_ONLY));
        gssContext = gssManager.createContext(gssServicePrincipalName.canonicalize(SPNEGO_OID), SPNEGO_OID, userCreds,
                GSSCredential.DEFAULT_LIFETIME);
        gssContext.requestMutualAuth(true);
        isEstablished = gssContext.isEstablished();
    }

    public String getBase64TicketForSpnegoHeader() throws PrivilegedActionException {
        final byte[] outToken = KerberosTestCase.doAsWrapper(loginContext.getSubject(),
                (PrivilegedExceptionAction<byte[]>) () -> gssContext.initSecContext(new byte[0], 0, 0));
        return Base64.getEncoder().encodeToString(outToken);
    }

    public String handleResponse(final String base64Token) throws PrivilegedActionException {
        if (isEstablished) {
            throw new IllegalStateException("GSS Context has already been established");
        }
        final byte[] token = Base64.getDecoder().decode(base64Token);
        final byte[] outToken = KerberosTestCase.doAsWrapper(loginContext.getSubject(),
                (PrivilegedExceptionAction<byte[]>) () -> gssContext.initSecContext(token, 0, token.length));
        isEstablished = gssContext.isEstablished();
        if (outToken == null || outToken.length == 0) {
            return null;
        }
        return Base64.getEncoder().encodeToString(outToken);
    }

    public void close() throws LoginException, GSSException, PrivilegedActionException {
        if (loginContext != null) {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                loginContext.logout();
                return null;
            });
        }
        if (gssContext != null) {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                gssContext.dispose();
                return null;
            });
        }
    }

    public boolean isEstablished() {
        return isEstablished;
    }

    /**
     * Performs authentication using provided principal name and password
     *
     * @param principal Principal name
     * @param password {@link SecureString}
     * @param settings {@link Settings}
     * @return authenticated {@link LoginContext} instance. Note: This needs to be
     *         closed {@link LoginContext#logout()} after usage.
     * @throws LoginException
     */
    private static LoginContext loginUsingPassword(final String principal, final SecureString password) throws LoginException {
        final Set<Principal> principals = new HashSet<>();
        principals.add(new KerberosPrincipal(principal));

        final Subject subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());

        final Configuration conf = new PasswordJaasConf(principal);
        final CallbackHandler callback = new KrbCallbackHandler(principal, password);
        final LoginContext loginContext = new LoginContext(CRED_CONF_NAME, subject, callback, conf);
        loginContext.login();
        return loginContext;
    }

    /**
     * Instead of jaas.conf, this requires refresh of {@link Configuration}.
     */
    static class PasswordJaasConf extends Configuration {
        private final String principal;

        PasswordJaasConf(final String principal) {
            this.principal = principal;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(final String name) {
            final Map<String, String> options = new HashMap<>();
            options.put("principal", principal);
            options.put("storeKey", Boolean.TRUE.toString());
            options.put("useTicketCache", Boolean.FALSE.toString());
            options.put("useKeyTab", Boolean.FALSE.toString());
            options.put("renewTGT", Boolean.FALSE.toString());
            options.put("refreshKrb5Config", Boolean.TRUE.toString());
            options.put("isInitiator", Boolean.TRUE.toString());
            options.put("debug", Boolean.TRUE.toString());

            return new AppConfigurationEntry[] {
                    new AppConfigurationEntry(SUN_KRB5_LOGIN_MODULE, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
        }
    }

    static class KrbCallbackHandler implements CallbackHandler {
        private final String principal;
        private final SecureString password;

        KrbCallbackHandler(final String principal, final SecureString password) {
            this.principal = principal;
            this.password = password;
        }

        public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callback;
                    if (pc.getPrompt().contains(principal)) {
                        pc.setPassword(password.getChars());
                        break;
                    }
                }
            }
        }
    }
}
