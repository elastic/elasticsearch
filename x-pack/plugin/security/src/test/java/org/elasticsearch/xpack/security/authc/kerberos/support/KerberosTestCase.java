/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosRealm;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * Base Test class for Kerberos
 */
public class KerberosTestCase extends ESTestCase {

    protected MiniKdc miniKdc;
    protected Path miniKdcWorkDir = null;
    protected InMemoryDirectoryServer ldapServer;

    @Before
    public void startMiniKdc() throws Exception {
        createLdapService();
        createMiniKdc();
        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
                miniKdc.start();
                return null;
            }
        });
    }

    @SuppressForbidden(reason = "Test dependency MiniKdc requires java.io.File and needs access to private field")
    private void createMiniKdc() throws Exception {
        miniKdcWorkDir = createTempDir();
        String backendConf = "kdc_identity_backend = org.apache.kerby.kerberos.kdc.identitybackend.LdapIdentityBackend\n"
                + "host=127.0.0.1\n" + "port=" + ldapServer.getListenPort() + "\n" + "admin_dn=uid=admin,ou=system,dc=example,dc=com\n"
                + "admin_pw=secret\n" + "base_dn=dc=example,dc=com";
        Files.write(miniKdcWorkDir.resolve("backend.conf"), backendConf.getBytes(StandardCharsets.UTF_8));
        assertTrue(Files.exists(miniKdcWorkDir.resolve("backend.conf")));
        miniKdc = new MiniKdc(MiniKdc.createConf(), miniKdcWorkDir.toFile());
    }

    private void createLdapService() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
        config.setSchema(null);
        ldapServer = new InMemoryDirectoryServer(config);
        ldapServer.importFromLDIF(true, getDataPath("/minikdc.ldiff").toString());
        // Must have privileged access because underlying server will accept socket
        // connections
        AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
            ldapServer.startListening();
            return null;
        });
    }

    @After
    public void tearDownMiniKdc() throws IOException, PrivilegedActionException {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
                miniKdc.stop();
                return null;
            }
        });

        ldapServer.shutDown(true);
    }

    @SuppressForbidden(reason = "Test dependency MiniKdc requires java.io.File")
    protected Path createPrincipalKeyTab(final Path dir, final String principalName) throws Exception {
        final Path ktabPath = dir.resolve(principalName.substring(principalName.indexOf("/") + 1) + ".keytab");
        miniKdc.createPrincipal(ktabPath.toFile(), principalName);
        assertTrue(Files.exists(ktabPath));
        return ktabPath;
    }

    protected void createPrincipal(final String principalName, final char[] password) throws Exception {
        miniKdc.createPrincipal(principalName, new String(password));
    }

    protected String principalName(final String user) {
        return user + "@" + miniKdc.getRealm();
    }

    /**
     * Invokes Subject.doAs inside a doPrivileged block
     * 
     * @param subject
     *            {@link Subject}
     * @param action
     *            {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @return
     * @throws PrivilegedActionException
     */
    public static <T> T doAsWrapper(Subject subject, PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAs(subject, action));
    }

    // Not thread safe
    public static class SpnegoClient {
        public static final String CRED_CONF_NAME = "PasswordConf";
        private static final String SUN_KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";
        private final GSSManager gssManager = GSSManager.getInstance();
        private final LoginContext loginContext;
        private final GSSContext gssContext;
        private boolean isEstablished;

        public SpnegoClient(final String userPrincipalName, final SecureString password, final String servicePrincipalName,
                final Settings settings) throws PrivilegedActionException, GSSException {
            final GSSName gssUserPrincipalName = gssManager.createName(userPrincipalName, GSSName.NT_USER_NAME);
            final GSSName gssServicePrincipalName = gssManager.createName(servicePrincipalName, GSSName.NT_USER_NAME);
            loginContext = AccessController.doPrivileged(
                    (PrivilegedExceptionAction<LoginContext>) () -> loginUsingPassword(userPrincipalName, password, settings));
            final GSSCredential userCreds = doAsWrapper(loginContext.getSubject(),
                    (PrivilegedExceptionAction<GSSCredential>) () -> gssManager.createCredential(gssUserPrincipalName,
                            GSSCredential.DEFAULT_LIFETIME, KerberosRealm.SPNEGO_OID, GSSCredential.INITIATE_ONLY));
            gssContext = gssManager.createContext(gssServicePrincipalName.canonicalize(KerberosRealm.SPNEGO_OID), KerberosRealm.SPNEGO_OID,
                    userCreds, GSSCredential.DEFAULT_LIFETIME);
            gssContext.requestMutualAuth(true);
            isEstablished = gssContext.isEstablished();
        }

        public String getBase64TicketForSpnegoHeader() throws PrivilegedActionException {
            final byte[] outToken = doAsWrapper(loginContext.getSubject(),
                    (PrivilegedExceptionAction<byte[]>) () -> gssContext.initSecContext(new byte[0], 0, 0));
            return Base64.getEncoder().encodeToString(outToken);
        }

        public String handleResponse(final String base64Token) throws PrivilegedActionException {
            if (isEstablished) {
                throw new IllegalStateException("GSS Context has already been established");
            }
            final byte[] token = Base64.getDecoder().decode(base64Token);
            final byte[] outToken = doAsWrapper(loginContext.getSubject(),
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
         * @param principal
         *            Principal name
         * @param password
         *            {@link SecureString}
         * @param settings
         *            {@link Settings}
         * @return authenticated {@link LoginContext} instance. Note: This needs to be
         *         closed {@link LoginContext#logout()} after usage.
         * @throws LoginException
         */
        private static LoginContext loginUsingPassword(final String principal, final SecureString password, final Settings settings)
                throws LoginException {
            final Set<Principal> principals = new HashSet<>();
            principals.add(new KerberosPrincipal(principal));

            final Subject subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());

            final Configuration conf = new PasswordJaasConf(principal, settings);
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
            private final Settings settings;

            PasswordJaasConf(final String principal, final Settings settings) {
                this.principal = principal;
                this.settings = settings;
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
                options.put("debug", String.valueOf(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(settings).toString()));

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
}
