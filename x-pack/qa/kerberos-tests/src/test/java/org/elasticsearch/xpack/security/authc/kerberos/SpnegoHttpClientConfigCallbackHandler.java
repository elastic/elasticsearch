/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.common.settings.SecureString;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
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
 * This class implements {@link HttpClientConfigCallback} which allows for
 * customization of {@link HttpAsyncClientBuilder}.
 * <p>
 * Based on the configuration, configures {@link HttpAsyncClientBuilder} to
 * support spengo auth scheme.<br>
 * It uses configured credentials either password or keytab for authentication.
 */
public class SpnegoHttpClientConfigCallbackHandler implements HttpClientConfigCallback {
    private static final String SUN_KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";
    private static final String CRED_CONF_NAME = "ESClientLoginConf";
    private static final Oid SPNEGO_OID = getSpnegoOid();

    private static Oid getSpnegoOid() {
        Oid oid = null;
        try {
            oid = new Oid("1.3.6.1.5.5.2");
        } catch (GSSException gsse) {
            throw ExceptionsHelper.convertToRuntime(gsse);
        }
        return oid;
    }

    private final String userPrincipalName;
    private final SecureString password;
    private final String keytabPath;
    private final boolean enableDebugLogs;
    private LoginContext loginContext;
    private GSSContext gssContext;

    /**
     * Constructs {@link SpnegoHttpClientConfigCallbackHandler} with given
     * principalName and password.
     *
     * @param userPrincipalName user principal name
     * @param password password for user
     * @param enableDebugLogs if {@code true} enables kerberos debug logs
     */
    public SpnegoHttpClientConfigCallbackHandler(
        final String userPrincipalName,
        final SecureString password,
        final boolean enableDebugLogs
    ) {
        this.userPrincipalName = userPrincipalName;
        this.password = password;
        this.keytabPath = null;
        this.enableDebugLogs = enableDebugLogs;
    }

    /**
     * Constructs {@link SpnegoHttpClientConfigCallbackHandler} with given
     * principalName and keytab.
     *
     * @param userPrincipalName User principal name
     * @param keytabPath path to keytab file for user
     * @param enableDebugLogs if {@code true} enables kerberos debug logs
     */
    public SpnegoHttpClientConfigCallbackHandler(final String userPrincipalName, final String keytabPath, final boolean enableDebugLogs) {
        this.userPrincipalName = userPrincipalName;
        this.keytabPath = keytabPath;
        this.password = null;
        this.enableDebugLogs = enableDebugLogs;
    }

    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        setupSpnegoAuthSchemeSupport(httpClientBuilder);
        return httpClientBuilder;
    }

    private void setupSpnegoAuthSchemeSupport(HttpAsyncClientBuilder httpClientBuilder) {
        final Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create()
            .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory())
            .build();

        final GSSManager gssManager = GSSManager.getInstance();
        try {
            final GSSName gssUserPrincipalName = gssManager.createName(userPrincipalName, GSSName.NT_USER_NAME);
            login();
            final AccessControlContext acc = AccessController.getContext();
            final GSSCredential credential = doAsPrivilegedWrapper(
                loginContext.getSubject(),
                (PrivilegedExceptionAction<GSSCredential>) () -> gssManager.createCredential(
                    gssUserPrincipalName,
                    GSSCredential.DEFAULT_LIFETIME,
                    SPNEGO_OID,
                    GSSCredential.INITIATE_ONLY
                ),
                acc
            );

            final KerberosCredentialsProvider credentialsProvider = new KerberosCredentialsProvider();
            credentialsProvider.setCredentials(
                new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.SPNEGO),
                new KerberosCredentials(credential)
            );
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        } catch (GSSException e) {
            throw new RuntimeException(e);
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e.getCause());
        }
        httpClientBuilder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
    }

    /**
     * If logged in {@link LoginContext} is not available, it attempts login and
     * returns {@link LoginContext}
     *
     * @return {@link LoginContext}
     * @throws PrivilegedActionException if the login triggers a checked exception
     */
    public synchronized LoginContext login() throws PrivilegedActionException {
        if (this.loginContext == null) {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                final Subject subject = new Subject(
                    false,
                    Collections.singleton(new KerberosPrincipal(userPrincipalName)),
                    Collections.emptySet(),
                    Collections.emptySet()
                );
                Configuration conf = null;
                final CallbackHandler callback;
                if (password != null) {
                    conf = new PasswordJaasConf(userPrincipalName, enableDebugLogs);
                    callback = new KrbCallbackHandler(userPrincipalName, password);
                } else {
                    conf = new KeytabJaasConf(userPrincipalName, keytabPath, enableDebugLogs);
                    callback = null;
                }
                loginContext = new LoginContext(CRED_CONF_NAME, subject, callback, conf);
                loginContext.login();
                return null;
            });
        }
        return loginContext;
    }

    /**
     * Privileged Wrapper that invokes action with Subject.doAs to perform work as
     * given subject.
     *
     * @param subject {@link Subject} to be used for this work
     * @param action {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @param acc the {@link AccessControlContext} to be tied to the specified
     *            subject and action see
     *            {@link Subject#doAsPrivileged(Subject, PrivilegedExceptionAction, AccessControlContext)}
     * @return the value returned by the PrivilegedExceptionAction's run method
     * @throws PrivilegedActionException if the specified action's run method threw a checked exception
     */
    static <T> T doAsPrivilegedWrapper(final Subject subject, final PrivilegedExceptionAction<T> action, final AccessControlContext acc)
        throws PrivilegedActionException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAsPrivileged(subject, action, acc));
        } catch (PrivilegedActionException pae) {
            if (pae.getCause()instanceof PrivilegedActionException privilegedActionException) {
                throw privilegedActionException;
            }
            throw pae;
        }
    }

    /**
     * This class matches {@link AuthScope} and based on that returns
     * {@link Credentials}. Only supports {@link AuthSchemes#SPNEGO} in
     * {@link AuthScope#getScheme()}
     */
    private static class KerberosCredentialsProvider implements CredentialsProvider {
        private AuthScope authScope;
        private Credentials credentials;

        @Override
        public void setCredentials(AuthScope authscope, Credentials credentials) {
            if (authscope.getScheme().regionMatches(true, 0, AuthSchemes.SPNEGO, 0, AuthSchemes.SPNEGO.length()) == false) {
                throw new IllegalArgumentException("Only " + AuthSchemes.SPNEGO + " auth scheme is supported in AuthScope");
            }
            this.authScope = authscope;
            this.credentials = credentials;
        }

        @Override
        public Credentials getCredentials(AuthScope authscope) {
            assert this.authScope != null && authscope != null;
            return authscope.match(this.authScope) > -1 ? this.credentials : null;
        }

        @Override
        public void clear() {
            this.authScope = null;
            this.credentials = null;
        }
    }

    /**
     * Jaas call back handler to provide credentials.
     */
    private static class KrbCallbackHandler implements CallbackHandler {
        private final String principal;
        private final SecureString password;

        KrbCallbackHandler(final String principal, final SecureString password) {
            this.principal = principal;
            this.password = password;
        }

        public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback pc) {
                    if (pc.getPrompt().contains(principal)) {
                        pc.setPassword(password.getChars());
                        break;
                    }
                }
            }
        }
    }

    /**
     * Usually we would have a JAAS configuration file for login configuration.
     * Instead of an additional file setting as we do not want the options to be
     * customizable we are constructing it in memory.
     * <p>
     * As we are using this instead of jaas.conf, this requires refresh of
     * {@link Configuration} and reqires appropriate security permissions to do so.
     */
    private static class PasswordJaasConf extends AbstractJaasConf {

        PasswordJaasConf(final String userPrincipalName, final boolean enableDebugLogs) {
            super(userPrincipalName, enableDebugLogs);
        }

        public void addOptions(final Map<String, String> options) {
            options.put("useTicketCache", Boolean.FALSE.toString());
            options.put("useKeyTab", Boolean.FALSE.toString());
        }
    }

    /**
     * Usually we would have a JAAS configuration file for login configuration. As
     * we have static configuration except debug flag, we are constructing in
     * memory. This avoids additional configuration required from the user.
     * <p>
     * As we are using this instead of jaas.conf, this requires refresh of
     * {@link Configuration} and requires appropriate security permissions to do so.
     */
    private static class KeytabJaasConf extends AbstractJaasConf {
        private final String keytabFilePath;

        KeytabJaasConf(final String userPrincipalName, final String keytabFilePath, final boolean enableDebugLogs) {
            super(userPrincipalName, enableDebugLogs);
            this.keytabFilePath = keytabFilePath;
        }

        public void addOptions(final Map<String, String> options) {
            options.put("useKeyTab", Boolean.TRUE.toString());
            options.put("keyTab", keytabFilePath);
            options.put("doNotPrompt", Boolean.TRUE.toString());
        }

    }

    private abstract static class AbstractJaasConf extends Configuration {
        private final String userPrincipalName;
        private final boolean enableDebugLogs;

        AbstractJaasConf(final String userPrincipalName, final boolean enableDebugLogs) {
            this.userPrincipalName = userPrincipalName;
            this.enableDebugLogs = enableDebugLogs;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(final String name) {
            final Map<String, String> options = new HashMap<>();
            options.put("principal", userPrincipalName);
            options.put("isInitiator", Boolean.TRUE.toString());
            options.put("storeKey", Boolean.TRUE.toString());
            options.put("debug", Boolean.toString(enableDebugLogs));
            addOptions(options);
            return new AppConfigurationEntry[] {
                new AppConfigurationEntry(
                    SUN_KRB5_LOGIN_MODULE,
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    Collections.unmodifiableMap(options)
                ) };
        }

        abstract void addOptions(Map<String, String> options);
    }

    /**
     * Initiates GSS context establishment and returns the
     * base64 encoded token to be sent to server.
     *
     * @return Base64 encoded token
     * @throws PrivilegedActionException when privileged action threw exception
     * @throws GSSException when GSS context creation fails
     */
    String getBase64EncodedTokenForSpnegoHeader(final String serviceHost) throws PrivilegedActionException, GSSException {
        final GSSManager gssManager = GSSManager.getInstance();
        final GSSName gssServicePrincipalName = AccessController.doPrivileged(
            (PrivilegedExceptionAction<GSSName>) () -> gssManager.createName("HTTP/" + serviceHost, null)
        );
        final GSSName gssUserPrincipalName = gssManager.createName(userPrincipalName, GSSName.NT_USER_NAME);
        loginContext = AccessController.doPrivileged(
            (PrivilegedExceptionAction<LoginContext>) () -> loginUsingPassword(userPrincipalName, password)
        );
        final GSSCredential userCreds = doAsWrapper(
            loginContext.getSubject(),
            (PrivilegedExceptionAction<GSSCredential>) () -> gssManager.createCredential(
                gssUserPrincipalName,
                GSSCredential.DEFAULT_LIFETIME,
                SPNEGO_OID,
                GSSCredential.INITIATE_ONLY
            )
        );
        gssContext = gssManager.createContext(gssServicePrincipalName, SPNEGO_OID, userCreds, GSSCredential.DEFAULT_LIFETIME);
        gssContext.requestMutualAuth(true);

        final byte[] outToken = doAsWrapper(
            loginContext.getSubject(),
            (PrivilegedExceptionAction<byte[]>) () -> gssContext.initSecContext(new byte[0], 0, 0)
        );
        return Base64.getEncoder().encodeToString(outToken);
    }

    private LoginContext loginUsingPassword(final String principal, final SecureString password) throws LoginException {
        final Set<Principal> principals = Collections.singleton(new KerberosPrincipal(principal));

        final Subject subject = new Subject(false, principals, Collections.emptySet(), Collections.emptySet());

        final Configuration conf = new PasswordJaasConf(principal, enableDebugLogs);
        final CallbackHandler callback = new KrbCallbackHandler(principal, password);
        final LoginContext loginContext = new LoginContext(CRED_CONF_NAME, subject, callback, conf);
        loginContext.login();
        return loginContext;
    }

    /**
     * Handles server response and returns new token if any to be sent to server.
     *
     * @param base64Token inToken received from server passed to initSecContext for
     *            gss negotiation
     * @return Base64 encoded token to be sent to server. May return {@code null} if
     *         nothing to be sent.
     * @throws PrivilegedActionException when privileged action threw exception
     */
    String handleResponse(final String base64Token) throws PrivilegedActionException {
        if (gssContext.isEstablished()) {
            throw new IllegalStateException("GSS Context has already been established");
        }
        final byte[] token = Base64.getDecoder().decode(base64Token);
        final byte[] outToken = doAsWrapper(
            loginContext.getSubject(),
            (PrivilegedExceptionAction<byte[]>) () -> gssContext.initSecContext(token, 0, token.length)
        );
        if (outToken == null || outToken.length == 0) {
            return null;
        }
        return Base64.getEncoder().encodeToString(outToken);
    }

    /**
     * @return {@code true} If the gss security context was established
     */
    boolean isEstablished() {
        return gssContext.isEstablished();
    }

    static <T> T doAsWrapper(final Subject subject, final PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAs(subject, action));
    }
}
