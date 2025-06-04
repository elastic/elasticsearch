/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Tuple;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Utility class that validates kerberos ticket for peer authentication.
 * <p>
 * This class takes care of login by ES service credentials using keytab,
 * GSSContext establishment, and then validating the incoming token.
 * <p>
 * It may respond with token which needs to be communicated with the peer.
 */
public class KerberosTicketValidator {
    static final Oid SPNEGO_OID = getOid("1.3.6.1.5.5.2");
    static final Oid KERBEROS_V5_OID = getOid("1.2.840.113554.1.2.2");
    static final Oid[] SUPPORTED_OIDS = new Oid[] { SPNEGO_OID, KERBEROS_V5_OID };

    private static Oid getOid(final String id) {
        Oid oid = null;
        try {
            oid = new Oid(id);
        } catch (GSSException gsse) {
            throw ExceptionsHelper.convertToRuntime(gsse);
        }
        return oid;
    }

    private static final Logger LOGGER = LogManager.getLogger(KerberosTicketValidator.class);

    private static final String KEY_TAB_CONF_NAME = "KeytabConf";
    private static final String SUN_KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";

    /**
     * Validates client kerberos ticket received from the peer.
     * <p>
     * First performs service login using keytab, supports multiple principals in
     * keytab and the principal is selected based on the request.
     * <p>
     * The GSS security context establishment state is handled as follows: <br>
     * If the context is established it will call {@link ActionListener#onResponse}
     * with a {@link Tuple} of username and outToken for peer reply. <br>
     * If the context is not established then it will call
     * {@link ActionListener#onResponse} with a Tuple where username is null but
     * with a outToken that needs to be sent to peer for further negotiation. <br>
     * Never calls {@link ActionListener#onResponse} with a {@code null} tuple. <br>
     * On failure, it will call {@link ActionListener#onFailure(Exception)}
     *
     * @param decodedToken base64 decoded kerberos ticket bytes
     * @param keytabPath Path to Service key tab file containing credentials for ES
     *            service.
     * @param krbDebug if {@code true} enables jaas krb5 login module debug logs.
     */
    public void validateTicket(
        final byte[] decodedToken,
        final Path keytabPath,
        final boolean krbDebug,
        final ActionListener<Tuple<String, String>> actionListener
    ) {
        final GSSManager gssManager = GSSManager.getInstance();
        GSSContext gssContext = null;
        LoginContext loginContext = null;
        try {
            loginContext = serviceLogin(keytabPath.toString(), krbDebug);
            GSSCredential serviceCreds = createCredentials(gssManager, loginContext.getSubject());
            gssContext = gssManager.createContext(serviceCreds);
            final String base64OutToken = encodeToString(acceptSecContext(decodedToken, gssContext, loginContext.getSubject()));
            LOGGER.trace(
                "validateTicket isGSSContextEstablished = {}, username = {}, outToken = {}",
                gssContext.isEstablished(),
                gssContext.getSrcName().toString(),
                base64OutToken
            );
            actionListener.onResponse(new Tuple<>(gssContext.isEstablished() ? gssContext.getSrcName().toString() : null, base64OutToken));
        } catch (GSSException e) {
            actionListener.onFailure(e);
        } catch (PrivilegedActionException pve) {
            if (pve.getCause() instanceof LoginException) {
                actionListener.onFailure((LoginException) pve.getCause());
            } else if (pve.getCause() instanceof GSSException) {
                actionListener.onFailure((GSSException) pve.getCause());
            } else {
                actionListener.onFailure(pve.getException());
            }
        } finally {
            privilegedLogoutNoThrow(loginContext);
            privilegedDisposeNoThrow(gssContext);
        }
    }

    /**
     * Encodes the specified byte array using base64 encoding scheme
     *
     * @param outToken byte array to be encoded
     * @return String containing base64 encoded characters. returns {@code null} if
     *         outToken is null or empty.
     */
    private static String encodeToString(final byte[] outToken) {
        if (outToken != null && outToken.length > 0) {
            return Base64.getEncoder().encodeToString(outToken);
        }
        return null;
    }

    /**
     * Handles GSS context establishment. Received token is passed to the GSSContext
     * on acceptor side and returns with out token that needs to be sent to peer for
     * further GSS context establishment.
     *
     * @param base64decodedTicket in token generated by peer
     * @param gssContext instance of acceptor {@link GSSContext}
     * @param subject authenticated subject
     * @return a byte[] containing the token to be sent to the peer. null indicates
     *         that no token is generated.
     * @throws PrivilegedActionException when privileged action threw exception
     * @see GSSContext#acceptSecContext(byte[], int, int)
     */
    private static byte[] acceptSecContext(final byte[] base64decodedTicket, final GSSContext gssContext, Subject subject)
        throws PrivilegedActionException {
        // process token with gss context
        return doAsWrapper(
            subject,
            (PrivilegedExceptionAction<byte[]>) () -> gssContext.acceptSecContext(base64decodedTicket, 0, base64decodedTicket.length)
        );
    }

    /**
     * For acquiring SPNEGO mechanism credentials for service based on the subject
     *
     * @param gssManager {@link GSSManager}
     * @param subject logged in {@link Subject}
     * @return {@link GSSCredential} for particular mechanism
     * @throws PrivilegedActionException when privileged action threw exception
     */
    private static GSSCredential createCredentials(final GSSManager gssManager, final Subject subject) throws PrivilegedActionException {
        return doAsWrapper(
            subject,
            (PrivilegedExceptionAction<GSSCredential>) () -> gssManager.createCredential(
                null,
                GSSCredential.DEFAULT_LIFETIME,
                SUPPORTED_OIDS,
                GSSCredential.ACCEPT_ONLY
            )
        );
    }

    /**
     * Privileged Wrapper that invokes action with Subject.doAs to perform work as
     * given subject.
     *
     * @param subject {@link Subject} to be used for this work
     * @param action {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @return the value returned by the PrivilegedExceptionAction's run method
     * @throws PrivilegedActionException when privileged action threw exception
     */
    private static <T> T doAsWrapper(final Subject subject, final PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAs(subject, action));
        } catch (PrivilegedActionException pae) {
            if (pae.getCause() instanceof PrivilegedActionException) {
                throw (PrivilegedActionException) pae.getCause();
            }
            throw pae;
        }
    }

    /**
     * Privileged wrapper for closing GSSContext, does not throw exceptions but logs
     * them as a debug message.
     *
     * @param gssContext GSSContext to be disposed.
     */
    private static void privilegedDisposeNoThrow(final GSSContext gssContext) {
        if (gssContext != null) {
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    gssContext.dispose();
                    return null;
                });
            } catch (PrivilegedActionException e) {
                LOGGER.debug("Could not dispose GSS Context", e.getCause());
            }
        }
    }

    /**
     * Privileged wrapper for closing LoginContext, does not throw exceptions but
     * logs them as a debug message.
     *
     * @param loginContext LoginContext to be closed
     */
    private static void privilegedLogoutNoThrow(final LoginContext loginContext) {
        if (loginContext != null) {
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    loginContext.logout();
                    return null;
                });
            } catch (PrivilegedActionException e) {
                LOGGER.debug("Could not close LoginContext", e.getCause());
            }
        }
    }

    /**
     * Performs authentication using provided keytab
     *
     * @param keytabFilePath Keytab file path
     * @param krbDebug if {@code true} enables jaas krb5 login module debug logs.
     * @return authenticated {@link LoginContext} instance. Note: This needs to be
     *         closed using {@link LoginContext#logout()} after usage.
     * @throws PrivilegedActionException when privileged action threw exception
     */
    private static LoginContext serviceLogin(final String keytabFilePath, final boolean krbDebug) throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<LoginContext>) () -> {
            final Subject subject = new Subject(false, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
            final Configuration conf = new KeytabJaasConf(keytabFilePath, krbDebug);
            final LoginContext loginContext = new LoginContext(KEY_TAB_CONF_NAME, subject, null, conf);
            loginContext.login();
            return loginContext;
        });
    }

    /**
     * Usually we would have a JAAS configuration file for login configuration. As
     * we have static configuration except debug flag, we are constructing in
     * memory. This avoids additional configuration required from the user.
     * <p>
     * As we are using this instead of jaas.conf, this requires refresh of
     * {@link Configuration} and requires appropriate security permissions to do so.
     */
    static class KeytabJaasConf extends Configuration {
        private final String keytabFilePath;
        private final boolean krbDebug;

        KeytabJaasConf(final String keytabFilePath, final boolean krbDebug) {
            this.keytabFilePath = keytabFilePath;
            this.krbDebug = krbDebug;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(final String name) {
            return new AppConfigurationEntry[] {
                new AppConfigurationEntry(
                    SUN_KRB5_LOGIN_MODULE,
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    Map.of(
                        "keyTab",
                        keytabFilePath,
                        // as acceptor, we can have multiple SPNs, we do not want to use any particular principal so it uses "*"
                        "principal",
                        "*",
                        "useKeyTab",
                        Boolean.TRUE.toString(),
                        "storeKey",
                        Boolean.TRUE.toString(),
                        "doNotPrompt",
                        Boolean.TRUE.toString(),
                        "isInitiator",
                        Boolean.FALSE.toString(),
                        "debug",
                        Boolean.toString(krbDebug)
                    )
                ) };
        }

    }
}
