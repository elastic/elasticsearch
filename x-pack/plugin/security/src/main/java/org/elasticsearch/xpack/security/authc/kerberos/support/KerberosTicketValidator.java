/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

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
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Responsible for validating Kerberos ticket<br>
 * Performs service login using keytab, supports multiple principals in keytab.
 */
public class KerberosTicketValidator {
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

    private static final Logger LOGGER = ESLoggerFactory.getLogger(KerberosTicketValidator.class);

    private static final String KEY_TAB_CONF_NAME = "KeytabConf";
    private static final String SUN_KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";

    /**
     * Validates client kerberos ticket.
     *
     * @param servicePrincipalName Service principal name
     * @param nameType {@link GSSName} type depending on GSS API principal entity
     * @param base64Ticket base64 encoded kerberos ticket
     * @param config {@link RealmConfig}
     * @return {@link Tuple} of user name {@link GSSContext#getSrcName()} and out
     *         token base64 encoded if any. When context is not yet established user
     *         name is {@code null}.
     * @throws LoginException thrown when service authentication fails
     *             {@link LoginContext#login()}
     * @throws GSSException thrown when GSS Context negotiation fails
     *             {@link GSSException}
     */
    public Tuple<String, String> validateTicket(final String servicePrincipalName, Oid nameType, final String base64Ticket,
            final RealmConfig config) throws LoginException, GSSException {
        final GSSManager gssManager = GSSManager.getInstance();
        GSSContext gssContext = null;
        LoginContext loginContext = null;
        try {
            Path keyTabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
            if (Files.exists(keyTabPath) == false) {
                throw new IllegalArgumentException("configured service key tab file does not exist for "
                        + RealmSettings.getFullSettingKey(config, KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH));
            }
            // do service login
            loginContext = serviceLogin(servicePrincipalName, keyTabPath.toString(), config.settings());
            // create credentials
            GSSCredential serviceCreds = createCredentials(servicePrincipalName, nameType, gssManager, loginContext);
            // create gss context
            gssContext = gssManager.createContext(serviceCreds);
            final byte[] outToken = acceptSecContext(base64Ticket, gssContext, loginContext);

            String base64OutToken = null;
            if (outToken != null && outToken.length > 0) {
                base64OutToken = Base64.getEncoder().encodeToString(outToken);
            }

            return new Tuple<>(gssContext.isEstablished() ? gssContext.getSrcName().toString() : null, base64OutToken);
        } catch (PrivilegedActionException pve) {
            if (pve.getException() instanceof LoginException) {
                throw (LoginException) pve.getCause();
            }
            if (pve.getException() instanceof GSSException) {
                throw (GSSException) pve.getCause();
            }
            throw ExceptionsHelper.convertToRuntime((Exception) ExceptionsHelper.unwrapCause(pve));
        } finally {
            privilegedDisposeNoThrow(loginContext);
            privilegedCloseNoThrow(gssContext);
        }
    }

    private static byte[] acceptSecContext(final String base64Ticket, GSSContext gssContext, LoginContext loginContext)
            throws PrivilegedActionException {
        final GSSContext finalGSSContext = gssContext;
        final byte[] token = Base64.getDecoder().decode(base64Ticket);
        // process token with gss context
        return doAsWrapper(loginContext.getSubject(),
                (PrivilegedExceptionAction<byte[]>) () -> finalGSSContext.acceptSecContext(token, 0, token.length));
    }

    private static GSSCredential createCredentials(final String servicePrincipalName, Oid nameType, final GSSManager gssManager,
            LoginContext loginContext) throws GSSException, PrivilegedActionException {
        final GSSName gssServicePrincipalName;
        if (servicePrincipalName.equals("*") == false) {
            gssServicePrincipalName = gssManager.createName(servicePrincipalName, nameType);
        } else {
            gssServicePrincipalName = null;
        }
        return doAsWrapper(loginContext.getSubject(), (PrivilegedExceptionAction<GSSCredential>) () -> gssManager
                .createCredential(gssServicePrincipalName, GSSCredential.DEFAULT_LIFETIME, SPNEGO_OID, GSSCredential.ACCEPT_ONLY));
    }

    /**
     * Privileged Wrapper that invokes action with Subject.doAs
     *
     * @param subject {@link Subject}
     * @param action {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @return
     * @throws PrivilegedActionException
     */
    private static <T> T doAsWrapper(Subject subject, PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAs(subject, action));
        } catch (PrivilegedActionException pae) {
            throw (PrivilegedActionException) pae.getException();
        }
    }

    /**
     * Privileged wrapper for closing GSSContext, does not throw exceptions but logs
     * them as warning.
     *
     * @param gssContext
     */
    private static void privilegedCloseNoThrow(final GSSContext gssContext) {
        if (gssContext != null) {
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    gssContext.dispose();
                    return null;
                });
            } catch (PrivilegedActionException e) {
                RuntimeException rte = ExceptionsHelper.convertToRuntime((Exception) ExceptionsHelper.unwrapCause(e));
                LOGGER.log(Level.DEBUG, "Could not dispose GSS Context", rte);
            }
            return;
        }
    }

    /**
     * Privileged wrapper for closing LoginContext, does not throw exceptions but
     * logs them as warning.
     *
     * @param loginContext
     */
    private static void privilegedDisposeNoThrow(final LoginContext loginContext) {
        if (loginContext != null) {
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    loginContext.logout();
                    return null;
                });
            } catch (PrivilegedActionException e) {
                RuntimeException rte = ExceptionsHelper.convertToRuntime((Exception) ExceptionsHelper.unwrapCause(e));
                LOGGER.log(Level.DEBUG, "Could not close LoginContext", rte);
            }
        }
    }

    /**
     * Performs authentication using provided principal name and keytab
     *
     * @param principal Principal name
     * @param keytabFilePath Keytab file path
     * @param settings {@link Settings}
     * @return authenticated {@link LoginContext} instance. Note: This needs to be
     *         closed {@link LoginContext#logout()} after usage.
     * @throws PrivilegedActionException
     */
    private static LoginContext serviceLogin(final String principal, final String keytabFilePath, final Settings settings)
            throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<LoginContext>) () -> {
            final Set<Principal> principals = new HashSet<>();
            if (principal.equals("*") == false) {
                principals.add(new KerberosPrincipal(principal));
            }

            final Subject subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());

            final Configuration conf = new KeytabJaasConf(principal, keytabFilePath, settings);
            final LoginContext loginContext = new LoginContext(KEY_TAB_CONF_NAME, subject, null, conf);
            loginContext.login();
            return loginContext;
        });
    }

    /**
     * Instead of jaas.conf, this requires refresh of {@link Configuration}.
     */
    static class KeytabJaasConf extends Configuration {
        private final String principal;
        private final String keytabFilePath;
        private final Settings settings;

        KeytabJaasConf(final String principal, final String keytabFilePath, final Settings settings) {
            this.principal = principal;
            this.keytabFilePath = keytabFilePath;
            this.settings = settings;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(final String name) {
            final Map<String, String> options = new HashMap<>();
            options.put("keyTab", keytabFilePath);
            options.put("principal", principal);
            options.put("useKeyTab", Boolean.TRUE.toString());
            options.put("storeKey", Boolean.TRUE.toString());
            options.put("doNotPrompt", Boolean.TRUE.toString());
            options.put("renewTGT", Boolean.FALSE.toString());
            options.put("refreshKrb5Config", Boolean.TRUE.toString());
            options.put("isInitiator", Boolean.FALSE.toString());
            options.put("debug", KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(settings).toString());

            return new AppConfigurationEntry[] {
                    new AppConfigurationEntry(SUN_KRB5_LOGIN_MODULE, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
        }

    }
}
