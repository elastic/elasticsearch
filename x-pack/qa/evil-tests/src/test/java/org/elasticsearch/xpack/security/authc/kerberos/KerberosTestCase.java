/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.security.auth.Subject;

import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Base Test class for Kerberos.
 * <p>
 * Takes care of starting {@link SimpleKdcLdapServer} as Kdc server backed by
 * Ldap Server.
 * <p>
 * Also assists in building principal names, creation of principals and realm
 * settings.
 */
public abstract class KerberosTestCase extends ESTestCase {

    protected static final String REALM_NAME = "test-kerb-realm";

    protected Settings globalSettings;
    protected Settings settings;
    protected List<String> serviceUserNames;
    protected List<String> clientUserNames;
    protected Path workDir = null;

    protected SimpleKdcLdapServer simpleKdcLdapServer;

    private static Locale restoreLocale;

    /*
     * Arabic and other language have problems due to handling of generalized time in SimpleKdcServer. For more, look at
     * org.apache.kerby.asn1.type.Asn1GeneralizedTime#toBytes
     */
    private static Set<String> UNSUPPORTED_LOCALE_LANGUAGES = Set.of(
        "ar",
        "ja",
        "th",
        "hi",
        "uz",
        "fa",
        "ks",
        "ckb",
        "ne",
        "dz",
        "mzn",
        "mr",
        "as",
        "bn",
        "lrc",
        "my",
        "ps",
        "ur",
        "pa",
        "ig",
        "sd");

    @BeforeClass
    public static void setupKerberos() throws Exception {
        if (isLocaleUnsupported()) {
            Logger logger = LogManager.getLogger(KerberosTestCase.class);
            logger.warn("Attempting to run Kerberos test on {} locale, but that breaks SimpleKdcServer. Switching to English.",
                Locale.getDefault());
            restoreLocale = Locale.getDefault();
            Locale.setDefault(Locale.ENGLISH);
        }
    }

    @AfterClass
    public static void restoreLocale() {
        if (restoreLocale != null) {
            Locale.setDefault(restoreLocale);
            restoreLocale = null;
        }
    }

    private static boolean isLocaleUnsupported() {
        return UNSUPPORTED_LOCALE_LANGUAGES.contains(Locale.getDefault().getLanguage());
    }

    @Before
    public void startSimpleKdcLdapServer() throws Exception {
        workDir = createTempDir();
        globalSettings = Settings.builder().put("path.home", workDir).build();

        final Path kdcLdiff = getDataPath("/kdc.ldiff");
        simpleKdcLdapServer = new SimpleKdcLdapServer(workDir, "com", "example", kdcLdiff);

        // Create SPNs and UPNs
        serviceUserNames = new ArrayList<>();
        Randomness.get().ints(randomIntBetween(1, 6)).forEach((i) -> {
            serviceUserNames.add("HTTP/" + randomAlphaOfLength(8));
        });
        final Path ktabPathForService = createPrincipalKeyTab(workDir, serviceUserNames.toArray(new String[0]));
        clientUserNames = new ArrayList<>();
        Randomness.get().ints(randomIntBetween(1, 6)).forEach((i) -> {
            String clientUserName = "client-" + randomAlphaOfLength(8);
            clientUserNames.add(clientUserName);
            try {
                createPrincipal(clientUserName, "spnego-test-password".toCharArray());
            } catch (Exception e) {
                throw ExceptionsHelper.convertToRuntime(e);
            }
        });
        settings =  KerberosRealmTestCase.buildKerberosRealmSettings(REALM_NAME, ktabPathForService.toString());
    }

    @After
    public void tearDownMiniKdc() throws IOException, PrivilegedActionException {
        if (simpleKdcLdapServer != null) {
            simpleKdcLdapServer.stop();
        }
    }

    protected Path getKeytabPath(Environment env) {
        final Setting<String> setting = KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.getConcreteSettingForNamespace(REALM_NAME);
        return env.configFile().resolve(setting.get(settings));
    }

    /**
     * Creates principals and exports them to the keytab created in the directory.
     *
     * @param dir        Directory where the key tab would be created.
     * @param princNames principal names to be created
     * @return {@link Path} to key tab file.
     * @throws Exception thrown if principal or keytab could not be created
     */
    protected Path createPrincipalKeyTab(final Path dir, final String... princNames) throws Exception {
        final Path path = dir.resolve(randomAlphaOfLength(10) + ".keytab");
        simpleKdcLdapServer.createPrincipal(path, princNames);
        return path;
    }

    /**
     * Creates principal with given name and password.
     *
     * @param principalName Principal name
     * @param password      Password
     * @throws Exception thrown if principal could not be created
     */
    protected void createPrincipal(final String principalName, final char[] password) throws Exception {
        simpleKdcLdapServer.createPrincipal(principalName, new String(password));
    }

    /**
     * Appends realm name to user to form principal name
     *
     * @param user user name
     * @return principal name in the form user@REALM
     */
    protected String principalName(final String user) {
        return user + "@" + simpleKdcLdapServer.getRealm();
    }

    /**
     * Invokes Subject.doAs inside a doPrivileged block
     *
     * @param subject {@link Subject}
     * @param action  {@link PrivilegedExceptionAction} action for performing inside
     *                Subject.doAs
     * @return T Type of value as returned by PrivilegedAction
     * @throws PrivilegedActionException when privileged action threw exception
     */
    static <T> T doAsWrapper(final Subject subject, final PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAs(subject, action));
    }

}
