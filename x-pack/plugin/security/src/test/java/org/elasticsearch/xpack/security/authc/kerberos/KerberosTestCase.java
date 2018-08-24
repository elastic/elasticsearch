/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.security.auth.Subject;

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

    protected Settings globalSettings;
    protected Settings settings;
    protected List<String> serviceUserNames;
    protected List<String> clientUserNames;
    protected Path workDir = null;

    protected SimpleKdcLdapServer simpleKdcLdapServer;

    private static Locale restoreLocale;
    private static Set<String> unsupportedLocaleLanguages;
    static {
        unsupportedLocaleLanguages = new HashSet<>();
        /*
         * arabic and other languages have problem due to handling of GeneralizedTime in
         * SimpleKdcServer For more look at :
         * org.apache.kerby.asn1.type.Asn1GeneralizedTime#toBytes()
         */
        unsupportedLocaleLanguages.add("ar");
        unsupportedLocaleLanguages.add("ja");
        unsupportedLocaleLanguages.add("th");
        unsupportedLocaleLanguages.add("hi");
        unsupportedLocaleLanguages.add("uz");
        unsupportedLocaleLanguages.add("fa");
        unsupportedLocaleLanguages.add("ks");
        unsupportedLocaleLanguages.add("ckb");
        unsupportedLocaleLanguages.add("ne");
        unsupportedLocaleLanguages.add("dz");
        unsupportedLocaleLanguages.add("mzn");
    }

    @BeforeClass
    public static void setupKerberos() throws Exception {
        if (isLocaleUnsupported()) {
            Logger logger = Loggers.getLogger(KerberosTestCase.class);
            logger.warn("Attempting to run Kerberos test on {} locale, but that breaks SimpleKdcServer. Switching to English.",
                    Locale.getDefault());
            restoreLocale = Locale.getDefault();
            Locale.setDefault(Locale.ENGLISH);
        }
    }

    @AfterClass
    public static void restoreLocale() throws Exception {
        if (restoreLocale != null) {
            Locale.setDefault(restoreLocale);
            restoreLocale = null;
        }
    }

    private static boolean isLocaleUnsupported() {
        return unsupportedLocaleLanguages.contains(Locale.getDefault().getLanguage());
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
                createPrincipal(clientUserName, "pwd".toCharArray());
            } catch (Exception e) {
                throw ExceptionsHelper.convertToRuntime(e);
            }
        });
        settings = buildKerberosRealmSettings(ktabPathForService.toString());
    }

    @After
    public void tearDownMiniKdc() throws IOException, PrivilegedActionException {
        simpleKdcLdapServer.stop();
    }

    /**
     * Creates principals and exports them to the keytab created in the directory.
     *
     * @param dir Directory where the key tab would be created.
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
     * @param password Password
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
     * @param action {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @return T Type of value as returned by PrivilegedAction
     * @throws PrivilegedActionException when privileged action threw exception
     */
    static <T> T doAsWrapper(final Subject subject, final PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAs(subject, action));
    }

    /**
     * Write content to provided keytab file.
     *
     * @param keytabPath {@link Path} to keytab file.
     * @param content Content for keytab
     * @return key tab path
     * @throws IOException if I/O error occurs while writing keytab file
     */
    public static Path writeKeyTab(final Path keytabPath, final String content) throws IOException {
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(keytabPath, StandardCharsets.US_ASCII)) {
            bufferedWriter.write(Strings.isNullOrEmpty(content) ? "test-content" : content);
        }
        return keytabPath;
    }

    /**
     * Build kerberos realm settings with default config and given keytab
     *
     * @param keytabPath key tab file path
     * @return {@link Settings} for kerberos realm
     */
    public static Settings buildKerberosRealmSettings(final String keytabPath) {
        return buildKerberosRealmSettings(keytabPath, 100, "10m", true, false);
    }

    /**
     * Build kerberos realm settings
     *
     * @param keytabPath key tab file path
     * @param maxUsersInCache max users to be maintained in cache
     * @param cacheTTL time to live for cached entries
     * @param enableDebugging for krb5 logs
     * @param removeRealmName {@code true} if we want to remove realm name from the username of form 'user@REALM'
     * @return {@link Settings} for kerberos realm
     */
    public static Settings buildKerberosRealmSettings(final String keytabPath, final int maxUsersInCache, final String cacheTTL,
            final boolean enableDebugging, final boolean removeRealmName) {
        final Settings.Builder builder = Settings.builder().put(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.getKey(), keytabPath)
                .put(KerberosRealmSettings.CACHE_MAX_USERS_SETTING.getKey(), maxUsersInCache)
                .put(KerberosRealmSettings.CACHE_TTL_SETTING.getKey(), cacheTTL)
                .put(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.getKey(), enableDebugging)
                .put(KerberosRealmSettings.SETTING_REMOVE_REALM_NAME.getKey(), removeRealmName);
        return builder.build();
    }

}
