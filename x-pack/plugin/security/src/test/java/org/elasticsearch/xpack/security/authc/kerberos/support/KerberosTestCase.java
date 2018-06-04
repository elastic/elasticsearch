/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

/**
 * Base Test class for Kerberos
 */
public abstract class KerberosTestCase extends ESTestCase {

    protected Settings globalSettings;
    protected Settings settings;
    protected List<String> serviceUserNames = new ArrayList<>();
    protected List<String> clientUserNames = new ArrayList<>();
    protected Path workDir = null;

    protected SimpleKdcLdapServer simpleKdcLdapServer;

    @Before
    public void startMiniKdc() throws Exception {
        workDir = createTempDir();
        globalSettings = Settings.builder().put("path.home", workDir).build();

        final Path kdcLdiff = getDataPath("/kdc.ldiff");
        simpleKdcLdapServer = new SimpleKdcLdapServer(workDir, "com", "example", kdcLdiff);

        // Create SPNs and UPNs
        serviceUserNames.clear();
        Randomness.get().ints(randomIntBetween(1, 6)).forEach((i) -> {
            serviceUserNames.add("HTTP/" + randomAlphaOfLength(6));
        });
        final Path ktabPathForService = createPrincipalKeyTab(workDir, serviceUserNames.toArray(new String[0]));
        clientUserNames.clear();
        Randomness.get().ints(randomIntBetween(1, 6)).forEach((i) -> {
            String clientUserName = "client-" + randomAlphaOfLength(6);
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

    protected Path createPrincipalKeyTab(final Path dir, final String... princNames) throws Exception {
        final Path path = dir.resolve(randomAlphaOfLength(10) + ".keytab");
        simpleKdcLdapServer.createPrincipal(path, princNames);
        return path;
    }

    protected void createPrincipal(final String principalName, final char[] password) throws Exception {
        simpleKdcLdapServer.createPrincipal(principalName, new String(password));
    }

    protected String principalName(final String user) {
        return user + "@" + simpleKdcLdapServer.getRealm();
    }

    /**
     * Invokes Subject.doAs inside a doPrivileged block
     *
     * @param subject {@link Subject}
     * @param action {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @return
     * @throws PrivilegedActionException
     */
    public static <T> T doAsWrapper(final Subject subject, final PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAs(subject, action));
    }

    public static Path writeKeyTab(final Path dir, final String name, final String content) throws IOException {
        final Path path = dir.resolve(name);
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(path, StandardCharsets.US_ASCII)) {
            bufferedWriter.write(Strings.isNullOrEmpty(content) ? "test-content" : content);
        }
        return path;
    }

    public static Settings buildKerberosRealmSettings(final String keytabPath) {
        return buildKerberosRealmSettings(keytabPath, 100, "10m", true);
    }

    public static Settings buildKerberosRealmSettings(final String keytabPath, final int maxUsersInCache, final String cacheTTL,
            final boolean enableDebugging) {
        final Settings.Builder builder = Settings.builder().put(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.getKey(), keytabPath)
                .put(KerberosRealmSettings.CACHE_MAX_USERS_SETTING.getKey(), maxUsersInCache)
                .put(KerberosRealmSettings.CACHE_TTL_SETTING.getKey(), cacheTTL)
                .put(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.getKey(), enableDebugging);
        return builder.build();
    }

}
