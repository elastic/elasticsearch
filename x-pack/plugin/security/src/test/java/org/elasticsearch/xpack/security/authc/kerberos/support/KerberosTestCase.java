/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
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

import javax.security.auth.Subject;

/**
 * Base Test class for Kerberos
 */
public abstract class KerberosTestCase extends ESTestCase {

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
    protected Path createPrincipalKeyTab(final Path dir, final String... principalNames) throws Exception {
        final Path ktabPath = dir.resolve(randomAlphaOfLength(10) + ".keytab");
        miniKdc.createPrincipal(ktabPath.toFile(), principalNames);
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
     * @param subject {@link Subject}
     * @param action {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @return
     * @throws PrivilegedActionException
     */
    public static <T> T doAsWrapper(Subject subject, PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
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
        Settings.Builder builder = Settings.builder().put(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.getKey(), keytabPath)
                .put(KerberosRealmSettings.CACHE_MAX_USERS_SETTING.getKey(), maxUsersInCache)
                .put(KerberosRealmSettings.CACHE_TTL_SETTING.getKey(), cacheTTL)
                .put(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.getKey(), enableDebugging);
        return builder.build();
    }

}
