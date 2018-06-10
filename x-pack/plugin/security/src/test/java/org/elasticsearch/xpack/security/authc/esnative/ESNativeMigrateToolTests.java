/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.security.authc.support.CharArrays;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the {@code ESNativeMigrateTool}
 */
public class ESNativeMigrateToolTests extends NativeRealmIntegTestCase {

    // Randomly use SSL (or not)
    private static boolean useSSL;

    @BeforeClass
    public static void setSSL() {
        useSSL = randomBoolean();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        logger.info("--> use SSL? {}", useSSL);
        Settings s = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.security.http.ssl.enabled", useSSL)
                .build();
        return s;
    }

    @Override
    protected boolean transportSSLEnabled() {
        return useSSL;
    }

    @Override
    protected boolean shouldSetReservedUserPasswords() {
        return false;
    }

    private Environment nodeEnvironment() throws Exception {
        return internalCluster().getInstances(Environment.class).iterator().next();
    }

    public void testRetrieveUsers() throws Exception {
        final Environment nodeEnvironment = nodeEnvironment();
        String home = Environment.PATH_HOME_SETTING.get(nodeEnvironment.settings());
        Path conf = nodeEnvironment.configFile();
        SecurityClient c = new SecurityClient(client());
        logger.error("--> creating users");
        int numToAdd = randomIntBetween(1,10);
        Set<String> addedUsers = new HashSet(numToAdd);
        for (int i = 0; i < numToAdd; i++) {
            String uname = randomAlphaOfLength(5);
            c.preparePutUser(uname, "s3kirt".toCharArray(), SecuritySettingsSource.HASHING_ALGORITHM, "role1", "user").get();
            addedUsers.add(uname);
        }
        logger.error("--> waiting for .security index");
        ensureGreen(SecurityIndexManager.SECURITY_INDEX_NAME);

        MockTerminal t = new MockTerminal();
        String username = nodeClientUsername();
        String password = new String(CharArrays.toUtf8Bytes(nodeClientPassword().getChars()), StandardCharsets.UTF_8);
        String url = getHttpURL();
        ESNativeRealmMigrateTool.MigrateUserOrRoles muor = new ESNativeRealmMigrateTool.MigrateUserOrRoles();

        Settings.Builder builder = Settings.builder()
                .put("path.home", home)
                .put("path.conf", conf.toString());
        SecuritySettingsSource.addSSLSettingsForStore(builder,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks", "testnode");
        Settings settings = builder.build();
        logger.error("--> retrieving users using URL: {}, home: {}", url, home);

        OptionParser parser = muor.getParser();
        OptionSet options = parser.parse("-u", username, "-p", password, "-U", url);
        logger.info("--> options: {}", options.asMap());
        Set<String> users = muor.getUsersThatExist(t, settings, new Environment(settings, conf), options);
        logger.info("--> output: \n{}", t.getOutput());
        for (String u : addedUsers) {
            assertThat("expected list to contain: " + u + ", real list: " + users, users.contains(u), is(true));
        }
    }

    public void testRetrieveRoles() throws Exception {
        final Environment nodeEnvironment = nodeEnvironment();
        String home = Environment.PATH_HOME_SETTING.get(nodeEnvironment.settings());
        Path conf = nodeEnvironment.configFile();
        SecurityClient c = new SecurityClient(client());
        logger.error("--> creating roles");
        int numToAdd = randomIntBetween(1,10);
        Set<String> addedRoles = new HashSet<>(numToAdd);
        for (int i = 0; i < numToAdd; i++) {
            String rname = randomAlphaOfLength(5);
            c.preparePutRole(rname)
                    .cluster("all", "none")
                    .runAs("root", "nobody")
                    .addIndices(new String[]{"index"}, new String[]{"read"},
                            new String[]{"body", "title"}, null, new BytesArray("{\"query\": {\"match_all\": {}}}"))
                    .get();
            addedRoles.add(rname);
        }
        logger.error("--> waiting for .security index");
        ensureGreen(SecurityIndexManager.SECURITY_INDEX_NAME);

        MockTerminal t = new MockTerminal();
        String username = nodeClientUsername();
        String password = new String(CharArrays.toUtf8Bytes(nodeClientPassword().getChars()), StandardCharsets.UTF_8);
        String url = getHttpURL();
        ESNativeRealmMigrateTool.MigrateUserOrRoles muor = new ESNativeRealmMigrateTool.MigrateUserOrRoles();
        Settings.Builder builder = Settings.builder().put("path.home", home);
        SecuritySettingsSource.addSSLSettingsForStore(builder,
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks", "testclient");
        Settings settings = builder.build();
        logger.error("--> retrieving roles using URL: {}, home: {}", url, home);

        OptionParser parser = muor.getParser();
        OptionSet options = parser.parse("-u", username, "-p", password, "-U", url);
        Set<String> roles = muor.getRolesThatExist(t, settings, new Environment(settings, conf), options);
        logger.info("--> output: \n{}", t.getOutput());;
        for (String r : addedRoles) {
            assertThat("expected list to contain: " + r, roles.contains(r), is(true));
        }
    }
}
