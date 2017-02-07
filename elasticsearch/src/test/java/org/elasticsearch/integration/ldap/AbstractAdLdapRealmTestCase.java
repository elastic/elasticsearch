/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration.ldap;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.security.authc.ldap.LdapRealm;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope.ONE_LEVEL;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope.SUB_TREE;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;
import static org.hamcrest.Matchers.equalTo;

/**
 * This test assumes all subclass tests will be of type SUITE.  It picks a random realm configuration for the tests, and
 * writes a group to role mapping file for each node.
 */
public abstract  class AbstractAdLdapRealmTestCase extends SecurityIntegTestCase {

    public static final String XPACK_SECURITY_AUTHC_REALMS_EXTERNAL = "xpack.security.authc.realms.external";
    public static final String PASSWORD = "NickFuryHeartsES";
    public static final String ASGARDIAN_INDEX = "gods";
    public static final String PHILANTHROPISTS_INDEX = "philanthropists";
    public static final String SECURITY_INDEX = "security";
    private static final String AD_ROLE_MAPPING =
            "SHIELD:  [ \"CN=SHIELD,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" ] \n" +
                    "Avengers:  [ \"CN=Avengers,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" ] \n" +
                    "Gods:  [ \"CN=Gods,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" ] \n" +
                    "Philanthropists:  [ \"CN=Philanthropists,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" ] \n";
    private static final String OLDAP_ROLE_MAPPING =
            "SHIELD: [ \"cn=SHIELD,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com\" ] \n" +
                    "Avengers: [ \"cn=Avengers,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com\" ] \n" +
                    "Gods: [ \"cn=Gods,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com\" ] \n" +
                    "Philanthropists: [ \"cn=Philanthropists,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com\" ] \n";

    protected static RealmConfig realmConfig;
    protected static boolean useGlobalSSL;

    @BeforeClass
    public static void setupRealm() {
        realmConfig = randomFrom(RealmConfig.values());
        useGlobalSSL = randomBoolean();
        ESLoggerFactory.getLogger("test").info("running test with realm configuration [{}], with direct group to role mapping [{}]. " +
                        "Settings [{}]", realmConfig, realmConfig.mapGroupsAsRoles, realmConfig.settings.getAsMap());
    }

    @AfterClass
    public static void cleanupRealm() {
        realmConfig = null;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Path nodeFiles = createTempDir();
        Path store = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        Settings.Builder builder = Settings.builder();
        if (useGlobalSSL) {
            builder.put(super.nodeSettings(nodeOrdinal).filter((s) -> s.startsWith("xpack.ssl.") == false))
                    .put(sslSettingsForStore(store, "testnode"));
        } else {
            builder.put(super.nodeSettings(nodeOrdinal));
        }
        builder.put(realmConfig.buildSettings(store, "testnode"))
                .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".files.role_mapping", writeFile(nodeFiles, "role_mapping.yml",
                        configRoleMappings()));
        return builder.build();
    }

    @Override
    protected Settings transportClientSettings() {
        if (useGlobalSSL) {
            Path store = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
            return Settings.builder()
                    .put(super.transportClientSettings().filter((s) -> s.startsWith("xpack.ssl.") == false))
                    .put(sslSettingsForStore(store, "testnode"))
                    .build();
        } else {
            return super.transportClientSettings();
        }
    }
    @Override
    protected boolean useGeneratedSSLConfig() {
        return useGlobalSSL == false;
    }

    protected String configRoleMappings() {
        return realmConfig.configRoleMappings();
    }

    @Override
    protected String configRoles() {
        return super.configRoles() +
                "\n" +
                "Avengers:\n" +
                "  cluster: [ NONE ]\n" +
                "  indices:\n" +
                "    - names: 'avengers'\n" +
                "      privileges: [ all ]\n" +
                "SHIELD:\n" +
                "  cluster: [ NONE ]\n" +
                "  indices:\n" +
                "    - names: '" + SECURITY_INDEX + "'\n" +
                "      privileges: [ all ]\n" +
                "Gods:\n" +
                "  cluster: [ NONE ]\n" +
                "  indices:\n" +
                "    - names: '" + ASGARDIAN_INDEX + "'\n" +
                "      privileges: [ all ]\n" +
                "Philanthropists:\n" +
                "  cluster: [ NONE ]\n" +
                "  indices:\n" +
                "    - names: '" + PHILANTHROPISTS_INDEX + "'\n" +
                "      privileges: [ all ]\n";
    }

    protected void assertAccessAllowed(String user, String index) throws IOException {
        Client client = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, userHeader(user, PASSWORD)));
        IndexResponse indexResponse = client.prepareIndex(index, "type").
                setSource(jsonBuilder()
                        .startObject()
                        .field("name", "value")
                        .endObject())
                .execute().actionGet();

        assertEquals("user " + user + " should have write access to index " + index,
                DocWriteResponse.Result.CREATED, indexResponse.getResult());

        refresh();

        GetResponse getResponse = client.prepareGet(index, "type", indexResponse.getId())
                .get();

        assertThat("user " + user + " should have read access to index " + index, getResponse.getId(), equalTo(indexResponse.getId()));
    }

    protected void assertAccessDenied(String user, String index) throws IOException {
        try {
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, userHeader(user, PASSWORD)))
                    .prepareIndex(index, "type").
                    setSource(jsonBuilder()
                            .startObject()
                            .field("name", "value")
                            .endObject())
                    .execute().actionGet();
            fail("Write access to index " + index + " should not be allowed for user " + user);
        } catch (ElasticsearchSecurityException e) {
            // expected
        }
        refresh();
    }

    protected static String userHeader(String username, String password) {
        return UsernamePasswordToken.basicAuthHeaderValue(username, new SecuredString(password.toCharArray()));
    }

    private Settings sslSettingsForStore(Path store, String password) {
        return Settings.builder()
                .put("xpack.ssl.keystore.path", store)
                .put("xpack.ssl.keystore.password", password)
                .put("xpack.ssl.verification_mode", "certificate")
                .put("xpack.ssl.truststore.path", store)
                .put("xpack.ssl.truststore.password", password).build();
    }

    /**
     * Represents multiple possible configurations for active directory and ldap
     */
    enum RealmConfig {

        AD(false, AD_ROLE_MAPPING,
                Settings.builder()
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".type", LdapRealm.AD_TYPE)
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".domain_name", "ad.test.elasticsearch.com")
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL
                                + ".group_search.base_dn", "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".group_search.scope", randomBoolean() ? SUB_TREE : ONE_LEVEL)
                        .build()),

        AD_SSL(false, AD_ROLE_MAPPING,
                Settings.builder()
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".type", LdapRealm.AD_TYPE)
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".domain_name", "ad.test.elasticsearch.com")
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL
                                + ".group_search.base_dn", "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".group_search.scope", randomBoolean() ? SUB_TREE : ONE_LEVEL)
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".url", "ldap://ad.test.elasticsearch.com:389")
                        .build()),

        AD_LDAP_GROUPS_FROM_SEARCH(true, AD_ROLE_MAPPING,
                Settings.builder()
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".type", LdapRealm.LDAP_TYPE)
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".url", "ldaps://ad.test.elasticsearch.com:636")
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL
                                + ".group_search.base_dn", "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".group_search.scope", randomBoolean() ? SUB_TREE : ONE_LEVEL)
                        .putArray(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".user_dn_templates",
                                "cn={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                        .build()),

        AD_LDAP_GROUPS_FROM_ATTRIBUTE(true, AD_ROLE_MAPPING,
                Settings.builder()
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".type", LdapRealm.LDAP_TYPE)
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".url", "ldaps://ad.test.elasticsearch.com:636")
                        .putArray(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".user_dn_templates",
                                "cn={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                        .build()),

        OLDAP(false, OLDAP_ROLE_MAPPING,
                Settings.builder()
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".type", LdapRealm.LDAP_TYPE)
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".url", "ldaps://54.200.235.244:636")
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".group_search.base_dn",
                                "ou=people, dc=oldap, dc=test, dc=elasticsearch, dc=com")
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".group_search.scope", randomBoolean() ? SUB_TREE : ONE_LEVEL)
                        .putArray(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".user_dn_templates",
                                "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com")
                        .build());

        final boolean mapGroupsAsRoles;
        final boolean loginWithCommonName;
        private final String roleMappings;
        private final Settings settings;

        RealmConfig(boolean loginWithCommonName, String roleMappings, Settings settings) {
            this.settings = settings;
            this.loginWithCommonName = loginWithCommonName;
            this.roleMappings = roleMappings;
            this.mapGroupsAsRoles = randomBoolean();
        }

        public Settings buildSettings(Path store, String password) {
            Settings.Builder builder = Settings.builder()
                    .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".order", 1)
                    .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".hostname_verification", false)
                    .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".unmapped_groups_as_roles", mapGroupsAsRoles)
                    .put(this.settings);
            if (useGlobalSSL == false) {
                builder.put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".ssl.truststore.path", store)
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".ssl.truststore.password", password);
            }

            return builder.build();
        }

        //if mapGroupsAsRoles is turned on we don't write anything to the rolemapping file
        public String configRoleMappings() {
            return mapGroupsAsRoles ? "" : roleMappings;
        }
    }
}
