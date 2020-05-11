/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingResponse;
import org.elasticsearch.xpack.core.security.authc.ldap.ActiveDirectorySessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope.ONE_LEVEL;
import static org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope.SUB_TREE;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.security.authc.ldap.AbstractActiveDirectoryTestCase.AD_GC_LDAPS_PORT;
import static org.elasticsearch.xpack.security.authc.ldap.AbstractActiveDirectoryTestCase.AD_GC_LDAP_PORT;
import static org.elasticsearch.xpack.security.authc.ldap.AbstractActiveDirectoryTestCase.AD_LDAPS_PORT;
import static org.elasticsearch.xpack.security.authc.ldap.AbstractActiveDirectoryTestCase.AD_LDAP_PORT;
import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;
import static org.hamcrest.Matchers.equalTo;

/**
 * This test assumes all subclass tests will be of type SUITE.  It picks a random realm configuration for the tests, and
 * writes a group to role mapping file for each node.
 */
public abstract class AbstractAdLdapRealmTestCase extends SecurityIntegTestCase {

    public static final String XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL = "xpack.security.authc.realms.active_directory.external";
    public static final String XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL = "xpack.security.authc.realms.ldap.external";
    public static final String PASSWORD = AbstractActiveDirectoryTestCase.PASSWORD;
    public static final String ASGARDIAN_INDEX = "gods";
    public static final String PHILANTHROPISTS_INDEX = "philanthropists";
    public static final String SECURITY_INDEX = "security";

    private static final RoleMappingEntry[] AD_ROLE_MAPPING = new RoleMappingEntry[] {
            new RoleMappingEntry(
                    "SHIELD:  [ \"CN=SHIELD,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" ]",
                    "{ \"roles\":[\"SHIELD\"], \"enabled\":true, \"rules\":" +
                            "{\"field\": {\"groups\": \"CN=SHIELD,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\"} } }"
            ),
            new RoleMappingEntry(
                    "Avengers:  [ \"CN=Avengers,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" ]",
                    "{ \"roles\":[\"Avengers\"], \"enabled\":true, \"rules\":" +
                            "{ \"field\": { \"groups\" : \"CN=Avengers,CN=Users,*\" } } }"
            ),
            new RoleMappingEntry(
                    "Gods:  [ \"CN=Gods,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" ]",
                    "{ \"roles\":[\"Gods\"], \"enabled\":true, \"rules\":{\"any\": [" +
                            " { \"field\":{ \"groups\":    \"CN=Gods,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" } }," +
                            " { \"field\":{ \"groups\": \"CN=Deities,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" } } " +
                            "] } }"
            ),
            new RoleMappingEntry(
                    "Philanthropists:  [ \"CN=Philanthropists,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" ]",
                    "{ \"roles\":[\"Philanthropists\"], \"enabled\":true, \"rules\": { \"all\": [" +
                            " { \"field\": { \"groups\" : \"CN=Philanthropists,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com\" } }," +
                            " { \"field\": { \"realm.name\" : \"external\" } } " +
                            "] } }"
            )
    };

    protected static RealmConfig realmConfig;
    protected static List<RoleMappingEntry> roleMappings;

    @BeforeClass
    public static void setupRealm() {
        realmConfig = randomFrom(RealmConfig.values());
        roleMappings = realmConfig.selectRoleMappings(ESTestCase::randomBoolean);
        LogManager.getLogger(AbstractAdLdapRealmTestCase.class).info(
                "running test with realm configuration [{}], with direct group to role mapping [{}]. Settings [{}]",
                realmConfig, realmConfig.mapGroupsAsRoles, realmConfig.settings);
    }

    @AfterClass
    public static void cleanupRealm() {
        realmConfig = null;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final RealmConfig realm = AbstractAdLdapRealmTestCase.realmConfig;
        Settings.Builder builder = Settings.builder();
        builder.put(super.nodeSettings(nodeOrdinal), true);
        builder.put(buildRealmSettings(realm, roleMappings, getNodeTrustedCertificates()));
        return builder.build();
    }

    protected Settings buildRealmSettings(RealmConfig realm, List<RoleMappingEntry> roleMappingEntries, List<String>
        certificateAuthorities) {
        Settings.Builder builder = Settings.builder();
        builder.put(realm.buildSettings(certificateAuthorities));
        configureFileRoleMappings(builder, realm.type, roleMappingEntries);
        return builder.build();
    }

    @Before
    public void setupRoleMappings() throws Exception {
        assertSecurityIndexActive();

        List<String> content = getRoleMappingContent(RoleMappingEntry::getNativeContent);
        if (content.isEmpty()) {
            return;
        }
        Map<String, ActionFuture<PutRoleMappingResponse>> futures = new LinkedHashMap<>(content.size());
        for (int i = 0; i < content.size(); i++) {
            final String name = "external_" + i;
            final PutRoleMappingRequestBuilder builder = new PutRoleMappingRequestBuilder(client())
                .source(name, new BytesArray(content.get(i)), XContentType.JSON);
            futures.put(name, builder.execute());
        }
        for (String mappingName : futures.keySet()) {
            final PutRoleMappingResponse response = futures.get(mappingName).get();
            logger.info("Created native role-mapping {} : {}", mappingName, response.isCreated());
        }
    }

    @After
    public void cleanupSecurityIndex() throws Exception {
        super.deleteSecurityIndex();
    }

    @Override
    public Set<String> excludeTemplates() {
        Set<String> templates = Sets.newHashSet(super.excludeTemplates());
        templates.add(SecurityIndexManager.SECURITY_MAIN_TEMPLATE_7); // don't remove the security index template
        return templates;
    }

    private List<String> getRoleMappingContent(Function<RoleMappingEntry, String> contentFunction) {
        return getRoleMappingContent(contentFunction, AbstractAdLdapRealmTestCase.roleMappings);
    }

    private List<String> getRoleMappingContent(Function<RoleMappingEntry, String> contentFunction, List<RoleMappingEntry> mappings) {
        return mappings.stream()
                .map(contentFunction)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    protected final void configureFileRoleMappings(Settings.Builder builder, String realmType, List<RoleMappingEntry> mappings) {
        String content = getRoleMappingContent(RoleMappingEntry::getFileContent, mappings).stream().collect(Collectors.joining("\n"));
        Path nodeFiles = createTempDir();
        String file = writeFile(nodeFiles, "role_mapping.yml", content);
        builder.put("xpack.security.authc.realms." + realmType + ".external.files.role_mapping", file);
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
        IndexResponse indexResponse = client.prepareIndex(index).
                setSource(jsonBuilder()
                        .startObject()
                        .field("name", "value")
                        .endObject())
                .execute().actionGet();

        assertEquals("user " + user + " should have write access to index " + index,
                DocWriteResponse.Result.CREATED, indexResponse.getResult());

        refresh();

        GetResponse getResponse = client.prepareGet(index, indexResponse.getId())
                .get();

        assertThat("user " + user + " should have read access to index " + index, getResponse.getId(), equalTo(indexResponse.getId()));
    }

    protected void assertAccessDenied(String user, String index) throws IOException {
        try {
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, userHeader(user, PASSWORD)))
                    .prepareIndex(index).
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
        return UsernamePasswordToken.basicAuthHeaderValue(username, new SecureString(password.toCharArray()));
    }

    /**
     * Collects all the certificates that are normally trusted by the node ( contained in testnode.jks )
     */
    List<String> getNodeTrustedCertificates() {
        Path testnodeCert =
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        Path testnodeClientProfileCert =
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.crt");
        Path activedirCert =
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/active-directory-ca.crt");
        Path testclientCert =
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt");
        Path openldapCert =
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/openldap.crt");
        Path samba4Cert =
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/samba4.crt");
        return Arrays.asList(testnodeCert.toString(), testnodeClientProfileCert.toString(), activedirCert.toString(), testclientCert
            .toString(), openldapCert.toString(), samba4Cert.toString());
    }

    static class RoleMappingEntry {
        @Nullable
        public final String fileContent;
        @Nullable
        public final String nativeContent;

        RoleMappingEntry(@Nullable String fileContent, @Nullable String nativeContent) {
            this.fileContent = fileContent;
            this.nativeContent = nativeContent;
        }

        String getFileContent() {
            return fileContent;
        }

        String getNativeContent() {
            return nativeContent;
        }

        RoleMappingEntry pickEntry(Supplier<Boolean> shouldPickFileContent) {
            if (nativeContent == null) {
                return new RoleMappingEntry(fileContent, null);
            }
            if (fileContent == null) {
                return new RoleMappingEntry(null, nativeContent);
            }
            if (shouldPickFileContent.get()) {
                return new RoleMappingEntry(fileContent, null);
            } else {
                return new RoleMappingEntry(null, nativeContent);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final RoleMappingEntry that = (RoleMappingEntry) o;
            return Objects.equals(this.fileContent, that.fileContent)
                    && Objects.equals(this.nativeContent, that.nativeContent);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(fileContent);
            result = 31 * result + Objects.hashCode(nativeContent);
            return result;
        }
    }

    /**
     * Represents multiple possible configurations for active directory and ldap
     */
    enum RealmConfig {

        AD(false, AD_ROLE_MAPPING,
                Settings.builder()
                    .put(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL + ".domain_name", ActiveDirectorySessionFactoryTests.AD_DOMAIN)
                    .put(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL
                                + ".group_search.base_dn", "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                    .put(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL + ".group_search.scope", randomBoolean() ? SUB_TREE : ONE_LEVEL)
                    .put(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL + ".url", ActiveDirectorySessionFactoryTests.AD_LDAP_URL)
                    .put(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL + ".follow_referrals",
                                ActiveDirectorySessionFactoryTests.FOLLOW_REFERRALS)
                    .put(getFullSettingKey("external",ActiveDirectorySessionFactorySettings.AD_LDAP_PORT_SETTING), AD_LDAP_PORT)
                    .put(getFullSettingKey("external",ActiveDirectorySessionFactorySettings.AD_LDAPS_PORT_SETTING), AD_LDAPS_PORT)
                    .put(getFullSettingKey("external",ActiveDirectorySessionFactorySettings.AD_GC_LDAP_PORT_SETTING), AD_GC_LDAP_PORT)
                    .put(getFullSettingKey("external",ActiveDirectorySessionFactorySettings.AD_GC_LDAPS_PORT_SETTING), AD_GC_LDAPS_PORT)
                    .build(),
            "active_directory"),

        AD_LDAP_GROUPS_FROM_SEARCH(true, AD_ROLE_MAPPING,
                Settings.builder()
                    .put(XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL + ".url", ActiveDirectorySessionFactoryTests.AD_LDAP_URL)
                    .put(XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL
                                + ".group_search.base_dn", "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                    .put(XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL + ".group_search.scope", randomBoolean() ? SUB_TREE : ONE_LEVEL)
                    .putList(XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL + ".user_dn_templates",
                                "cn={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                    .put(XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL + ".follow_referrals",
                                ActiveDirectorySessionFactoryTests.FOLLOW_REFERRALS)
                    .build(),
            "ldap"),

        AD_LDAP_GROUPS_FROM_ATTRIBUTE(true, AD_ROLE_MAPPING,
                Settings.builder()
                    .put(XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL + ".url", ActiveDirectorySessionFactoryTests.AD_LDAP_URL)
                    .putList(XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL + ".user_dn_templates",
                                "cn={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                    .put(XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL + ".follow_referrals",
                                ActiveDirectorySessionFactoryTests.FOLLOW_REFERRALS)
                    .build(),
            "ldap");

        final String type;
        final boolean mapGroupsAsRoles;
        final boolean loginWithCommonName;
        private final RoleMappingEntry[] roleMappings;
        final Settings settings;

        RealmConfig(boolean loginWithCommonName, RoleMappingEntry[] roleMappings, Settings settings, String type) {
            this.settings = settings;
            this.loginWithCommonName = loginWithCommonName;
            this.roleMappings = roleMappings;
            this.mapGroupsAsRoles = randomBoolean();
            this.type = type;
        }

        public Settings buildSettings(List<String> certificateAuthorities) {
            return buildSettings(certificateAuthorities, randomInt());
        }


        protected Settings buildSettings(List<String> certificateAuthorities, int order) {
            Settings.Builder builder = Settings.builder()
                .put("xpack.security.authc.realms." + type + ".external.order", order)
                .put("xpack.security.authc.realms." + type + ".external.ssl.verification_mode", VerificationMode.CERTIFICATE)
                .put("xpack.security.authc.realms." + type + ".external.unmapped_groups_as_roles", mapGroupsAsRoles)
                .put(this.settings)
                .putList("xpack.security.authc.realms." + type + ".external.ssl.certificate_authorities", certificateAuthorities);
            return builder.build();
        }

        public List<RoleMappingEntry> selectRoleMappings(Supplier<Boolean> shouldPickFileContent) {
            // if mapGroupsAsRoles is turned on we use empty role mapping
            if (mapGroupsAsRoles) {
                return Collections.emptyList();
            } else {
                return Arrays.stream(this.roleMappings)
                        .map(e -> e.pickEntry(shouldPickFileContent))
                        .collect(Collectors.toList());
            }
        }
    }
}
