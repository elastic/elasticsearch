/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.extensions.XPackExtension;
import org.elasticsearch.xpack.security.authc.support.Hasher;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class RealmSettingsTests extends ESTestCase {

    private static final List<String> HASH_ALGOS = Arrays.stream(Hasher.values()).map(Hasher::name).collect(Collectors.toList());

    public void testRealmWithoutTypeDoesNotValidate() throws Exception {
        final Settings.Builder builder = baseSettings("x", false);
        builder.remove("type");
        assertErrorWithMessage("empty1", "missing realm type", realm("empty1", builder).build());
    }

    public void testRealmWithBlankTypeDoesNotValidate() throws Exception {
        final Settings.Builder builder = baseSettings("", false);
        assertErrorWithMessage("empty2", "missing realm type", realm("empty2", builder).build());
    }

    /**
     * This test exists because (in 5.x), we want to be backwards compatible and accept custom realms that
     * have not been updated to explicitly declare their settings.
     *
     * @see XPackExtension#getRealmSettings()
     */
    public void testRealmWithUnknownTypeAcceptsAllSettings() throws Exception {
        final Settings.Builder settings = baseSettings("tam", true)
                .put("ip", "8.6.75.309")
                .put(randomAlphaOfLengthBetween(4, 8), randomTimeValue());
        assertSuccess(realm("tam", settings));
    }

    public void testFileRealmWithAllSettingsValidatesSuccessfully() throws Exception {
        assertSuccess(fileRealm("file1"));
    }

    public void testFileRealmWithUnknownConfigurationDoesNotValidate() throws Exception {
        final Settings.Builder builder = realm("file2", fileSettings().put("not-valid", randomInt()));
        assertErrorWithCause("file2", "unknown setting [not-valid]", builder.build());
    }

    public void testNativeRealmWithAllSettingsValidatesSuccessfully() throws Exception {
        assertSuccess(nativeRealm("native1"));
    }

    public void testNativeRealmWithUnknownConfigurationDoesNotValidate() throws Exception {
        final Settings.Builder builder = realm("native2", nativeSettings().put("not-valid", randomAlphaOfLength(10)));
        assertErrorWithCause("native2", "unknown setting [not-valid]", builder.build());
    }

    public void testLdapRealmWithUserTemplatesAndGroupAttributesValidatesSuccessfully() throws Exception {
        assertSuccess(ldapRealm("ldap1", false, false));
    }

    public void testLdapRealmWithUserSearchAndGroupSearchValidatesSuccessfully() throws Exception {
        assertSuccess(ldapRealm("ldap2", true, true));
    }

    public void testActiveDirectoryRealmWithAllSettingsValidatesSuccessfully() throws Exception {
        assertSuccess(activeDirectoryRealm("ad1"));
    }

    public void testPkiRealmWithCertificateAuthoritiesValidatesSuccessfully() throws Exception {
        assertSuccess(pkiRealm("pki1", false));
    }

    public void testPkiRealmWithTrustStoreValidatesSuccessfully() throws Exception {
        assertSuccess(pkiRealm("pki2", true));
    }

    public void testPkiRealmWithFullSslSettingsDoesNotValidate() throws Exception {
        final Settings.Builder realm = realm("pki3", configureSsl("", pkiSettings(true), true, true));
        assertError("pki3", realm.build());
    }

    public void testSettingsWithMultipleRealmsValidatesSuccessfully() throws Exception {
        final Settings settings = Settings.builder()
                .put(fileRealm("file1").build())
                .put(nativeRealm("native2").build())
                .put(ldapRealm("ldap3", true, false).build())
                .put(activeDirectoryRealm("ad4").build())
                .put(pkiRealm("pki5", false).build())
                .build();
        assertSuccess(settings);
    }

    private Settings.Builder nativeRealm(String name) {
        return realm(name, nativeSettings());
    }

    private Settings.Builder nativeSettings() {
        return baseSettings("native", true);
    }

    private Settings.Builder fileRealm(String name) {
        return realm(name, fileSettings());
    }

    private Settings.Builder fileSettings() {
        return baseSettings("file", true);
    }

    private Settings.Builder ldapRealm(String name, boolean userSearch, boolean groupSearch) {
        return realm(name, ldapSettings(userSearch, groupSearch));
    }

    private Settings.Builder ldapSettings(boolean userSearch, boolean groupSearch) {
        final Settings.Builder builder = commonLdapSettings("ldap")
                .put("bind_dn", "elasticsearch")
                .put("bind_password", "t0p_s3cr3t")
                .put("follow_referrals", randomBoolean());

        if (userSearch) {
            builder.put("user_search.base_dn", "o=people, dc=example, dc=com");
            builder.put("user_search.scope", "sub_tree");
            builder.put("user_search.attribute", randomAlphaOfLengthBetween(2, 5));
            builder.put("user_search.pool.enabled", randomBoolean());
            builder.put("user_search.pool.size", randomIntBetween(10, 100));
            builder.put("user_search.pool.initial_size", randomIntBetween(1, 10));
            builder.put("user_search.pool.health_check.enabled", randomBoolean());
            builder.put("user_search.pool.health_check.dn", randomAlphaOfLength(32));
            builder.put("user_search.pool.health_check.interval", randomPositiveTimeValue());
        } else {
            builder.putArray("user_dn_templates",
                    "cn={0}, ou=staff, o=people, dc=example, dc=com",
                    "cn={0}, ou=visitors, o=people, dc=example, dc=com");
        }

        if (groupSearch) {
            builder.put("group_search.base_dn", "o=groups, dc=example, dc=com");
            builder.put("group_search.scope", "one_level");
            builder.put("group_search.filter", "userGroup");
            builder.put("group_search.user_attribute", "uid");
        } else {
            builder.put("user_group_attribute", randomAlphaOfLength(8));
        }
        return builder;
    }

    private Settings.Builder activeDirectoryRealm(String name) {
        return realm(name, activeDirectorySettings());
    }

    private Settings.Builder activeDirectorySettings() {
        final Settings.Builder builder = commonLdapSettings("active_directory")
                .put("domain_name", "MEGACORP");
        builder.put("user_search.base_dn", "o=people, dc.example, dc.com");
        builder.put("user_search.scope", "sub_tree");
        builder.put("user_search.filter", randomAlphaOfLength(5) + "={0}");
        builder.put("group_search.base_dn", "o=groups, dc=example, dc=com");
        builder.put("group_search.scope", "one_level");
        return builder;
    }

    private Settings.Builder commonLdapSettings(String type) {
        final Settings.Builder builder = baseSettings(type, true)
                .putArray("url", "ldap://dir1.internal:9876", "ldap://dir2.internal:9876", "ldap://dir3.internal:9876")
                .put("load_balance.type", "round_robin")
                .put("load_balance.cache_ttl", randomTimeValue())
                .put("unmapped_groups_as_roles", randomBoolean())
                .put("files.role_mapping", "x-pack/" + randomAlphaOfLength(8) + ".yml")
                .put("timeout.tcp_connect", randomPositiveTimeValue())
                .put("timeout.tcp_read", randomPositiveTimeValue())
                .put("timeout.ldap_search", randomPositiveTimeValue());
        configureSsl("ssl.", builder, randomBoolean(), randomBoolean());
        return builder;
    }

    private Settings.Builder pkiRealm(String name, boolean useTrustStore) {
        return realm(name, pkiSettings(useTrustStore));
    }

    private Settings.Builder pkiSettings(boolean useTrustStore) {
        final Settings.Builder builder = baseSettings("pki", false)
                .put("username_pattern", "CN=\\D(\\d+)(?:,\\|$)")
                .put("files.role_mapping", "x-pack/" + randomAlphaOfLength(8) + ".yml");

        if (useTrustStore) {
            builder.put("truststore.path", randomAlphaOfLengthBetween(8, 32));
            builder.put("truststore.password", randomAlphaOfLengthBetween(4, 12));
            builder.put("truststore.algorithm", randomAlphaOfLengthBetween(6, 10));
        } else {
            builder.putArray("certificate_authorities", generateRandomStringArray(5, 32, false, false));
        }
        return builder;
    }

    private Settings.Builder configureSsl(String prefix, Settings.Builder builder, boolean useKeyStore, boolean useTrustStore) {
        if (useKeyStore) {
            builder.put(prefix + "keystore.path", "x-pack/ssl/" + randomAlphaOfLength(5) + ".jks");
            builder.put(prefix + "keystore.password", randomAlphaOfLength(8));
            builder.put(prefix + "keystore.key_password", randomAlphaOfLength(8));
        } else {
            builder.put(prefix + "key", "x-pack/ssl/" + randomAlphaOfLength(5) + ".key");
            builder.put(prefix + "key_passphrase", randomAlphaOfLength(32));
            builder.put(prefix + "certificate", "x-pack/ssl/" + randomAlphaOfLength(5) + ".cert");
        }

        if (useTrustStore) {
            builder.put(prefix + "truststore.path", "x-pack/ssl/" + randomAlphaOfLength(5) + ".jts");
            builder.put(prefix + "truststore.password", randomAlphaOfLength(8));
        } else {
            builder.put(prefix + "certificate_authorities", "x-pack/ssl/" + randomAlphaOfLength(8) + ".ca");
        }

        builder.put(prefix + "verification_mode", "full");
        builder.putArray(prefix + "supported_protocols", randomSubsetOf(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS));
        builder.putArray(prefix + "cipher_suites", randomSubsetOf(XPackSettings.DEFAULT_CIPHERS));

        return builder;
    }

    private Settings.Builder baseSettings(String type, boolean withCacheSettings) {
        final Settings.Builder builder = Settings.builder()
                .put("type", type)
                .put("order", randomInt())
                .put("enabled", true);
        if (withCacheSettings) {
            builder.put("cache.ttl", randomPositiveTimeValue())
                    .put("cache.max_users", randomIntBetween(1_000, 1_000_000))
                    .put("cache.hash_algo", randomFrom(HASH_ALGOS));
        }
        return builder;
    }

    private Settings.Builder realm(String name, Settings.Builder settings) {
        return settings.normalizePrefix(realmPrefix(name));
    }

    private String realmPrefix(String name) {
        return RealmSettings.PREFIX + name + ".";
    }

    private void assertSuccess(Settings.Builder builder) {
        assertSuccess(builder.build());
    }

    private void assertSuccess(Settings settings) {
        assertThat(group().get(settings), notNullValue());
    }

    private void assertErrorWithCause(String realmName, String message, Settings settings) {
        final IllegalArgumentException exception = assertError(realmName, settings);
        assertThat(exception.getCause(), notNullValue());
        assertThat(exception.getCause().getMessage(), containsString(message));
    }

    private void assertErrorWithMessage(String realmName, String message, Settings settings) {
        final IllegalArgumentException exception = assertError(realmName, settings);
        assertThat(exception.getMessage(), containsString(message));
    }

    private IllegalArgumentException assertError(String realmName, Settings settings) {
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> group().get(settings)
        );
        assertThat(exception.getMessage(), containsString(realmPrefix(realmName)));
        return exception;
    }

    private Setting<?> group() {
        final List<Setting<?>> list = new ArrayList<>();
        final List<XPackExtension> noExtensions = Collections.emptyList();
        RealmSettings.addSettings(list, noExtensions);
        assertThat(list, hasSize(1));
        return list.get(0);
    }
}