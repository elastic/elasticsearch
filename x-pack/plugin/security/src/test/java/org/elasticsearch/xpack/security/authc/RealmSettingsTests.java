/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.InternalRealmsSettings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class RealmSettingsTests extends ESTestCase {
    private static final List<String> CACHE_HASHING_ALGOS = Hasher.getAvailableAlgoCacheHash();

    public void testRealmWithBlankTypeDoesNotValidate() throws Exception {
        final Settings.Builder builder = baseSettings(false);
        assertErrorWithMessage("", "empty", "unknown setting [" + realmPrefix("", "empty"), realm("", "empty", builder).build());
    }

    public void testFileRealmWithAllSettingsValidatesSuccessfully() throws Exception {
        assertSuccess(fileRealm("file1"));
    }

    public void testFileRealmWithUnknownConfigurationDoesNotValidate() throws Exception {
        final Settings.Builder builder = realm("file", "file2", fileSettings().put("not-valid", randomInt()));
        assertErrorWithMessage("file", "file2", "unknown setting [" + realmPrefix("file", "file2") + "not-valid]", builder.build());
    }

    public void testNativeRealmWithAllSettingsValidatesSuccessfully() throws Exception {
        assertSuccess(nativeRealm("native1"));
    }

    public void testNativeRealmWithUnknownConfigurationDoesNotValidate() throws Exception {
        final Settings.Builder builder = realm("native", "native2", nativeSettings().put("not-valid", randomAlphaOfLength(10)));
        assertErrorWithMessage("native", "native2", "unknown setting [" + realmPrefix("native", "native2") + "not-valid]", builder.build());
    }

    public void testLdapRealmWithUserTemplatesAndGroupAttributesValidatesSuccessfully() throws Exception {
        assertSuccess(ldapRealm("ldap1", false, false));
    }

    public void testLdapRealmWithUserSearchAndGroupSearchValidatesSuccessfully() throws Exception {
        assertSuccess(ldapRealm("ldap2", true, true));
    }

    public void testActiveDirectoryRealmWithAllSettingsValidatesSuccessfully() throws Exception {
        assertSuccess(activeDirectoryRealm("ad1", true));
    }

    public void testPkiRealmWithCertificateAuthoritiesValidatesSuccessfully() throws Exception {
        assertSuccess(pkiRealm("pki1", false));
    }

    public void testPkiRealmWithTrustStoreValidatesSuccessfully() throws Exception {
        assertSuccess(pkiRealm("pki2", true));
    }

    public void testPkiRealmWithFullSslSettingsDoesNotValidate() throws Exception {
        final Settings.Builder realm = realm("pki", "pki3", configureSsl("", pkiSettings(true), true, true));
        assertError("pki", "pki3", realm.build());
    }

    public void testSettingsWithMultipleRealmsValidatesSuccessfully() throws Exception {
        final Settings settings = Settings.builder()
                .put(fileRealm("file1").build())
                .put(nativeRealm("native2").build())
                .put(ldapRealm("ldap3", true, false).build())
                .put(activeDirectoryRealm("ad4", false).build()) // don't load SSL twice
                .put(pkiRealm("pki5", false).build())
                .build();
        assertSuccess(settings);
    }

    public void testSettingsWithKeystoreOnlyRealmDoesNotValidate() throws Exception {
        final String securePasswordKey = RealmSettings.getFullSettingKey(
            new RealmConfig.RealmIdentifier("ldap", "ldap_1"), PoolingSessionFactorySettings.SECURE_BIND_PASSWORD);
        final Settings.Builder builder = Settings.builder()
            .put(ldapRealm("ldap-1", randomBoolean(), randomBoolean()).build());
        SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
            secureSettings.setString(securePasswordKey, "secret-password");
        });
        final Settings settings = builder.build();
        final SettingsException exception = expectThrows(SettingsException.class, () -> RealmSettings.getRealmSettings(settings));
        assertThat(exception.getMessage(), containsString("elasticsearch.keystore"));
        assertThat(exception.getMessage(), containsString("elasticsearch.yml"));
        assertThat(exception.getMessage(), containsString(securePasswordKey));
    }

    private Settings.Builder nativeRealm(String name) {
        return realm("native", name, nativeSettings());
    }

    private Settings.Builder nativeSettings() {
        return baseSettings(true);
    }

    private Settings.Builder fileRealm(String name) {
        return realm("file", name, fileSettings());
    }

    private Settings.Builder fileSettings() {
        return baseSettings(true);
    }

    private Settings.Builder ldapRealm(String name, boolean userSearch, boolean groupSearch) {
        return realm("ldap", name, ldapSettings(userSearch, groupSearch));
    }

    private Settings.Builder ldapSettings(boolean userSearch, boolean groupSearch) {
        final Settings.Builder builder = commonLdapSettings("ldap", true)
                .put("bind_dn", "elasticsearch")
                .put("follow_referrals", randomBoolean());

        SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
            secureSettings.setString("secure_bind_password", "t0p_s3cr3t");
        });

        if (userSearch) {
            builder.put("user_search.base_dn", "o=people, dc=example, dc=com");
            builder.put("user_search.scope", "sub_tree");
            builder.put("user_search.filter", "(" + randomAlphaOfLengthBetween(2, 5) + "={0})");
            builder.put("user_search.pool.enabled", randomBoolean());
            builder.put("user_search.pool.size", randomIntBetween(10, 100));
            builder.put("user_search.pool.initial_size", randomIntBetween(1, 10));
            builder.put("user_search.pool.health_check.enabled", randomBoolean());
            builder.put("user_search.pool.health_check.dn", randomAlphaOfLength(32));
            builder.put("user_search.pool.health_check.interval", randomPositiveTimeValue());
        } else {
            builder.putList("user_dn_templates",
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

    private Settings.Builder activeDirectoryRealm(String name, boolean configureSSL) {
        return realm("active_directory", name, activeDirectorySettings(configureSSL));
    }

    private Settings.Builder activeDirectorySettings(boolean configureSSL) {
        final Settings.Builder builder = commonLdapSettings("active_directory", configureSSL)
                .put("domain_name", "MEGACORP");
        builder.put("user_search.base_dn", "o=people, dc.example, dc.com");
        builder.put("user_search.scope", "sub_tree");
        builder.put("user_search.filter", randomAlphaOfLength(5) + "={0}");
        builder.put("group_search.base_dn", "o=groups, dc=example, dc=com");
        builder.put("group_search.scope", "one_level");
        return builder;
    }

    private Settings.Builder commonLdapSettings(String type, boolean configureSSL) {
        final Settings.Builder builder = baseSettings(true)
                .putList("url", "ldap://dir1.internal:9876", "ldap://dir2.internal:9876", "ldap://dir3.internal:9876")
                .put("load_balance.type", "round_robin")
                .put("load_balance.cache_ttl", randomTimeValue())
                .put("unmapped_groups_as_roles", randomBoolean())
                .put("files.role_mapping", "x-pack/" + randomAlphaOfLength(8) + ".yml")
                .put("timeout.tcp_connect", randomPositiveTimeValue())
                .put("timeout.response", randomPositiveTimeValue())
                .put("timeout.ldap_search", randomPositiveTimeValue());
        if (configureSSL) {
            configureSsl("ssl.", builder, randomBoolean(), randomBoolean());
        }
        return builder;
    }

    private Settings.Builder pkiRealm(String name, boolean useTrustStore) {
        return realm("pki", name, pkiSettings(useTrustStore));
    }

    private Settings.Builder pkiSettings(boolean useTrustStore) {
        final Settings.Builder builder = baseSettings(false)
                .put("username_pattern", "CN=\\D(\\d+)(?:,\\|$)")
                .put("files.role_mapping", "x-pack/" + randomAlphaOfLength(8) + ".yml");

        if (useTrustStore) {
            builder.put("truststore.path", randomAlphaOfLengthBetween(8, 32));
            SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
                secureSettings.setString("truststore.secure_password", randomAlphaOfLength(8));
            });
            builder.put("truststore.algorithm", randomAlphaOfLengthBetween(6, 10));
        } else {
            builder.putList("certificate_authorities", generateRandomStringArray(5, 32, false, false));
        }
        if (randomBoolean()) {
            builder.put("delegation.enabled", randomBoolean());
        }
        return builder;
    }

    private Settings.Builder configureSsl(String prefix, Settings.Builder builder, boolean useKeyStore, boolean useTrustStore) {
        if (useKeyStore) {
            builder.put(prefix + "keystore.path", "ssl/" + randomAlphaOfLength(5) + ".jks");
            SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
                secureSettings.setString(prefix + "keystore.secure_password", randomAlphaOfLength(8));
                secureSettings.setString(prefix + "keystore.secure_key_password", randomAlphaOfLength(8));
            });
        } else {
            builder.put(prefix + "key", "x-pack/ssl/" + randomAlphaOfLength(5) + ".key");
            SecuritySettingsSource.addSecureSettings(builder, secureSettings ->
                    secureSettings.setString(prefix + "secure_key_passphrase", randomAlphaOfLength(32)));

            builder.put(prefix + "certificate", "ssl/" + randomAlphaOfLength(5) + ".cert");
        }

        if (useTrustStore) {
            builder.put(prefix + "truststore.path", "x-pack/ssl/" + randomAlphaOfLength(5) + ".jts");
            SecuritySettingsSource.addSecureSettings(builder, secureSettings ->
                    secureSettings.setString(prefix + "truststore.secure_password", randomAlphaOfLength(8)));
        } else {
            builder.put(prefix + "certificate_authorities", "ssl/" + randomAlphaOfLength(8) + ".ca");
        }

        builder.put(prefix + "verification_mode", "full");
        builder.putList(prefix + "supported_protocols", randomSubsetOf(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS));
        builder.putList(prefix + "cipher_suites", randomSubsetOf(XPackSettings.DEFAULT_CIPHERS));

        return builder;
    }

    private Settings.Builder baseSettings(boolean withCacheSettings) {
        final Settings.Builder builder = Settings.builder()
                .put("order", randomInt())
                .put("enabled", true);
        if (withCacheSettings) {
            builder.put("cache.ttl", randomPositiveTimeValue())
                .put("cache.max_users", randomIntBetween(1_000, 1_000_000))
                .put("cache.hash_algo", randomFrom(CACHE_HASHING_ALGOS));
        }
        return builder;
    }

    private Settings.Builder realm(String type, String name, Settings.Builder settings) {
        final String prefix = realmPrefix(type, name);
        final MockSecureSettings secureSettings = normaliseSecureSettingPrefix(prefix, settings.getSecureSettings());
        final Settings.Builder builder = Settings.builder().put(settings.normalizePrefix(prefix).build(), false);
        if (secureSettings != null) {
            builder.setSecureSettings(secureSettings);
        }
        return builder;
    }

    private MockSecureSettings normaliseSecureSettingPrefix(String prefix, SecureSettings settings) {
        if (settings == null) {
            return null;
        }
        if (settings instanceof MockSecureSettings) {
            final MockSecureSettings source = (MockSecureSettings) settings;
            final MockSecureSettings target = new MockSecureSettings();
            for (String key : settings.getSettingNames()) {
                target.setString(prefix + key, source.getString(key).toString());
            }
            return target;
        } else {
            throw new IllegalArgumentException("Source settings " + settings.getClass() + " is not a " + MockSecureSettings.class);
        }
    }

    private String realmPrefix(String type, String name) {
        return RealmSettings.PREFIX + type + "." + name + ".";
    }

    private void assertSuccess(Settings.Builder builder) {
        assertSuccess(builder.build());
    }

    private void assertSuccess(Settings settings) {
        try {
            validate(settings);
        } catch (RuntimeException e) {
            fail("Settings do not validate: " + e);
        }
        assertThat(RealmSettings.getRealmSettings(settings), Matchers.not(Matchers.anEmptyMap()));
    }

    private void assertErrorWithMessage(String realmType, String realmName, String message, Settings settings) {
        final IllegalArgumentException exception = assertError(realmType, realmName, settings);
        assertThat(exception.getMessage(), containsString(message));
    }

    private IllegalArgumentException assertError(String realmType, String realmName, Settings settings) {
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> validate(settings)
        );
        assertThat(exception.getMessage(), containsString(realmPrefix(realmType, realmName)));
        return exception;
    }

    private void validate(Settings settings) {
        final Set<Setting<?>> settingsSet = new HashSet<>(InternalRealmsSettings.getSettings());
        final AbstractScopedSettings validator = new AbstractScopedSettings(settings, settingsSet, Collections.emptySet(),
            Setting.Property.NodeScope) {
        };
        validator.validate(settings, false);
    }
}
