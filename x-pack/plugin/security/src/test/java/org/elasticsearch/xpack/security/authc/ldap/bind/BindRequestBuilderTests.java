/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.ldap.bind;

import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.GSSAPIBindRequest;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SimpleBindRequest;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig.RealmIdentifier;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;

import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosRealmTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.BIND_DN;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.SASL_GSSAPI_KEYTAB_PATH;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.SASL_GSSAPI_USE_KEYTAB;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.SECURE_BIND_PASSWORD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class BindRequestBuilderTests extends ESTestCase {
    private RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "ldap-1");

    public void testForInvalidBindModeExceptionIsThrown() throws LDAPException {
        final String invalidBindMode = randomAlphaOfLength(7);
        final Settings realmSettings = Settings.builder()
                .put("path.home", createTempDir())
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_USE_KEYTAB), "true")
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_PRINCIPAL), "bind-user@DEV.LOCAL")
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.BIND_MODE), invalidBindMode).build();
        final Environment env = TestEnvironment.newEnvironment(realmSettings);
        final RealmConfig realmConfig = new RealmConfig(realmId, realmSettings, env, new ThreadContext(realmSettings));
        final BindRequestBuilder builder = new BindRequestBuilder(realmConfig, c -> c.getSetting(BIND_DN, () -> null));

        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> builder.build());
        assertThat(iae.getMessage(),
                equalTo("only [simple] and [sasl_gssapi] bind mode are allowed, ["+invalidBindMode+"] is invalid"));
    }

    public void testGSSAPIBindRequestWithNoKeytabPathFails() throws LDAPException {
        final Settings realmSettings = Settings.builder()
                .put("path.home", createTempDir())
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_USE_KEYTAB), "true")
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_PRINCIPAL), "bind-user@DEV.LOCAL")
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.BIND_MODE), "sasl_gssapi").build();
        final Environment env = TestEnvironment.newEnvironment(realmSettings);
        final RealmConfig realmConfig = new RealmConfig(realmId, realmSettings, env, new ThreadContext(realmSettings));
        final BindRequestBuilder builder = new BindRequestBuilder(realmConfig, c -> c.getSetting(BIND_DN, () -> null));

        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> builder.build());
        assertThat(iae.getMessage(),
                equalTo("setting [" + RealmSettings.getFullSettingKey(realmConfig, SASL_GSSAPI_USE_KEYTAB)
                        + "] is enabled but keytab path [" + RealmSettings.getFullSettingKey(realmConfig, SASL_GSSAPI_KEYTAB_PATH)
                        + "] has not been configured"));
    }

    public void testGSSAPIBindRequestWithBothKeyTabAndPasswordFails() throws LDAPException, IOException {
        final Path dir = createTempDir();
        Path configDir = dir.resolve("config");
        if (Files.exists(configDir) == false) {
            configDir = Files.createDirectory(configDir);
        }
        final Settings.Builder settingsBuilder = Settings.builder().put("path.home", dir)
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.BIND_MODE), "sasl_gssapi")
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_PRINCIPAL), "bind-user@DEV.LOCAL");
        KerberosRealmTestCase.writeKeyTab(dir.resolve("config").resolve("bind_principal.keytab"), null);
        settingsBuilder.put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_USE_KEYTAB), Boolean.toString(true))
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_KEYTAB_PATH), "bind_principal.keytab")
                .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, realmId, "passwd"));

        final Settings realmSettings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(realmSettings);
        final RealmConfig realmConfig = new RealmConfig(realmId, realmSettings, env, new ThreadContext(realmSettings));
        final BindRequestBuilder builder = new BindRequestBuilder(realmConfig, c -> c.getSetting(BIND_DN, () -> null));

        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> builder.build());
        assertThat(iae.getMessage(),
                equalTo("You cannot specify both [" + RealmSettings.getFullSettingKey(realmConfig, SASL_GSSAPI_USE_KEYTAB) + "] and (["
                        + RealmSettings.getFullSettingKey(realmConfig, LEGACY_BIND_PASSWORD) + "] or ["
                        + RealmSettings.getFullSettingKey(realmConfig, SECURE_BIND_PASSWORD) + "])"));
    }

    public void testGSSAPIBindRequestWithNoPasswordFails() throws LDAPException, IOException {
        final Path dir = createTempDir();
        Path configDir = dir.resolve("config");
        if (Files.exists(configDir) == false) {
            configDir = Files.createDirectory(configDir);
        }
        final Settings.Builder settingsBuilder = Settings.builder().put("path.home", dir)
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.BIND_MODE), "sasl_gssapi")
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_PRINCIPAL), "bind-user@DEV.LOCAL");
        KerberosRealmTestCase.writeKeyTab(dir.resolve("config").resolve("bind_principal.keytab"), null);
        settingsBuilder.put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_USE_KEYTAB), Boolean.toString(false));

        final Settings realmSettings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(realmSettings);
        final RealmConfig realmConfig = new RealmConfig(realmId, realmSettings, env, new ThreadContext(realmSettings));
        final BindRequestBuilder builder = new BindRequestBuilder(realmConfig, c -> c.getSetting(BIND_DN, () -> null));

        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> builder.build());
        assertThat(iae.getMessage(), equalTo("Either keytab [" + RealmSettings.getFullSettingKey(realmConfig, SASL_GSSAPI_KEYTAB_PATH)
                + "] or principal password " + RealmSettings.getFullSettingKey(realmConfig, SECURE_BIND_PASSWORD) + " must be configured"));
    }

    public void testGSSAPIBindRequest() throws LDAPException, IOException {
        final Path dir = createTempDir();
        Path configDir = dir.resolve("config");
        if (Files.exists(configDir) == false) {
            configDir = Files.createDirectory(configDir);
        }
        final Settings.Builder settingsBuilder = Settings.builder().put("path.home", dir)
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.BIND_MODE), "sasl_gssapi")
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_PRINCIPAL), "bind-user@DEV.LOCAL");
        final boolean useKeyTab = randomBoolean();
        if (useKeyTab) {
            KerberosRealmTestCase.writeKeyTab(dir.resolve("config").resolve("bind_principal.keytab"), null);
            settingsBuilder
                    .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_USE_KEYTAB), Boolean.toString(useKeyTab))
                    .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_KEYTAB_PATH), "bind_principal.keytab");
        } else {
            settingsBuilder.setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, realmId, "passwd"));
        }

        final Settings realmSettings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(realmSettings);
        final RealmConfig realmConfig = new RealmConfig(realmId, realmSettings, env, new ThreadContext(realmSettings));
        final BindRequestBuilder builder = new BindRequestBuilder(realmConfig, c -> c.getSetting(BIND_DN, () -> null));
        final BindRequest bindRequest = builder.build();
        assertThat(bindRequest, instanceOf(GSSAPIBindRequest.class));
        if (useKeyTab) {
            assertThat(((GSSAPIBindRequest) bindRequest).getKeyTabPath(),
                    is(dir.resolve("config").resolve("bind_principal.keytab").toString()));
        } else {
            assertThat(((GSSAPIBindRequest) bindRequest).getPasswordString(), is("passwd"));
        }
    }

    public void testSimpleBindRequest() throws LDAPException {
        final Settings realmSettings = Settings.builder()
                .put("path.home", createTempDir())
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.BIND_DN), "bind-user@DEV.LOCAL")
                .put(getFullSettingKey(realmId, PoolingSessionFactorySettings.BIND_MODE), "simple")
                .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, realmId, "passwd"))
                .build();
        final Environment env = TestEnvironment.newEnvironment(realmSettings);
        final RealmConfig realmConfig = new RealmConfig(realmId, realmSettings, env,
                new ThreadContext(realmSettings));
        final BindRequestBuilder builder = new BindRequestBuilder(realmConfig, c -> c.getSetting(BIND_DN, () -> null));
        final BindRequest bindRequest = builder.build();
        assertThat(bindRequest, instanceOf(SimpleBindRequest.class));
    }

    private SecureSettings secureSettings(Function<String, Setting.AffixSetting<SecureString>> settingFactory,
                                          RealmConfig.RealmIdentifier identifier, String value) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(getFullSettingKey(identifier, settingFactory), value);
        return secureSettings;
    }
}
