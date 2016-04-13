/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.audit.AuditTrailModule;
import org.elasticsearch.shield.audit.index.IndexAuditTrail;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackPlugin;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.not;

public class SecuritySettingsTests extends ESTestCase {

    private static final String TRIBE_T1_SHIELD_ENABLED = "tribe.t1." + Security.enabledSetting();
    private static final String TRIBE_T2_SHIELD_ENABLED = "tribe.t2." + Security.enabledSetting();

    public void testShieldIsMandatoryOnTribes() {
        Settings settings = Settings.builder().put("tribe.t1.cluster.name", "non_existing")
                .put("tribe.t2.cluster.name", "non_existing").build();

        Security security = new Security(settings);

        Settings additionalSettings = security.additionalSettings();


        assertThat(additionalSettings.getAsArray("tribe.t1.plugin.mandatory", null), arrayContaining(XPackPlugin.NAME));
        assertThat(additionalSettings.getAsArray("tribe.t2.plugin.mandatory", null), arrayContaining(XPackPlugin.NAME));
    }

    public void testAdditionalMandatoryPluginsOnTribes() {
        Settings settings = Settings.builder().put("tribe.t1.cluster.name", "non_existing")
                .putArray("tribe.t1.plugin.mandatory", "test_plugin").build();

        Security security = new Security(settings);

        //simulate what PluginsService#updatedSettings does to make sure we don't override existing mandatory plugins
        try {
            Settings.builder().put(settings).put(security.additionalSettings()).build();
            fail("shield cannot change the value of a setting that is already defined, so a exception should be thrown");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString(XPackPlugin.NAME));
            assertThat(e.getMessage(), containsString("plugin.mandatory"));
        }
    }

    public void testMandatoryPluginsOnTribesShieldAlreadyMandatory() {
        Settings settings = Settings.builder().put("tribe.t1.cluster.name", "non_existing")
                .putArray("tribe.t1.plugin.mandatory", "test_plugin", XPackPlugin.NAME).build();

        Security security = new Security(settings);

        //simulate what PluginsService#updatedSettings does to make sure we don't override existing mandatory plugins
        Settings finalSettings = Settings.builder().put(settings).put(security.additionalSettings()).build();

        String[] finalMandatoryPlugins = finalSettings.getAsArray("tribe.t1.plugin.mandatory", null);
        assertThat(finalMandatoryPlugins, notNullValue());
        assertThat(finalMandatoryPlugins.length, equalTo(2));
        assertThat(finalMandatoryPlugins[0], equalTo("test_plugin"));
        assertThat(finalMandatoryPlugins[1], equalTo(XPackPlugin.NAME));
    }

    public void testShieldIsEnabledByDefaultOnTribes() {
        Settings settings = Settings.builder().put("tribe.t1.cluster.name", "non_existing")
                .put("tribe.t2.cluster.name", "non_existing").build();

        Security security = new Security(settings);

        Settings additionalSettings = security.additionalSettings();

        assertThat(additionalSettings.getAsBoolean(TRIBE_T1_SHIELD_ENABLED, null), equalTo(true));
        assertThat(additionalSettings.getAsBoolean(TRIBE_T2_SHIELD_ENABLED, null), equalTo(true));
    }

    public void testShieldDisabledOnATribe() {
        Settings settings = Settings.builder().put("tribe.t1.cluster.name", "non_existing")
                .put(TRIBE_T1_SHIELD_ENABLED, false)
                .put("tribe.t2.cluster.name", "non_existing").build();

        Security security = new Security(settings);

        try {
            security.additionalSettings();
            fail("shield cannot change the value of a setting that is already defined, so a exception should be thrown");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString(TRIBE_T1_SHIELD_ENABLED));
        }
    }

    public void testShieldDisabledOnTribesShieldAlreadyMandatory() {
        Settings settings = Settings.builder().put("tribe.t1.cluster.name", "non_existing")
                .put(TRIBE_T1_SHIELD_ENABLED, false)
                .put("tribe.t2.cluster.name", "non_existing")
                .putArray("tribe.t1.plugin.mandatory", "test_plugin", XPackPlugin.NAME).build();

        Security security = new Security(settings);

        try {
            security.additionalSettings();
            fail("shield cannot change the value of a setting that is already defined, so a exception should be thrown");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString(TRIBE_T1_SHIELD_ENABLED));
        }
    }

    public void testShieldSettingsCopiedForTribeNodes() {
        Settings settings = Settings.builder()
                .put("tribe.t1.cluster.name", "non_existing")
                .put("tribe.t2.cluster.name", "non_existing")
                .put("xpack.security.foo", "bar")
                .put("xpack.security.bar", "foo")
                .putArray("xpack.security.something.else.here", new String[] { "foo", "bar" })
                .build();

        Security security = new Security(settings);
        Settings additionalSettings = security.additionalSettings();

        assertThat(additionalSettings.get("xpack.security.foo"), nullValue());
        assertThat(additionalSettings.get("xpack.security.bar"), nullValue());
        assertThat(additionalSettings.getAsArray("xpack.security.something.else.here"), is(Strings.EMPTY_ARRAY));
        assertThat(additionalSettings.get("tribe.t1.xpack.security.foo"), is("bar"));
        assertThat(additionalSettings.get("tribe.t1.xpack.security.bar"), is("foo"));
        assertThat(additionalSettings.getAsArray("tribe.t1.xpack.security.something.else.here"), arrayContaining("foo", "bar"));
        assertThat(additionalSettings.get("tribe.t2.xpack.security.foo"), is("bar"));
        assertThat(additionalSettings.get("tribe.t2.xpack.security.bar"), is("foo"));
        assertThat(additionalSettings.getAsArray("tribe.t2.xpack.security.something.else.here"), arrayContaining("foo", "bar"));
    }

    public void testValidAutoCreateIndex() {
        Security.validateAutoCreateIndex(Settings.EMPTY);
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", true).build());

        try {
            Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", false).build());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(ShieldTemplateService.SECURITY_INDEX_NAME));
            assertThat(e.getMessage(), not(containsString(IndexAuditTrail.INDEX_NAME_PREFIX)));
        }

        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".security").build());
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", "*s*").build());
        Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".s*").build());

        try {
            Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", "foo").build());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(ShieldTemplateService.SECURITY_INDEX_NAME));
            assertThat(e.getMessage(), not(containsString(IndexAuditTrail.INDEX_NAME_PREFIX)));
        }

        try {
            Security.validateAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".shield_audit_log*").build());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(ShieldTemplateService.SECURITY_INDEX_NAME));
        }

        Security.validateAutoCreateIndex(Settings.builder()
                        .put("action.auto_create_index", ".security")
                        .put(AuditTrailModule.ENABLED_SETTING.getKey(), true)
                        .build());

        try {
            Security.validateAutoCreateIndex(Settings.builder()
                    .put("action.auto_create_index", ".security")
                    .put(AuditTrailModule.ENABLED_SETTING.getKey(), true)
                    .put(AuditTrailModule.OUTPUTS_SETTING.getKey(), randomFrom("index", "logfile,index"))
                    .build());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(ShieldTemplateService.SECURITY_INDEX_NAME));
            assertThat(e.getMessage(), containsString(IndexAuditTrail.INDEX_NAME_PREFIX));
        }

        Security.validateAutoCreateIndex(Settings.builder()
                .put("action.auto_create_index", ".shield_audit_log*,.security")
                .put(AuditTrailModule.ENABLED_SETTING.getKey(), true)
                .put(AuditTrailModule.OUTPUTS_SETTING.getKey(), randomFrom("index", "logfile,index"))
                .build());
    }
}
