/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.shield.audit.index.IndexAuditTrail;
import org.elasticsearch.shield.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.shield.support.AbstractShieldModule;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.shield.Security.featureEnabledSetting;
import static org.elasticsearch.shield.Security.setting;

/**
 *
 */
public class AuditTrailModule extends AbstractShieldModule.Node {

    public static final Setting<Boolean> ENABLED_SETTING =
            Setting.boolSetting(featureEnabledSetting("audit"), false, Property.NodeScope);
    public static final Setting<List<String>> OUTPUTS_SETTING =
            Setting.listSetting(setting("audit.outputs"),
                    s -> s.getAsMap().containsKey(setting("audit.outputs")) ?
                            Collections.emptyList() : Collections.singletonList(LoggingAuditTrail.NAME),
                    Function.identity(), Property.NodeScope);
    private final boolean enabled;

    public AuditTrailModule(Settings settings) {
        super(settings);
        enabled = ENABLED_SETTING.get(settings);
    }

    @Override
    protected void configureNode() {
        if (!enabled) {
            bind(AuditTrail.class).toInstance(AuditTrail.NOOP);
            return;
        }
        List<String> outputs = OUTPUTS_SETTING.get(settings);
        if (outputs.isEmpty()) {
            bind(AuditTrail.class).toInstance(AuditTrail.NOOP);
            return;
        }
        bind(AuditTrail.class).to(AuditTrailService.class).asEagerSingleton();
        Multibinder<AuditTrail> binder = Multibinder.newSetBinder(binder(), AuditTrail.class);

        Set<String> uniqueOutputs = Sets.newHashSet(outputs);
        for (String output : uniqueOutputs) {
            switch (output) {
                case LoggingAuditTrail.NAME:
                    binder.addBinding().to(LoggingAuditTrail.class);
                    bind(LoggingAuditTrail.class).asEagerSingleton();
                    break;
                case IndexAuditTrail.NAME:
                    binder.addBinding().to(IndexAuditTrail.class);
                    bind(IndexAuditTrail.class).asEagerSingleton();
                    break;
                default:
                    throw new ElasticsearchException("unknown audit trail output [" + output + "]");
            }
        }
    }

    public static boolean auditingEnabled(Settings settings) {
        return ENABLED_SETTING.get(settings);
    }

    public static boolean indexAuditLoggingEnabled(Settings settings) {
        if (auditingEnabled(settings)) {
            List<String> outputs = OUTPUTS_SETTING.get(settings);
            for (String output : outputs) {
                if (output.equals(IndexAuditTrail.NAME)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean fileAuditLoggingEnabled(Settings settings) {
        if (auditingEnabled(settings)) {
            List<String> outputs = OUTPUTS_SETTING.get(settings);
            for (String output : outputs) {
                if (output.equals(LoggingAuditTrail.NAME)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(ENABLED_SETTING);
        settingsModule.registerSetting(OUTPUTS_SETTING);
        LoggingAuditTrail.registerSettings(settingsModule);
        IndexAuditTrail.registerSettings(settingsModule);
    }
}
