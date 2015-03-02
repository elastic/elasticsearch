/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.cluster.settings.ClusterDynamicSettingsModule;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.shield.authc.Realms;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.license.LicenseService;
import org.elasticsearch.shield.signature.InternalSignatureService;
import org.elasticsearch.shield.transport.filter.IPFilter;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class ShieldPlugin extends AbstractPlugin {

    public static final String NAME = "shield";

    public static final String ENABLED_SETTING_NAME = NAME + ".enabled";

    private final Settings settings;
    private final boolean enabled;
    private final boolean clientMode;

    public ShieldPlugin(Settings settings) {
        this.settings = settings;
        this.enabled = shieldEnabled(settings);
        this.clientMode = clientMode(settings);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Elasticsearch Shield (security)";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        return enabled ?
                ImmutableList.<Class<? extends Module>>of(ShieldModule.class) :
                ImmutableList.<Class<? extends Module>>of(ShieldDisabledModule.class);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        return enabled && !clientMode ?
                ImmutableList.<Class<? extends LifecycleComponent>>of(LicenseService.class, FileRolesStore.class, Realms.class, InternalSignatureService.class, IPFilter.class) :
                ImmutableList.<Class<? extends LifecycleComponent>>of();
    }

    @Override
    public Settings additionalSettings() {
        if (!enabled) {
            return ImmutableSettings.EMPTY;
        }

        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        addUserSettings(settingsBuilder);
        addTribeSettings(settingsBuilder);
        return settingsBuilder.build();
    }

    public void onModule(ClusterDynamicSettingsModule clusterDynamicSettingsModule) {
        clusterDynamicSettingsModule.addDynamicSettings("shield.transport.filter.*", "shield.http.filter.*", "transport.profiles.*", IPFilter.IP_FILTER_ENABLED_SETTING, IPFilter.IP_FILTER_ENABLED_HTTP_SETTING);
    }

    private void addUserSettings(ImmutableSettings.Builder settingsBuilder) {
        String authHeaderSettingName = Headers.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER;
        if (settings.get(authHeaderSettingName) != null) {
            return;
        }
        String userSetting = settings.get("shield.user");
        if (userSetting == null) {
            return;
        }
        int i = userSetting.indexOf(":");
        if (i < 0 || i == userSetting.length() - 1) {
            throw new ShieldSettingsException("invalid [shield.user] settings. must be in the form of \"<username>:<password>\"");
        }
        String username = userSetting.substring(0, i);
        String password = userSetting.substring(i + 1);
        settingsBuilder.put(authHeaderSettingName, UsernamePasswordToken.basicAuthHeaderValue(username, new SecuredString(password.toCharArray())));
    }

    /*
     We inject additional settings on each tribe client if the current node is a tribe node, to make sure that every tribe has shield installed and enabled too:
     - if shield is loaded on the tribe node we make sure it is also loaded on every tribe, by making it mandatory there
     (this means that the tribe node will fail at startup if shield is not loaded on any tribe due to missing mandatory plugin)
     - if shield is loaded and enabled on the tribe node, we make sure it is also enabled on every tribe, by forcibly enabling it
       (that means it's not possible to disable shield on the tribe clients)
     */
    private void addTribeSettings(ImmutableSettings.Builder settingsBuilder) {
        Map<String, Settings> tribesSettings = settings.getGroups("tribe", true);
        if (tribesSettings.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Settings> tribeSettings : tribesSettings.entrySet()) {
            String tribePrefix = "tribe." + tribeSettings.getKey() + ".";

            //we copy over existing mandatory plugins under additional settings, as they would get overridden otherwise (arrays don't get merged)
            String[] existingMandatoryPlugins = tribeSettings.getValue().getAsArray("plugin.mandatory", null);
            if (existingMandatoryPlugins == null) {
                //shield is mandatory on every tribe if installed and enabled on the tribe node
                settingsBuilder.putArray(tribePrefix + "plugin.mandatory", NAME);
            } else {
                if (!isShieldMandatory(existingMandatoryPlugins)) {
                    String[] updatedMandatoryPlugins = new String[existingMandatoryPlugins.length + 1];
                    System.arraycopy(existingMandatoryPlugins, 0, updatedMandatoryPlugins, 0, existingMandatoryPlugins.length);
                    updatedMandatoryPlugins[updatedMandatoryPlugins.length - 1] = NAME;
                    //shield is mandatory on every tribe if installed and enabled on the tribe node
                    settingsBuilder.putArray(tribePrefix + "plugin.mandatory", updatedMandatoryPlugins);
                }
            }
            //shield must be enabled on every tribe if it's enabled on the tribe node
            settingsBuilder.put(tribePrefix + ENABLED_SETTING_NAME, true);
        }
    }

    private static boolean isShieldMandatory(String[] existingMandatoryPlugins) {
        for (String existingMandatoryPlugin : existingMandatoryPlugins) {
            if (NAME.equals(existingMandatoryPlugin)) {
                return true;
            }
        }
        return false;
    }

    public static Path configDir(Environment env) {
        return new File(env.configFile(), NAME).toPath();
    }

    public static Path resolveConfigFile(Environment env, String name) {
        return configDir(env).resolve(name);
    }

    public static boolean clientMode(Settings settings) {
        return !"node".equals(settings.get(Client.CLIENT_TYPE_SETTING));
    }

    public static boolean shieldEnabled(Settings settings) {
        return settings.getAsBoolean(ENABLED_SETTING_NAME, true);
    }
}
