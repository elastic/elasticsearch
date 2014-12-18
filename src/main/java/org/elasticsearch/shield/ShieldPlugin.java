/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.Headers;
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

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;

/**
 *
 */
public class ShieldPlugin extends AbstractPlugin {

    public static final String NAME = "shield";

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
                ImmutableList.<Class<? extends LifecycleComponent>>of(LicenseService.class, FileRolesStore.class, Realms.class) :
                ImmutableList.<Class<? extends LifecycleComponent>>of();
    }

    @Override
    public Settings additionalSettings() {
        if (!enabled) {
            return ImmutableSettings.EMPTY;
        }
        String setting = Headers.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER;
        if (settings.get(setting) != null) {
            return ImmutableSettings.EMPTY;
        }
        String user = settings.get("shield.user");
        if (user == null) {
            return ImmutableSettings.EMPTY;
        }
        int i = user.indexOf(":");
        if (i < 0 || i == user.length() - 1) {
            throw new ShieldSettingsException("Invalid [shield.user] settings. Must be in the form of \"<username>:<password>\"");
        }
        String username = user.substring(0, i);
        String password = user.substring(i + 1);
        return ImmutableSettings.builder()
                .put(setting, UsernamePasswordToken.basicAuthHeaderValue(username, new SecuredString(password.toCharArray()))).build();
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
        return settings.getAsBoolean("shield.enabled", true);
    }
}
