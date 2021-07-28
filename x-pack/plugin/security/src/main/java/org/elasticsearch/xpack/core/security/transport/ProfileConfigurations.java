/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

public final class ProfileConfigurations {

    private ProfileConfigurations() {}

    public static Map<String, SSLConfiguration> get(Settings settings, SSLService sslService, SSLConfiguration defaultConfiguration) {
        Set<String> profileNames = settings.getGroups("transport.profiles.", true).keySet();
        Map<String, SSLConfiguration> profileConfiguration = new HashMap<>(profileNames.size() + 1);
        for (String profileName : profileNames) {
            if (profileName.equals(TransportSettings.DEFAULT_PROFILE)) {
                // don't attempt to parse ssl settings from the profile;
                // profiles need to be killed with fire
                if (settings.getByPrefix("transport.profiles.default.xpack.security.ssl.").isEmpty()) {
                    continue;
                } else {
                    throw new IllegalArgumentException("SSL settings should not be configured for the default profile. " +
                        "Use the [xpack.security.transport.ssl] settings instead.");
                }
            }
            SSLConfiguration configuration = sslService.getSSLConfiguration("transport.profiles." + profileName + "." + setting("ssl"));
            profileConfiguration.put(profileName, configuration);
        }

        assert profileConfiguration.containsKey(TransportSettings.DEFAULT_PROFILE) == false;
        profileConfiguration.put(TransportSettings.DEFAULT_PROFILE, defaultConfiguration);
        return profileConfiguration;
    }
}
