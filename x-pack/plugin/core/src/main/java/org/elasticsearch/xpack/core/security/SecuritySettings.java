/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.util.Optional;

public final class SecuritySettings {

    public static Settings addTransportSettings(final Settings settings) {
        final Settings.Builder builder = Settings.builder();
        if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings)) {
            final String transportType = NetworkModule.TRANSPORT_TYPE_SETTING.get(settings);
            if (SecurityField.NAME4.equals(transportType) == false && SecurityField.NIO.equals(transportType) == false) {
                throw new IllegalArgumentException("transport type setting [" + NetworkModule.TRANSPORT_TYPE_KEY
                    + "] must be [" + SecurityField.NAME4 + "] or [" + SecurityField.NIO + "]" + " but is ["
                    + transportType + "]");
            }
        } else {
            // default to security4
            builder.put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4);
        }
        return builder.build();
    }

    public static Settings addUserSettings(final Settings settings) {
        final Settings.Builder builder = Settings.builder();
        String authHeaderSettingName = ThreadContext.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER;
        if (settings.get(authHeaderSettingName) == null) {
            Optional<String> userOptional = SecurityField.USER_SETTING.get(settings); // TODO migrate to securesetting!
            userOptional.ifPresent(userSetting -> {
                final int i = userSetting.indexOf(":");
                if (i < 0 || i == userSetting.length() - 1) {
                    throw new IllegalArgumentException("invalid [" + SecurityField.USER_SETTING.getKey()
                        + "] setting. must be in the form of \"<username>:<password>\"");
                }
                String username = userSetting.substring(0, i);
                String password = userSetting.substring(i + 1);
                builder.put(authHeaderSettingName, UsernamePasswordToken.basicAuthHeaderValue(username, new SecureString(password)));
            });
        }
        return builder.build();
    }

}
