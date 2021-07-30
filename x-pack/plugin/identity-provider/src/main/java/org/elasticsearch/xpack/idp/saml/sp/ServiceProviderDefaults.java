/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.joda.time.Duration;
import org.joda.time.ReadableDuration;
import org.opensaml.saml.saml2.core.NameID;

import java.util.List;

/**
 * Defines default values for a service provider if they are not configured in {@link SamlServiceProviderDocument}
 */
public final class ServiceProviderDefaults {

    public static final Setting<String> APPLICATION_NAME_SETTING
        = Setting.simpleString("xpack.idp.privileges.application", Setting.Property.NodeScope);
    public static final Setting<String> NAMEID_FORMAT_SETTING
        = Setting.simpleString("xpack.idp.defaults.nameid_format", NameID.TRANSIENT, Setting.Property.NodeScope);
    public static final Setting<TimeValue> AUTHN_EXPIRY_SETTING
        = Setting.timeSetting("xpack.idp.defaults.authn_expiry", TimeValue.timeValueMinutes(5), Setting.Property.NodeScope);

    public final String applicationName;
    public final String nameIdFormat;
    public final ReadableDuration authenticationExpiry;

    public ServiceProviderDefaults(String applicationName,
                                   String nameIdFormat,
                                   ReadableDuration authenticationExpiry) {
        this.applicationName = applicationName;
        this.nameIdFormat = nameIdFormat;
        this.authenticationExpiry = authenticationExpiry;
    }

    public static ServiceProviderDefaults forSettings(Settings settings) {
        final String appplication = require(settings, APPLICATION_NAME_SETTING);
        final String nameId = NAMEID_FORMAT_SETTING.get(settings);
        final TimeValue expiry = AUTHN_EXPIRY_SETTING.get(settings);
        return new ServiceProviderDefaults(appplication, nameId, Duration.millis(expiry.millis()));
    }

    private static <T> T require(Settings settings, Setting<T> setting) {
        if (setting.exists(settings)) {
            return setting.get(settings);
        }
        throw new IllegalStateException("Setting [" + setting.getKey() + "] must be configured");
    }

    public static List<Setting<?>> getSettings() {
        return List.of(APPLICATION_NAME_SETTING, NAMEID_FORMAT_SETTING, AUTHN_EXPIRY_SETTING);
    }
}
