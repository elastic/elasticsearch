/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.Duration;
import org.joda.time.ReadableDuration;
import org.opensaml.saml.saml2.core.NameID;

/**
 * Defines default values for a service provider if they are not configured in {@link SamlServiceProviderDocument}
 */
public final class ServiceProviderDefaults {

    public static final Setting<String> APPLICATION_NAME_SETTING
        = Setting.simpleString("xpack.idp.privileges.application", Setting.Property.NodeScope);
    public static final Setting<String> LOGIN_ACTION_SETTING
        = Setting.simpleString("xpack.idp.privileges.login", Setting.Property.NodeScope);
    public static final Setting<String> NAMEID_FORMAT_SETTING
        = Setting.simpleString("xpack.idp.defaults.nameid_format", NameID.TRANSIENT, Setting.Property.NodeScope);
    public static final Setting<TimeValue> AUTHN_EXPIRY_SETTING
        = Setting.timeSetting("xpack.idp.defaults.authn_expiry", TimeValue.timeValueMinutes(5), Setting.Property.NodeScope);

    public final String applicationName;
    public final String loginAction;
    public final String nameIdFormat;
    public final ReadableDuration authenticationExpiry;

    public ServiceProviderDefaults(String applicationName,
                                   String loginAction,
                                   String nameIdFormat,
                                   ReadableDuration authenticationExpiry) {
        this.applicationName = applicationName;
        this.loginAction = loginAction;
        this.nameIdFormat = nameIdFormat;
        this.authenticationExpiry = authenticationExpiry;
    }

    public static ServiceProviderDefaults forSettings(Settings settings) {
        final String appplication = require(settings, APPLICATION_NAME_SETTING);
        final String login = require(settings, LOGIN_ACTION_SETTING);
        final String nameId = require(settings, NAMEID_FORMAT_SETTING);
        final TimeValue expiry = require(settings, AUTHN_EXPIRY_SETTING);
        return new ServiceProviderDefaults(appplication, login, nameId, Duration.millis(expiry.millis()));
    }

    private static <T> T require(Settings settings, Setting<T> setting) {
        final T value = setting.get(settings);
        if (value == null) {
            throw new IllegalStateException("Setting [" + setting.getKey() + "] must be configured");
        }
        return value;
    }
}
