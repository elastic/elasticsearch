/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.saml;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.Set;
import java.util.function.Function;

public class SingleSpSamlRealmSettings {
    public static final String TYPE = "saml";

    public static final Setting.AffixSetting<String> SP_ENTITY_ID = RealmSettings.simpleString(
        TYPE,
        "sp.entity_id",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<String> SP_ACS = RealmSettings.simpleString(TYPE, "sp.acs", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> SP_LOGOUT = RealmSettings.simpleString(TYPE, "sp.logout", Setting.Property.NodeScope);

    public static Set<Setting.AffixSetting<?>> getSettings() {
        Set<Setting.AffixSetting<?>> samlSettings = SamlRealmSettings.getSettings(TYPE);
        samlSettings.add(SP_ENTITY_ID);
        samlSettings.add(SP_ACS);
        samlSettings.add(SP_LOGOUT);

        return samlSettings;
    }

    public static <T> String getFullSettingKey(String realmName, Function<String, Setting.AffixSetting<T>> setting) {
        return RealmSettings.getFullSettingKey(realmName, setting.apply(TYPE));
    }
}
