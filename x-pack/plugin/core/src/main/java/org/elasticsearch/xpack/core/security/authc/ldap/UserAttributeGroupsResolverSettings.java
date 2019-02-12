/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public final class UserAttributeGroupsResolverSettings {
    public static final Setting.AffixSetting<String> ATTRIBUTE = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(LdapRealmSettings.LDAP_TYPE), "user_group_attribute",
            key -> new Setting<>(key, "memberOf", Function.identity(), Setting.Property.NodeScope));

    private UserAttributeGroupsResolverSettings() {
    }

    public static Set<Setting.AffixSetting<?>> getSettings() {
        return Collections.singleton(ATTRIBUTE);
    }
}
