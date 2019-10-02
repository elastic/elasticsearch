/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public final class LdapMetaDataResolverSettings {
    public static final Function<String, Setting.AffixSetting<List<String>>> ADDITIONAL_META_DATA_SETTING = RealmSettings.affixSetting(
            "metadata", key -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Setting.Property.NodeScope));

    private LdapMetaDataResolverSettings() {}

    public static List<Setting.AffixSetting<?>> getSettings(String type) {
        return Collections.singletonList(ADDITIONAL_META_DATA_SETTING.apply(type));
    }
}
