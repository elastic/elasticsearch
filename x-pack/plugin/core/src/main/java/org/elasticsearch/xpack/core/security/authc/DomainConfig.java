/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.settings.Setting;

import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.DOMAIN_UID_LITERAL_USERNAME_SETTING;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.DOMAIN_UID_SUFFIX_SETTING;

public record DomainConfig(String name, Set<String> memberRealmNames, boolean literalUsername, String suffix)
    implements
        Comparable<DomainConfig> {

    public DomainConfig {
        final Setting<String> suffixSetting = DOMAIN_UID_SUFFIX_SETTING.getConcreteSettingForNamespace(name);
        final Setting<Boolean> literalUsernameSetting = DOMAIN_UID_LITERAL_USERNAME_SETTING.getConcreteSettingForNamespace(name);

        if (literalUsername && suffix == null) {
            throw new IllegalArgumentException(
                "[" + suffixSetting.getKey() + "] must be configured when [" + literalUsernameSetting.getKey() + "] is set to [true]"
            );
        } else if (false == literalUsername && suffix != null) {
            throw new IllegalArgumentException(
                "[" + suffixSetting.getKey() + "] must not be configured when [" + literalUsernameSetting.getKey() + "] is set to [false]"
            );
        }
    }

    @Override
    public int compareTo(DomainConfig o) {
        return name.compareTo(o.name);
    }
}
