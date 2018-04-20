/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public final class LdapSessionFactorySettings {

    public static final Setting<List<String>> USER_DN_TEMPLATES_SETTING = Setting.listSetting("user_dn_templates",
            Collections.emptyList(), Function.identity(), Setting.Property.NodeScope);

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactorySettings.getSettings());
        settings.add(USER_DN_TEMPLATES_SETTING);
        return settings;
    }
}
