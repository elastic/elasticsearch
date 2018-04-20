/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public final class UserAttributeGroupsResolverSettings {
    public static final Setting<String> ATTRIBUTE = new Setting<>("user_group_attribute", "memberOf",
            Function.identity(), Setting.Property.NodeScope);

    private UserAttributeGroupsResolverSettings() {}

    public static Set<Setting<?>> getSettings() {
        return Collections.singleton(ATTRIBUTE);
    }
}
