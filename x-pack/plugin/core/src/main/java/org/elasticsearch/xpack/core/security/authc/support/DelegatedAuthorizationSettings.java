/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.settings.Setting;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Settings related to "Delegated Authorization" (aka Lookup Realms)
 */
public class DelegatedAuthorizationSettings {

    public static final Setting<List<String>> AUTHZ_REALMS = Setting.listSetting("authorization_realms",
        Collections.emptyList(), Function.identity(), Setting.Property.NodeScope);

    public static Collection<Setting<?>> getSettings() {
        return Collections.singleton(AUTHZ_REALMS);
    }
}
