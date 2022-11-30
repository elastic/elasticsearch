/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.jwt;

import org.elasticsearch.common.settings.Setting;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * Settings used by JwtRealmsService for common handling of JWT realm instances.
 */
public class JwtRealmsServiceSettings {

    public static final List<String> DEFAULT_PRINCIPAL_CLAIMS = List.of("sub", "oid", "client_id", "appid", "azp", "email");

    public static final Setting<List<String>> PRINCIPAL_CLAIMS_SETTING = Setting.listSetting(
        "xpack.security.authc.jwt.principal_claims",
        DEFAULT_PRINCIPAL_CLAIMS,
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * Get all settings shared by all JWT Realms.
     * @return All settings shared by all JWT Realms.
     */
    public static Collection<Setting<?>> getSettings() {
        return List.of(PRINCIPAL_CLAIMS_SETTING);
    }

    private JwtRealmsServiceSettings() {}
}
