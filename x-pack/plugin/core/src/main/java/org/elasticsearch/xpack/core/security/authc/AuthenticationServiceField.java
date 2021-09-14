/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.settings.Setting;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

public final class AuthenticationServiceField {

    public static final Setting<Boolean> RUN_AS_ENABLED =
            Setting.boolSetting(setting("authc.run_as.enabled"), true, Setting.Property.NodeScope);
    public static final String RUN_AS_USER_HEADER = "es-security-runas-user";

    private AuthenticationServiceField() {}
}
