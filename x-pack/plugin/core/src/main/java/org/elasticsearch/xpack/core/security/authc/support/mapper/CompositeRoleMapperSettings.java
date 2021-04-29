/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;

import java.util.Collection;

public final class CompositeRoleMapperSettings {
    private CompositeRoleMapperSettings() {}

    public static Collection<? extends Setting.AffixSetting<?>> getSettings(String type) {
        return DnRoleMapperSettings.getSettings(type);
    }
}
