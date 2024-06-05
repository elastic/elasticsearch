/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.file;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;

import java.util.HashSet;
import java.util.Set;

public final class FileRealmSettings {
    public static final String TYPE = "file";
    public static final String DEFAULT_NAME = "default_file";

    private FileRealmSettings() {}

    /**
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> set = new HashSet<>(CachingUsernamePasswordRealmSettings.getSettings(TYPE));
        set.addAll(RealmSettings.getStandardSettings(TYPE));
        return set;
    }
}
