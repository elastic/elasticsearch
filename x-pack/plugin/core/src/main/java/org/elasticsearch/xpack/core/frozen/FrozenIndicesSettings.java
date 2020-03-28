/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.frozen;

import org.elasticsearch.common.settings.Setting;

public class FrozenIndicesSettings {
    public static final Setting<Boolean> INDEX_FROZEN_SETTING = Setting.boolSetting("index.frozen", false, Setting.Property.IndexScope,
        Setting.Property.PrivateIndex);
}
