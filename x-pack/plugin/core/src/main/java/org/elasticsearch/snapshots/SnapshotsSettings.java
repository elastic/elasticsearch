/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.settings.Setting;

public class SnapshotsSettings {
    public static final Setting<Boolean> SOURCE_ONLY_SETTING = Setting.boolSetting("index.source_only", false, Setting
        .Property.IndexScope, Setting.Property.Final, Setting.Property.PrivateIndex);
}
