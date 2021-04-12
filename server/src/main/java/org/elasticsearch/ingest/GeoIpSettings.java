/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.settings.Setting;

public final class GeoIpSettings {
    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting("geoip.downloader.enabled", true,
        Setting.Property.Dynamic, Setting.Property.NodeScope);
}
