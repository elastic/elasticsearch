/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

public final class IngestSettings {

    private IngestSettings() {
        // utility class
    }

    public static final Setting<TimeValue> GROK_WATCHDOG_INTERVAL = Setting.timeSetting(
        "ingest.grok.watchdog.interval",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> GROK_WATCHDOG_MAX_EXECUTION_TIME = Setting.timeSetting(
        "ingest.grok.watchdog.max_execution_time",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

}
