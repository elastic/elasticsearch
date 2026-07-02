/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

public final class IngestSettings {

    private IngestSettings() {
        // utility class
    }

    // this watchdog interval setting is deprecated because it no longer controls any behavior
    public static final Setting<TimeValue> GROK_WATCHDOG_INTERVAL = Setting.timeSetting(
        "ingest.grok.watchdog.interval",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<TimeValue> GROK_WATCHDOG_MAX_EXECUTION_TIME = Setting.timeSetting(
        "ingest.grok.watchdog.max_execution_time",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    /**
     * The maximum cumulative number of bytes that can be written into a single document's fields over its entire ingest
     * lifecycle (across all processors and any nested pipelines). This guards against pipelines that chain many processors
     * that each copy or expand an already-large field (e.g. many `set` processors with `copy_from`) -- individually cheap,
     * but the resulting document can otherwise blow up memory when it's later serialized in full. See
     * https://github.com/elastic/security/issues/5580.
     */
    public static final Setting<ByteSizeValue> MAX_CUMULATIVE_FIELD_VALUE_BYTES = Setting.byteSizeSetting(
        "ingest.max_cumulative_field_value_size",
        ByteSizeValue.ofMb(50),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

}
