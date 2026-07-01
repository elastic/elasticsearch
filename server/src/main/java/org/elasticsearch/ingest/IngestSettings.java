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
     * The maximum number of ingest pipelines that may exist at once. Pipelines are stored in the cluster state, which is held in heap on
     * every node and serialized on every cluster state update, so an unbounded number of them can destabilize the cluster. This is a safety
     * limit; it is only enforced when creating a new pipeline, so existing pipelines above the limit continue to work.
     */
    public static final Setting<Integer> MAX_PIPELINES = Setting.intSetting(
        "ingest.pipeline.max_pipelines",
        10000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The maximum serialized size of a single ingest pipeline. This bounds the total contribution of one pipeline (including its
     * description, processors, and any other fields) to the cluster state. This is a safety limit and is enforced when creating or updating
     * a pipeline.
     */
    public static final Setting<ByteSizeValue> MAX_PIPELINE_SIZE = Setting.byteSizeSetting(
        "ingest.pipeline.max_pipeline_size",
        ByteSizeValue.ofMb(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The maximum combined serialized size of <em>all</em> ingest pipelines. Per-pipeline and per-count limits do not bound the aggregate --
     * many pipelines each just under the per-pipeline limit can still accumulate enough data in the cluster state to destabilize the cluster
     * (cluster state is held in heap on every node and re-serialized on every update). This caps that aggregate. It is only enforced when
     * creating a new pipeline, so existing pipelines above the limit continue to work.
     */
    public static final Setting<ByteSizeValue> MAX_TOTAL_METADATA_SIZE = Setting.byteSizeSetting(
        "ingest.pipeline.max_total_metadata_size",
        ByteSizeValue.ofMb(50),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

}
