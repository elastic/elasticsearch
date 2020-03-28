/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.analytics;

import org.elasticsearch.common.settings.Setting;

public class AnalyticsSettings {
    /**
     * Index setting describing the maximum number of top metrics that
     * can be collected per bucket. This defaults to a low number because
     * there can be a *huge* number of buckets
     */
    public static final Setting<Integer> MAX_BUCKET_SIZE_SETTING =
        Setting.intSetting("index.top_metrics_max_size", 10, 1, Setting.Property.Dynamic, Setting.Property.IndexScope);
}
