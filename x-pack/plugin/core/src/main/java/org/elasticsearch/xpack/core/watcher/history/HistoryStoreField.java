/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.history;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;

import java.time.ZonedDateTime;

public final class HistoryStoreField {

    public static final String INDEX_PREFIX = ".watcher-history-";
    public static final String INDEX_PREFIX_WITH_TEMPLATE = INDEX_PREFIX + WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION + "-";
    public static final String INDEX_PREFIX_WITH_TEMPLATE_10 = INDEX_PREFIX
        + WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION_10
        + "-";
    private static final DateFormatter indexTimeFormat = DateFormatter.forPattern("yyyy.MM.dd");

    /**
     * Calculates the correct history index name for a given time
     */
    public static String getHistoryIndexNameForTime(ZonedDateTime time, ClusterState state) {
        if (state == null || state.nodes().getMinNodeVersion().onOrAfter(Version.V_7_7_0)) {
            return INDEX_PREFIX_WITH_TEMPLATE + indexTimeFormat.format(time);
        } else {
            return INDEX_PREFIX_WITH_TEMPLATE_10 + indexTimeFormat.format(time);
        }
    }
}
