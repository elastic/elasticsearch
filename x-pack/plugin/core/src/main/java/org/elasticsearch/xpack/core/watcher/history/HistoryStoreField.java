/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.history;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;

import java.time.ZonedDateTime;

public final class HistoryStoreField {

    public static final String INDEX_PREFIX = ".watcher-history-";
    public static final String INDEX_PREFIX_WITH_TEMPLATE = INDEX_PREFIX + WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION + "-";
    private static final DateFormatter indexTimeFormat = DateFormatter.forPattern("yyyy.MM.dd");

    /**
     * Calculates the correct history index name for a given time
     */
    public static String getHistoryIndexNameForTime(ZonedDateTime time) {
        return INDEX_PREFIX_WITH_TEMPLATE + indexTimeFormat.format(time);
    }
}
