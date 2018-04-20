/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.history;

import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public final class HistoryStoreField {

    public static final String INDEX_PREFIX = ".watcher-history-";
    public static final String INDEX_PREFIX_WITH_TEMPLATE = INDEX_PREFIX + WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION + "-";
    public static final String DOC_TYPE = "doc";
    static final DateTimeFormatter indexTimeFormat = DateTimeFormat.forPattern("YYYY.MM.dd");

    /**
     * Calculates the correct history index name for a given time
     */
    public static String getHistoryIndexNameForTime(DateTime time) {
        return INDEX_PREFIX_WITH_TEMPLATE + indexTimeFormat.print(time);
    }
}
