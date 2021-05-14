/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.history;

import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;

public final class HistoryStoreField {

    public static final String INDEX_PREFIX = ".watcher-history-";
    public static final String DATA_STREAM = INDEX_PREFIX + WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION;
}
