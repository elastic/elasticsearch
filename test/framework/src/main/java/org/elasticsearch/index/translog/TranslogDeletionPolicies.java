/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;

public class TranslogDeletionPolicies {

    public static TranslogDeletionPolicy createTranslogDeletionPolicy() {
        return new TranslogDeletionPolicy(
            IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getDefault(Settings.EMPTY).getBytes(),
            IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getDefault(Settings.EMPTY).getMillis(),
            IndexSettings.INDEX_TRANSLOG_RETENTION_TOTAL_FILES_SETTING.getDefault(Settings.EMPTY)
        );
    }

    public static TranslogDeletionPolicy createTranslogDeletionPolicy(IndexSettings indexSettings) {
        return new TranslogDeletionPolicy(
            indexSettings.getTranslogRetentionSize().getBytes(),
            indexSettings.getTranslogRetentionAge().getMillis(),
            indexSettings.getTranslogRetentionTotalFiles()
        );
    }

}
