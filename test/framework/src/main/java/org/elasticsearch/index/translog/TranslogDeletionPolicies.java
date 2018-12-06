/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;

public class TranslogDeletionPolicies {

    public static TranslogDeletionPolicy createTranslogDeletionPolicy() {
        return new TranslogDeletionPolicy(
                IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getDefault(Settings.EMPTY).getBytes(),
                IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getDefault(Settings.EMPTY).getMillis()
        );
    }

    public static TranslogDeletionPolicy createTranslogDeletionPolicy(IndexSettings indexSettings) {
        return new TranslogDeletionPolicy(indexSettings.getTranslogRetentionSize().getBytes(),
                indexSettings.getTranslogRetentionAge().getMillis());
    }

}
