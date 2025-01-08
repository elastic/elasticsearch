/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos.searchablesnapshot;

import org.elasticsearch.oldrepos.Snapshot;
import org.elasticsearch.test.cluster.util.Version;

/**
 * Test case mounting index created in ES_v5 - Custom-Analyzer - standard token filter
 *
 * PUT /index
 * {
 *   "settings": {
 *     "analysis": {
 *       "analyzer": {
 *         "custom_analyzer": {
 *           "type": "custom",
 *           "tokenizer": "standard",
 *           "filter": [
 *             "standard",
 *             "lowercase"
 *           ]
 *         }
 *       }
 *     }
 *   },
 *   "mappings": {
 *     "my_type": {
 *       "properties": {
 *         "content": {
 *           "type": "text",
 *           "analyzer": "custom_analyzer"
 *         }
 *       }
 *     }
 *   }
 * }
 */
public class MountFromVersion5CustomAnalyzerIT extends SearchableSnapshotTestCase {

    public MountFromVersion5CustomAnalyzerIT(Version version) {
        super(version, Snapshot.FIVE_CUSTOM_ANALYZER, warnings -> {
            assertEquals(1, warnings.size());
            assertEquals("The [standard] token filter is " + "deprecated and will be removed in a future version.", warnings.getFirst());
        });
    }
}
