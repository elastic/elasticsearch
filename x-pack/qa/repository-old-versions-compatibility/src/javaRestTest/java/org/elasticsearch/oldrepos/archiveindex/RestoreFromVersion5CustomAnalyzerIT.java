/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos.archiveindex;

import org.elasticsearch.oldrepos.TestSnapshotCases;
import org.elasticsearch.test.cluster.util.Version;

/**
 * Test case restoring index created in ES_v5 - Custom-Analyzer - standard token filter
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
public class RestoreFromVersion5CustomAnalyzerIT extends ArchiveIndexTestCase {

    public RestoreFromVersion5CustomAnalyzerIT(Version version) {
        super(version, TestSnapshotCases.ES_VERSION_5_STANDARD_TOKEN_FILTER, warnings -> {
            assertEquals(1, warnings.size());
            assertEquals("The [standard] token filter is " + "deprecated and will be removed in a future version.", warnings.getFirst());
        });
    }
}
