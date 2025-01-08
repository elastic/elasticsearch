/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos.archiveindex;

import org.elasticsearch.oldrepos.Snapshot;
import org.elasticsearch.test.cluster.util.Version;

/**
 * Test case restoring snapshot created in ES_v5 - Basic mapping
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
public class RestoreFromVersion5IT extends ArchiveIndexTestCase {

    public RestoreFromVersion5IT(Version version) {
        super(version, Snapshot.FIVE);
    }
}
