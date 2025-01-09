/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos.searchablesnapshot;

import org.elasticsearch.oldrepos.TestSnapshotCases;
import org.elasticsearch.test.cluster.util.Version;

/**
 * Test case mounting snapshot created in ES_v5 - Basic mapping
 *
 * PUT /index
 * {
 *   "settings": {
 *     "number_of_shards": 1,
 *     "number_of_replicas": 1
 *   },
 *   "mappings": {
 *     "my_type": {
 *       "properties": {
 *         "title": {
 *           "type": "text"
 *         },
 *         "created_at": {
 *           "type": "date"
 *         },
 *         "views": {
 *           "type": "integer"
 *         }
 *       }
 *     }
 *   }
 * }
 */
public class MountFromVersion5IT extends SearchableSnapshotTestCase {

    public MountFromVersion5IT(Version version) {
        super(version, TestSnapshotCases.ES_VERSION_5);
    }
}
