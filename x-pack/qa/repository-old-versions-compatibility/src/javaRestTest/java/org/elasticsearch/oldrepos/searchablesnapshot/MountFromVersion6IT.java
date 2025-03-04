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
 * Test case mounting snapshot created in ES_v6 - Basic mapping
 *
 * PUT /index
 * {
 *   "settings": {
 *     "number_of_shards": 1,
 *     "number_of_replicas": 1
 *   },
 *   "mappings": {
 *     "_doc": {
 *       "properties": {
 *         "title": {
 *           "type": "text"
 *         },
 *         "content": {
 *           "type": "text"
 *         },
 *         "created_at": {
 *           "type": "date"
 *         }
 *       }
 *     }
 *   }
 * }
 */
public class MountFromVersion6IT extends SearchableSnapshotTestCase {

    public MountFromVersion6IT(Version version) {
        super(version, TestSnapshotCases.ES_VERSION_6);
    }
}
