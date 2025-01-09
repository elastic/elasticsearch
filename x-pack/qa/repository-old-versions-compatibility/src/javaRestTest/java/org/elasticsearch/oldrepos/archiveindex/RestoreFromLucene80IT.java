/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos.archiveindex;

import org.elasticsearch.test.cluster.util.Version;

/**
 * Test case restoring snapshot created with luceneCoded8.0
 * 1. Index Created in ES_v6
 * 2. Cluster upgraded to ES_v7.0.0 -> LuceneVersion 8.0.0 -> LuceneCodec lucene80
 * 3. Added 5 documents to index and created a snapshot (Steps 1-3 into resources/snapshot_vlucene80.zip)
 * 4. Index Restored to version: Current-1 : 8.x
 * 5. Cluster upgraded to version: Current : 9.x
 */
public class RestoreFromLucene80IT extends ArchiveIndexTestCase {

    public RestoreFromLucene80IT(Version version) {
        super(version, "lucene80");
    }
}
