/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import org.elasticsearch.test.cluster.util.Version;

/**
 * Test suite for Archive indices backward compatibility with N-2 versions.
 * The test suite creates a cluster in the N-1 version, where N is the current version.
 * Restores snapshots from old-clusters (version 5/6) and upgrades it to the current version.
 * Test methods are executed after each upgrade.
 *
 * For example the test suite creates a cluster of version 8, then restores a snapshot of an index created
 * when deployed ES version 5/6. The cluster then upgrades to version 9, verifying that the archive index
 * is successfully restored.
 */
public class OldRepositoriesIT extends AbstractUpgradeCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public OldRepositoriesIT(Version version) {
        super(version);
    }

    /**
     * Test case restoring a snapshot created in ES_v5 - Basic mapping
     * 1. Index Created in ES_v5
     * 2. Added 1 documents to index and created a snapshot (Steps 1-2 into resources/snapshot_v5.zip)
     * 3. Index Restored to version: Current-1: 8.x
     * 4. Cluster upgraded to version Current: 9.x
     */
    public void testRestoreMountIndexVersion5() throws Exception {
        verifyCompatibility(TestSnapshotCases.ES_VERSION_5);
    }

    /**
     * Test case mounting a snapshot created in ES_v6 - Basic mapping
     * 1. Index Created in ES_v6
     * 2. Added 1 documents to index and created a snapshot (Steps 1-2 into resources/snapshot_v6.zip)
     * 3. Index Restored to version: Current-1: 8.x
     * 4. Cluster upgraded to version Current: 9.x
     */
    public void testRestoreMountIndexVersion6() throws Exception {
        verifyCompatibility(TestSnapshotCases.ES_VERSION_6);
    }

    /**
     * Test case restoring snapshot created in ES_v5 - Custom-analyzer including a standard-token-filter
     {
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
     * 1. Index Created in ES_v5
     * 2. Added 1 documents to index and created a snapshot (Steps 1-2 into resources/snapshot_v5_standard_token_filter.zip)
     * 3. Index Restored to version: Current: 9.x
     */
    public void testRestoreMountVersion5StandardTokenFilter() throws Exception {
        verifyCompatibilityNoUpgrade(TestSnapshotCases.ES_VERSION_5_STANDARD_TOKEN_FILTER, warnings -> {
            assertEquals(1, warnings.size());
            assertEquals("The [standard] token filter is " + "deprecated and will be removed in a future version.", warnings.getFirst());
        });
    }

    /**
     * Test case restoring snapshot created in ES_v6 - Custom-analyzer including a standard-token-filter
     {
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
     * 1. Index Created in ES_v6
     * 2. Added 1 documents to index and created a snapshot (Steps 1-2 into resources/snapshot_v6_standard_token_filter.zip)
     * 3. Index Restored to version: Current: 9.x
     */
    public void testRestoreMountVersion6StandardTokenFilter() throws Exception {
        verifyCompatibilityNoUpgrade(TestSnapshotCases.ES_VERSION_6_STANDARD_TOKEN_FILTER, warnings -> {
            assertEquals(1, warnings.size());
            assertEquals("The [standard] token filter is " + "deprecated and will be removed in a future version.", warnings.getFirst());
        });
    }

    /**
     * Test case restoring snapshot created with luceneCoded8.0
     * 1. Index Created in ES_v6
     * 2. Cluster upgraded to ES_v7.0.0 -> LuceneVersion 8.0.0 -> LuceneCodec lucene80
     * 3. Added 1 documents to index and created a snapshot (Steps 1-3 into resources/snapshot_vlucene_80.zip)
     * 4. Index Restored to version: Current : 9.x
     */
    public void testRestoreMountVersion6LuceneCodec80() throws Exception {
        verifyCompatibilityNoUpgrade(TestSnapshotCases.ES_VERSION_6_LUCENE_CODEC_80);
    }

    /**
     * Test case restoring snapshot created with luceneCoded8.4
     * 1. Index Created in ES_v6
     * 2. Cluster upgraded to ES_v7.6.0 -> LuceneVersion 8.4.0 -> LuceneCodec lucene84
     * 3. Added 1 documents to index and created a snapshot (Steps 1-3 into resources/snapshot_vlucene_84.zip)
     * 4. Index Restored to version: Current: 9.x
     */
    public void testRestoreMountVersion6LuceneCodec84() throws Exception {
        verifyCompatibilityNoUpgrade(TestSnapshotCases.ES_VERSION_6_LUCENE_CODEC_84);
    }

    /**
     * Test case restoring snapshot created with luceneCoded8.4
     * 1. Index Created in ES_v6
     * 2. Cluster upgraded to ES_v7.9.0 -> LuceneVersion 8.6.0 -> LuceneCodec lucene86
     * 3. Added 1 documents to index and created a snapshot (Steps 1-3 into resources/snapshot_vlucene_86.zip)
     * 4. Index Restored to version: Current: 9.x
     */
    public void testRestoreMountVersion6LuceneCodec86() throws Exception {
        verifyCompatibilityNoUpgrade(TestSnapshotCases.ES_VERSION_6_LUCENE_CODEC_86);
    }

    /**
     * Test case restoring snapshot created with luceneCoded8.4
     * 1. Index Created in ES_v6
     * 2. Cluster upgraded to ES_v7.10.0 -> LuceneVersion 8.7.0 -> LuceneCodec lucene87
     * 3. Added 1 documents to index and created a snapshot (Steps 1-3 into resources/snapshot_vlucene_87.zip)
     * 4. Index Restored to version: Current: 9.x
     */
    public void testRestoreMountVersion6LuceneCodec87() throws Exception {
        verifyCompatibilityNoUpgrade(TestSnapshotCases.ES_VERSION_6_LUCENE_CODEC_87);
    }
}
