/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorIndexOptions;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.instanceOf;

/**
 * A shard's {@link FsDirectoryFactory.HybridDirectory} reads the dense vector options from the shard's current mapping.
 */
public class HybridDirectoryMappingTests extends ESSingleNodeTestCase {

    public void testOptionsComeFromTheMapping() {
        FsDirectoryFactory.HybridDirectory directory = createIndexDirectory("""
            {
              "properties": {
                "plain": { "type": "dense_vector", "dims": 64 },
                "merged": {
                  "type": "dense_vector", "dims": 64,
                  "index_options": { "type": "bbq_hnsw", "on_disk_merge": true }
                },
                "rescored": {
                  "type": "dense_vector", "dims": 64,
                  "index_options": { "type": "bbq_hnsw", "on_disk_rescore": true }
                },
                "outer": {
                  "properties": {
                    "inner": {
                      "type": "dense_vector", "dims": 64,
                      "index_options": { "type": "bbq_hnsw", "on_disk_merge": true, "on_disk_rescore": true }
                    }
                  }
                },
                "text": { "type": "keyword" }
              }
            }""");

        assertOptions(directory, "plain", false, false);
        assertOptions(directory, "merged", false, true);
        assertOptions(directory, "rescored", true, false);
        assertOptions(directory, "outer.inner", true, true);
        assertNull(directory.vectorIndexOptions("text"));
        assertNull(directory.vectorIndexOptions("absent"));
    }

    /** A field added by a mapping update is known to the directory without reopening the shard. */
    public void testAFieldAddedByAMappingUpdate() {
        FsDirectoryFactory.HybridDirectory directory = createIndexDirectory("""
            { "properties": { "vector": { "type": "dense_vector", "dims": 64 } } }""");
        assertNull(directory.vectorIndexOptions("added"));

        indicesAdmin().preparePutMapping("test").setSource("""
            {
              "properties": {
                "added": {
                  "type": "dense_vector", "dims": 64,
                  "index_options": { "type": "bbq_hnsw", "on_disk_merge": true }
                }
              }
            }""").get();

        assertOptions(directory, "added", false, true);
    }

    /** Turning the options on and off again reaches the directory each time. */
    public void testOptionsFlipWithTheMapping() {
        FsDirectoryFactory.HybridDirectory directory = createIndexDirectory(vectorMapping(false));
        assertOptions(directory, "vector", false, false);

        indicesAdmin().preparePutMapping("test").setSource(vectorMapping(true)).get();
        assertOptions(directory, "vector", true, true);

        indicesAdmin().preparePutMapping("test").setSource(vectorMapping(false)).get();
        assertOptions(directory, "vector", false, false);
    }

    private FsDirectoryFactory.HybridDirectory createIndexDirectory(String mapping) {
        assumeTrue("hybridfs needs a 64-bit JVM", Constants.JRE_IS_64BIT);
        IndexService indexService = createIndex(
            "test",
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "hybridfs"))
                .setMapping(mapping)
        );
        var directory = FilterDirectory.unwrap(indexService.getShard(0).store().directory());
        assertThat(directory, instanceOf(FsDirectoryFactory.HybridDirectory.class));
        return (FsDirectoryFactory.HybridDirectory) directory;
    }

    private static void assertOptions(FsDirectoryFactory.HybridDirectory directory, String field, boolean rescore, boolean merge) {
        DenseVectorIndexOptions options = directory.vectorIndexOptions(field);
        assertNotNull(field, options);
        assertEquals(field + " on_disk_rescore", rescore, options.isOnDiskRescore());
        assertEquals(field + " on_disk_merge", merge, options.isOnDiskMerge());
    }

    private static String vectorMapping(boolean onDisk) {
        return Strings.format("""
            {
              "properties": {
                "vector": {
                  "type": "dense_vector", "dims": 64,
                  "index_options": { "type": "bbq_hnsw", "on_disk_merge": %s, "on_disk_rescore": %s }
                }
              }
            }""", onDisk, onDisk);
    }
}
