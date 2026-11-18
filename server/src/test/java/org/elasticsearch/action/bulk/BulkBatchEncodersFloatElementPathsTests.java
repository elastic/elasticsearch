/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;
import java.util.Set;

/** The coordinating node reads the float vector paths of an index from its mapping in the cluster state. */
public class BulkBatchEncodersFloatElementPathsTests extends ESTestCase {

    public void testReadsFloatVectorPathsFromMapping() {
        IndexMetadata indexMetadata = indexWithMapping("""
            {"_doc":{"properties":{
              "v":{"type":"dense_vector","dims":3},
              "b":{"type":"dense_vector","dims":3,"element_type":"byte"},
              "obj":{"properties":{"v":{"type":"dense_vector","dims":3,"element_type":"bfloat16"}}},
              "n":{"type":"float"}
            }}}""");
        assertEquals(Set.of("v", "obj.v"), BulkBatchEncoders.floatElementPaths(indexMetadata));
        // a second index with the same mapping reads the same paths
        assertEquals(
            Set.of("v", "obj.v"),
            BulkBatchEncoders.floatElementPaths(indexWithMapping(indexMetadata.mapping().source().string()))
        );
    }

    public void testNoMappingOrNoVectors() {
        assertEquals(Set.of(), BulkBatchEncoders.floatElementPaths(null));
        assertEquals(
            Set.of(),
            BulkBatchEncoders.floatElementPaths(
                IndexMetadata.builder("no-mapping").settings(indexSettings(IndexVersion.current(), 1, 0)).build()
            )
        );
        assertEquals(Set.of(), BulkBatchEncoders.floatElementPaths(indexWithMapping("""
            {"_doc":{"properties":{"n":{"type":"float"}}}}""")));
    }

    /**
     * Outside the strict columnar index modes, an index that keeps vectors in {@code _source} gets no float paths, so that the
     * {@code _source} the batch path rebuilds from the encoded row keeps the spelling of its vectors: with float paths, an element
     * {@code 0.0123} would come back as {@code 0.012299999594688416}, the float widened to double. The strict columnar modes render
     * vectors in {@code _source} from the indexed floats, so they keep their float paths.
     */
    public void testNoFloatPathsWhenSourceKeepsVectorText() {
        final String mapping = """
            {"_doc":{"properties":{"v":{"type":"dense_vector","dims":3}}}}""";
        final Settings.Builder keepVectors = Settings.builder()
            .put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), false);

        assertEquals(Set.of("v"), BulkBatchEncoders.floatElementPaths(indexWithMapping(mapping, Settings.EMPTY)));
        assertEquals(Set.of(), BulkBatchEncoders.floatElementPaths(indexWithMapping(mapping, keepVectors.build())));
        assertEquals(
            Set.of(),
            BulkBatchEncoders.floatElementPaths(
                indexWithMapping(mapping, Settings.builder().put(keepVectors.build()).put(IndexSettings.MODE.getKey(), "logsdb").build())
            )
        );
        for (String strictColumnar : new String[] { "columnar", "logsdb_columnar", "vectordb_columnar" }) {
            assertEquals(
                strictColumnar,
                Set.of("v"),
                BulkBatchEncoders.floatElementPaths(
                    indexWithMapping(
                        mapping,
                        Settings.builder().put(keepVectors.build()).put(IndexSettings.MODE.getKey(), strictColumnar).build()
                    )
                )
            );
        }
    }

    private static IndexMetadata indexWithMapping(String mapping) {
        return indexWithMapping(mapping, Settings.EMPTY);
    }

    private static IndexMetadata indexWithMapping(String mapping, Settings settings) {
        return IndexMetadata.builder(randomAlphaOfLength(8).toLowerCase(Locale.ROOT))
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(settings))
            .putMapping(mapping)
            .build();
    }
}
