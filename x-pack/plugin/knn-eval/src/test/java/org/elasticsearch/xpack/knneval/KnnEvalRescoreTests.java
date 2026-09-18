/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/** Tests candidate and rescore window resolution for DiskBBQ mappings. */
public class KnnEvalRescoreTests extends ESTestCase {

    private static KnnEvalRescore rescoreFor(String indexType) {
        return KnnEvalRescore.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", indexType, "rescore_vector", Map.of("oversample", 3.0)))
        );
    }

    public void testOnlyDiskBbqIsSupported() {
        assertNotNull(rescoreFor("bbq_disk"));
        for (String indexType : List.of("hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw", "flat", "int8_flat", "int4_flat", "bbq_flat")) {
            assertThat(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> KnnEvalRescore.fromFieldMapping("emb", Map.of("type", "dense_vector", "index_options", Map.of("type", indexType)))
                ).getMessage(),
                containsString("field [emb] must use [index_options.type=bbq_disk], found [" + indexType + "]")
            );
        }
    }

    public void testExplicitDiskBbqIndexOptionsAreRequired() {
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> KnnEvalRescore.fromFieldMapping("emb", Map.of("type", "dense_vector")))
                .getMessage(),
            containsString("field [emb] must use [index_options.type=bbq_disk], found [null]")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> KnnEvalRescore.fromFieldMapping("emb", null)).getMessage(),
            containsString("field [emb] must use [index_options.type=bbq_disk], found [null]")
        );
    }

    public void testDetectsCappedRescoreWindow() {
        KnnEvalRescore rescore = rescoreFor("bbq_disk");
        assertFalse(rescore.autoCalibrate());
        assertFalse(rescore.isRescoreWindowCapped(10, 10.0f));
        assertTrue(rescore.isRescoreWindowCapped(10, 10000.0f));
    }

    public void testAutoCalibrationIsReadFromTheMapping() {
        KnnEvalRescore rescore = KnnEvalRescore.fromFieldMapping(
            "emb",
            Map.of(
                "type",
                "dense_vector",
                "index_options",
                Map.of("type", "bbq_disk", "auto_calibrate", true, "rescore_vector", Map.of("oversample", 3.0))
            )
        );
        assertTrue(rescore.autoCalibrate());
    }
}
