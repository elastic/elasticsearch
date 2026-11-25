/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.MinHashIndexParamBounds;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class ReindexDataStreamTransportActionTests extends ESTestCase {
    private static final IndexVersion OLD_VERSION = IndexVersion.fromId(7170099);

    private final IndexMetadata oldInvalid = index(
        "old-invalid",
        Settings.builder()
            .put("index.analysis.filter.big.type", "min_hash")
            .put("index.analysis.filter.big.hash_count", 1)
            .put("index.analysis.filter.big.bucket_count", 512)
            .put("index.analysis.filter.big.hash_set_size", 200)
            .put("index.analysis.filter.small.type", "min_hash")
            .put("index.analysis.filter.small.hash_set_size", 20)
            .put("index.analysis.filter.other.type", "lowercase")
    );
    private final IndexMetadata oldValid = index(
        "old-valid",
        Settings.builder().put("index.analysis.filter.small.type", "min_hash").put("index.analysis.filter.small.hash_set_size", 20)
    );
    private final ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
        .put(oldInvalid, false)
        .put(oldValid, false)
        .build();

    public void testReindexCompatibilityCheckRejectsMinHashViolations() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ReindexDataStreamTransportAction.reindexCompatibilityCheck(
                "ds",
                List.of(oldInvalid.getIndex(), oldValid.getIndex()),
                project
            )
        );

        assertEquals(
            "Reindexing data stream [ds] would fail: backing indices define [min_hash] filters that new indices reject: "
                + "index [old-invalid] [big]: "
                + MinHashIndexParamBounds.violation(1, 512, 200)
                + " "
                + MinHashIndexParamBounds.REINDEX_REMEDIATION,
            e.getMessage()
        );
    }

    public void testReindexCompatibilityCheckAllowsValidIndices() {
        ReindexDataStreamTransportAction.reindexCompatibilityCheck("ds", List.of(oldValid.getIndex()), project);
        ReindexDataStreamTransportAction.reindexCompatibilityCheck("ds", List.of(), project);
    }

    private static IndexMetadata index(String name, Settings.Builder analysisSettings) {
        return IndexMetadata.builder(name)
            .settings(settings(OLD_VERSION).put(analysisSettings.build()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
