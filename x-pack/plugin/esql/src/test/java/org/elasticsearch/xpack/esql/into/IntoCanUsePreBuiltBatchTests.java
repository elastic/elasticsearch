/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class IntoCanUsePreBuiltBatchTests extends ESTestCase {

    public void testConcreteIndexWithoutRoutingPathIsEligible() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder("dest").settings(indexSettings(IndexVersion.current(), 1, 0)))
            .build();
        assertTrue(IntoPageWriter.Factory.canUsePreBuiltBatch(project, "dest"));
    }

    public void testIndexWithDefaultPipelineIsNotEligible() {
        IndexMetadata index = IndexMetadata.builder("dest")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "my-pipeline"))
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, true).build();
        assertFalse(IntoPageWriter.Factory.canUsePreBuiltBatch(project, "dest"));
    }

    public void testIndexWithFinalPipelineIsNotEligible() {
        IndexMetadata index = IndexMetadata.builder("dest")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.FINAL_PIPELINE.getKey(), "my-final-pipeline"))
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, true).build();
        assertFalse(IntoPageWriter.Factory.canUsePreBuiltBatch(project, "dest"));
    }

    public void testConcreteIndexWithRoutingPathIsNotEligible() {
        IndexMetadata index = IndexMetadata.builder("tsdb-dest")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.host")
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            )
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, true).build();
        assertFalse(IntoPageWriter.Factory.canUsePreBuiltBatch(project, "tsdb-dest"));
    }

    // Before the E6 fix, project.index("my-alias") returned null so routesFromSource returned false.
    public void testAliasWhoseWriteIndexHasRoutingPathIsNotEligible() {
        IndexMetadata backing = IndexMetadata.builder("backing-000001")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.host")
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            )
            .putAlias(AliasMetadata.builder("my-alias").writeIndex(true))
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(backing, true).build();
        assertFalse(IntoPageWriter.Factory.canUsePreBuiltBatch(project, "my-alias"));
    }

    public void testDataStreamWhoseWriteIndexHasRoutingPathIsNotEligible() {
        IndexMetadata backing = IndexMetadata.builder(".ds-metrics-000001")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.host")
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            )
            .build();
        Index backingIndex = backing.getIndex();
        DataStream ds = DataStream.builder("metrics", List.of(backingIndex)).build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(backing, true).put(ds).build();
        assertFalse(IntoPageWriter.Factory.canUsePreBuiltBatch(project, "metrics"));
    }

    public void testMissingNameMatchingV2TemplateIsNotEligible() {
        ComposableIndexTemplate template = ComposableIndexTemplate.builder().indexPatterns(List.of("metrics-*")).build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put("my-template", template).build();
        assertFalse(IntoPageWriter.Factory.canUsePreBuiltBatch(project, "metrics-logs-123"));
    }

    public void testMissingIndexWithNoTemplateIsEligible() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        assertTrue(IntoPageWriter.Factory.canUsePreBuiltBatch(project, "new-dest"));
    }
}
