/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

public class InferenceIndexTests extends ESTestCase {

    private static ClusterState clusterState(ProjectMetadata project) {
        var nodes = DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("node")).build();
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(Metadata.builder().put(project).build()).build();
    }

    private static ClusterState mixedVersionClusterState(ProjectMetadata project) {
        var oldNode = DiscoveryNodeUtils.builder("old-node").version(VersionInformation.inferVersions(Version.V_8_9_0)).build();
        var newNode = DiscoveryNodeUtils.create("new-node");
        var nodes = DiscoveryNodes.builder().add(oldNode).add(newNode).build();
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(Metadata.builder().put(project).build()).build();
    }

    private static IndexMetadata indexWithMappings(String indexName, String mappingsJson) {
        return IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", jsonToMap(mappingsJson)))
            .build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> jsonToMap(String json) {
        return (Map<String, Object>) XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2().get("_doc");
    }

    public void testInferenceIndexHasV4Mappings_GivenIndexDoesNotExist() {
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        assertTrue(InferenceIndex.inferenceIndexHasV4Mappings(clusterState(project)));
    }

    public void testInferenceIndexHasV4Mappings_GivenIndexDoesNotExistInMixedVersionCluster() {
        // In a mixed-version cluster, the old master may create the index with v3 mappings,
        // so we must conservatively return false.
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        assertFalse(InferenceIndex.inferenceIndexHasV4Mappings(mixedVersionClusterState(project)));
    }

    public void testInferenceIndexHasV4Mappings_GivenV4Mappings() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(indexWithMappings(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV4()), false)
            .build();

        assertTrue(InferenceIndex.inferenceIndexHasV4Mappings(clusterState(project)));
    }

    public void testInferenceIndexHasV4Mappings_GivenV3Mappings() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(indexWithMappings(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3()), false)
            .build();

        assertFalse(InferenceIndex.inferenceIndexHasV4Mappings(clusterState(project)));
    }

    public void testInferenceIndexHasV4Mappings_GivenV2Mappings() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(indexWithMappings(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV2()), false)
            .build();

        assertFalse(InferenceIndex.inferenceIndexHasV4Mappings(clusterState(project)));
    }

    public void testInferenceIndexHasV4Mappings_GivenV1Mappings() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(indexWithMappings(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV1()), false)
            .build();

        assertFalse(InferenceIndex.inferenceIndexHasV4Mappings(clusterState(project)));
    }

    public void testInferenceIndexHasV4Mappings_GivenMappingHasNoMeta() {
        String mappingsWithNoMeta = """
            {
              "_doc": {
                "dynamic": "strict",
                "properties": { "model_id": { "type": "keyword" } }
              }
            }
            """;
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(indexWithMappings(InferenceIndex.INDEX_NAME, mappingsWithNoMeta), false)
            .build();

        assertFalse(InferenceIndex.inferenceIndexHasV4Mappings(clusterState(project)));
    }

    public void testInferenceIndexHasV4Mappings_GivenMappingMetaHasNoVersionKey() {
        String mappingsWithNoVersionKey = """
            {
              "_doc": {
                "_meta": { "some_other_key": 1 },
                "dynamic": "strict",
                "properties": { "model_id": { "type": "keyword" } }
              }
            }
            """;
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(indexWithMappings(InferenceIndex.INDEX_NAME, mappingsWithNoVersionKey), false)
            .build();

        assertFalse(InferenceIndex.inferenceIndexHasV4Mappings(clusterState(project)));
    }

    public void testInferenceIndexHasV4Mappings_GivenMigratedIndexWithV4Mappings() {
        // Simulates a system index migration: concrete index is ".inference-reindexed-for-10",
        // with ".inference" as an alias pointing to it — mirroring what SystemIndexMigrator does.
        String migratedIndexName = ".inference-reindexed-for-10";
        IndexMetadata migratedIndex = IndexMetadata.builder(migratedIndexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", jsonToMap(InferenceIndex.mappingsV4())))
            .putAlias(AliasMetadata.builder(InferenceIndex.INDEX_NAME).build())
            .build();

        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(migratedIndex, false).build();

        // projectMetadata.index(".inference") returns null here because ".inference" is an alias,
        // not a concrete name. The method must fall through to the indicesLookup resolution path.
        assertNull(project.index(InferenceIndex.INDEX_NAME));
        assertTrue(InferenceIndex.inferenceIndexHasV4Mappings(clusterState(project)));
    }

    public void testInferenceIndexHasV4Mappings_GivenMigratedIndexWithV3Mappings() {
        // Same migration scenario but the concrete index still has v3 mappings (pre-mapping-upgrade).
        String migratedIndexName = ".inference-reindexed-for-10";
        IndexMetadata migratedIndex = IndexMetadata.builder(migratedIndexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", jsonToMap(InferenceIndex.mappingsV3())))
            .putAlias(AliasMetadata.builder(InferenceIndex.INDEX_NAME).build())
            .build();

        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(migratedIndex, false).build();

        assertNull(project.index(InferenceIndex.INDEX_NAME));
        assertFalse(InferenceIndex.inferenceIndexHasV4Mappings(clusterState(project)));
    }
}
