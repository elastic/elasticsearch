/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.TransformMetadata;

import static org.hamcrest.Matchers.equalTo;

public class TransformMetadataTests extends AbstractChunkedSerializingTestCase<TransformMetadata> {

    @Override
    protected TransformMetadata createTestInstance() {
        return new TransformMetadata.Builder().resetMode(randomBoolean()).upgradeMode(randomBoolean()).build();
    }

    @Override
    protected Writeable.Reader<TransformMetadata> instanceReader() {
        return TransformMetadata::new;
    }

    @Override
    protected TransformMetadata doParseInstance(XContentParser parser) {
        return TransformMetadata.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected TransformMetadata mutateInstance(TransformMetadata instance) {
        return new TransformMetadata.Builder().resetMode(instance.resetMode() == false)
            .upgradeMode(instance.upgradeMode() == false)
            .build();
    }

    public void testTransformMetadataFromClusterState() {
        var expectedTransformMetadata = new TransformMetadata.Builder().resetMode(true).upgradeMode(true).build();
        var projectId = randomUniqueProjectId();
        var clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(
                Metadata.builder().put(ProjectMetadata.builder(projectId).putCustom(TransformMetadata.TYPE, expectedTransformMetadata))
            )
            .build();

        assertThat(TransformMetadata.transformMetadata(clusterState, projectId), equalTo(expectedTransformMetadata));
        assertThat(TransformMetadata.getTransformMetadata(clusterState), equalTo(expectedTransformMetadata));
    }

    public void testTransformMetadataFromMissingClusterState() {
        assertThat(TransformMetadata.transformMetadata(null, randomUniqueProjectId()), equalTo(TransformMetadata.EMPTY_METADATA));
        assertThat(TransformMetadata.getTransformMetadata(null), equalTo(TransformMetadata.EMPTY_METADATA));
    }

    public void testTransformMetadataFromMissingProjectId() {
        assertThat(
            TransformMetadata.transformMetadata(ClusterState.builder(new ClusterName("_name")).build(), null),
            equalTo(TransformMetadata.EMPTY_METADATA)
        );
    }

    public void testTransformMetadataWhenAbsentFromClusterState() {
        var projectId = randomUniqueProjectId();
        var clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId)))
            .build();

        assertThat(TransformMetadata.transformMetadata(clusterState, projectId), equalTo(TransformMetadata.EMPTY_METADATA));
        assertThat(TransformMetadata.getTransformMetadata(clusterState), equalTo(TransformMetadata.EMPTY_METADATA));
    }
}
