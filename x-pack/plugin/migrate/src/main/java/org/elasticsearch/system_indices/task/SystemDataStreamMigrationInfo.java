/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.task;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;

import java.util.stream.Stream;

final class SystemDataStreamMigrationInfo extends SystemResourceMigrationInfo {
    private final DataStream dataStream;
    private final String dataStreamName;

    private SystemDataStreamMigrationInfo(
        DataStream dataStream,
        String dataStreamName,
        String featureName,
        String origin,
        SystemIndices.Feature owningFeature
    ) {
        super(featureName, origin, owningFeature);
        this.dataStreamName = dataStreamName;
        this.dataStream = dataStream;
    }

    public static SystemDataStreamMigrationInfo build(
        DataStream dataStream,
        SystemDataStreamDescriptor dataStreamDescriptor,
        SystemIndices.Feature feature
    ) {
        return new SystemDataStreamMigrationInfo(
            dataStream,
            dataStreamDescriptor.getDataStreamName(),
            feature.getName(),
            dataStreamDescriptor.getOrigin(),
            feature
        );
    }

    public String getDataStreamName() {
        return dataStreamName;
    }

    @Override
    protected String getCurrentResourceName() {
        return getDataStreamName();
    }

    @Override
    Stream<IndexMetadata> getIndices(ProjectMetadata metadata) {
        return Stream.concat(dataStream.getIndices().stream(), dataStream.getFailureIndices().stream()).map(metadata::getIndexSafe);
    }

    @Override
    boolean isCurrentIndexClosed() {
        // data stream can't be closed
        return false;
    }
}
