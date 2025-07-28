/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.task;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.indices.SystemIndices;

import java.util.Objects;
import java.util.stream.Stream;

class SystemResourceMigrationFactory {
    /**
     * Convenience factory method holding the logic for creating instances from a Feature object.
     * @param feature The feature that is being migrated
     * @param metadata The current metadata, as index migration depends on the current state of the cluster.
     * @param indexScopedSettings This is necessary to make adjustments to the indices settings for unmanaged indices.
     * @return A {@link Stream} of {@link SystemIndexMigrationInfo}s that represent all the indices the given feature currently owns.
     */
    static Stream<SystemResourceMigrationInfo> fromFeature(
        SystemIndices.Feature feature,
        ProjectMetadata metadata,
        IndexScopedSettings indexScopedSettings
    ) {
        return Stream.concat(
            getSystemIndicesMigrationInfos(feature, metadata, indexScopedSettings),
            getSystemDataStreamsMigrationInfos(feature, metadata)
        );
    }

    private static Stream<SystemIndexMigrationInfo> getSystemIndicesMigrationInfos(
        SystemIndices.Feature feature,
        ProjectMetadata metadata,
        IndexScopedSettings indexScopedSettings
    ) {
        return feature.getIndexDescriptors()
            .stream()
            .flatMap(descriptor -> descriptor.getMatchingIndices(metadata).stream().map(metadata::index).filter(imd -> {
                assert imd != null
                    : "got null IndexMetadata for index in system descriptor ["
                        + descriptor.getIndexPattern()
                        + "] in feature ["
                        + feature.getName()
                        + "]";
                return Objects.nonNull(imd);
            }).map(imd -> SystemIndexMigrationInfo.build(imd, descriptor, feature, indexScopedSettings)));
    }

    private static Stream<SystemDataStreamMigrationInfo> getSystemDataStreamsMigrationInfos(
        SystemIndices.Feature feature,
        ProjectMetadata metadata
    ) {
        return feature.getDataStreamDescriptors().stream().map(descriptor -> {
            DataStream dataStream = metadata.dataStreams().get(descriptor.getDataStreamName());
            return dataStream != null ? SystemDataStreamMigrationInfo.build(dataStream, descriptor, feature) : null;
        }).filter(Objects::nonNull);
    }
}
