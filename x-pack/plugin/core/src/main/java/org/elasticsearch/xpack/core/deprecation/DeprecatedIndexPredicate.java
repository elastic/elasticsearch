/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class DeprecatedIndexPredicate {

    public static final IndexVersion MINIMUM_WRITEABLE_VERSION_AFTER_UPGRADE = IndexVersions.UPGRADE_TO_LUCENE_10_0_0;

    /**
     * This predicate allows through only indices that were created with a previous lucene version, meaning that they need to be reindexed
     * in order to be writable in the _next_ lucene version. It excludes searchable snapshots as they are not writable.
     *
     * It ignores searchable snapshots as they are not writable.
     *
     * @param metadata the cluster metadata
     * @param filterToBlockedStatus if true, only indices that are write blocked will be returned,
     *                              if false, only those without a block are returned
     * @param includeSystem if true, all indices including system will be returned,
     *                              if false, only non-system indices are returned
     * @return a predicate that returns true for indices that need to be reindexed
     */
    public static Predicate<Index> getReindexRequiredPredicate(
        ProjectMetadata metadata,
        boolean filterToBlockedStatus,
        boolean includeSystem
    ) {
        return index -> {
            IndexMetadata indexMetadata = metadata.index(index);
            return reindexRequired(indexMetadata, filterToBlockedStatus, includeSystem);
        };
    }

    /**
     * This method check if the indices that were created with a previous lucene version, meaning that they need to be reindexed
     * in order to be writable in the _next_ lucene version. It excludes searchable snapshots as they are not writable.
     *
     * @param indexMetadata the index metadata
     * @param filterToBlockedStatus if true, only indices that are write blocked will be returned,
     *                              if false, only those without a block are returned
     * @param includeSystem if true, all indices including system will be returned,
     *                              if false, only non-system indices are returned
     * @return returns true for indices that need to be reindexed
     */
    public static boolean reindexRequired(IndexMetadata indexMetadata, boolean filterToBlockedStatus, boolean includeSystem) {
        return creationVersionBeforeMinimumWritableVersion(indexMetadata)
            && (includeSystem || isNotSystem(indexMetadata))
            && isNotSearchableSnapshot(indexMetadata)
            && matchBlockedStatus(indexMetadata, filterToBlockedStatus);
    }

    /**
     * This method checks if this index is on the current transport version for the current minor release version.
     *
     * @param indexMetadata the index metadata
     * @param filterToBlockedStatus if true, only indices that are write blocked will be returned,
     *                              if false, only those without a block are returned
     * @param includeSystem if true, all indices including system will be returned,
     *                              if false, only non-system indices are returned
     * @return returns true for indices that need to be reindexed
     */
    public static boolean reindexRequiredForTransportVersion(
        IndexMetadata indexMetadata,
        boolean filterToBlockedStatus,
        boolean includeSystem
    ) {
        return transportVersionBeforeCurrentMinorRelease(indexMetadata)
            && (includeSystem || isNotSystem(indexMetadata))
            && isNotSearchableSnapshot(indexMetadata)
            && matchBlockedStatus(indexMetadata, filterToBlockedStatus);
    }

    /**
     * This method checks if this index requires reindexing based on if it has percolator fields older than the current transport version
     * for the current minor release.
     *
     * @param indexMetadata the index metadata
     * @param filterToBlockedStatus if true, only indices that are write blocked will be returned,
     *                              if false, only those without a block are returned
     * @param includeSystem if true, all indices including system will be returned,
     *                              if false, only non-system indices are returned
     * @return returns a message as a string for each incompatible percolator field found
     */
    public static List<String> reindexRequiredForPecolatorFields(
        IndexMetadata indexMetadata,
        boolean filterToBlockedStatus,
        boolean includeSystem
    ) {
        List<String> percolatorIncompatibleFieldMappings = new ArrayList<>();
        if (reindexRequiredForTransportVersion(indexMetadata, filterToBlockedStatus, includeSystem) && indexMetadata.mapping() != null) {
            percolatorIncompatibleFieldMappings.addAll(
                findInPropertiesRecursively(
                    indexMetadata.mapping().type(),
                    indexMetadata.mapping().sourceAsMap(),
                    property -> "percolator".equals(property.get("type")),
                    (type, entry) -> "Field [" + entry.getKey() + "] is of type [" + indexMetadata.mapping().type() + "]",
                    "",
                    ""
                )
            );
        }
        return percolatorIncompatibleFieldMappings;
    }

    private static boolean isNotSystem(IndexMetadata indexMetadata) {
        return indexMetadata.isSystem() == false;
    }

    private static boolean isNotSearchableSnapshot(IndexMetadata indexMetadata) {
        return indexMetadata.isSearchableSnapshot() == false;
    }

    private static boolean creationVersionBeforeMinimumWritableVersion(IndexMetadata metadata) {
        return metadata.getCreationVersion().before(MINIMUM_WRITEABLE_VERSION_AFTER_UPGRADE);
    }

    private static boolean matchBlockedStatus(IndexMetadata indexMetadata, boolean filterToBlockedStatus) {
        return MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.get(indexMetadata.getSettings()) == filterToBlockedStatus;
    }

    private static boolean transportVersionBeforeCurrentMinorRelease(IndexMetadata indexMetadata) {
        // We divide each transport version by 1000 to get the base id.
        return indexMetadata.getTransportVersion().id() / 1000 < TransportVersion.current().id() / 1000;
    }

    /**
     * iterates through the "properties" field of mappings and returns any predicates that match in the
     * form of issue-strings.
     *
     * @param type the document type
     * @param parentMap the mapping to read properties from
     * @param predicate the predicate to check against for issues, issue is returned if predicate evaluates to true
     * @param fieldFormatter a function that takes a type and mapping field entry and returns a formatted field representation
     * @return a list of issues found in fields
     */
    @SuppressWarnings("unchecked")
    public static List<String> findInPropertiesRecursively(
        String type,
        Map<String, Object> parentMap,
        Function<Map<?, ?>, Boolean> predicate,
        BiFunction<String, Map.Entry<?, ?>, String> fieldFormatter,
        String fieldBeginMarker,
        String fieldEndMarker
    ) {
        List<String> issues = new ArrayList<>();
        Map<?, ?> properties = (Map<?, ?>) parentMap.get("properties");
        if (properties == null) {
            return issues;
        }
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            Map<String, Object> valueMap = (Map<String, Object>) entry.getValue();
            if (predicate.apply(valueMap)) {
                issues.add(fieldBeginMarker + fieldFormatter.apply(type, entry) + fieldEndMarker);
            }

            Map<?, ?> values = (Map<?, ?>) valueMap.get("fields");
            if (values != null) {
                for (Map.Entry<?, ?> multifieldEntry : values.entrySet()) {
                    Map<String, Object> multifieldValueMap = (Map<String, Object>) multifieldEntry.getValue();
                    if (predicate.apply(multifieldValueMap)) {
                        issues.add(
                            fieldBeginMarker
                                + fieldFormatter.apply(type, entry)
                                + ", multifield: "
                                + multifieldEntry.getKey()
                                + fieldEndMarker
                        );
                    }
                    if (multifieldValueMap.containsKey("properties")) {
                        issues.addAll(
                            findInPropertiesRecursively(
                                type,
                                multifieldValueMap,
                                predicate,
                                fieldFormatter,
                                fieldBeginMarker,
                                fieldEndMarker
                            )
                        );
                    }
                }
            }
            if (valueMap.containsKey("properties")) {
                issues.addAll(findInPropertiesRecursively(type, valueMap, predicate, fieldFormatter, fieldBeginMarker, fieldEndMarker));
            }
        }

        return issues;
    }
}
