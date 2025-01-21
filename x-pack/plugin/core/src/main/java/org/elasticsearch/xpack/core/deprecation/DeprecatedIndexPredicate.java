/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.util.function.Predicate;

public class DeprecatedIndexPredicate {

    public static final IndexVersion MINIMUM_WRITEABLE_VERSION_AFTER_UPGRADE = IndexVersions.UPGRADE_TO_LUCENE_10_0_0;

    /*
     * This predicate allows through only indices that were created with a previous lucene version, meaning that they need to be reindexed
     * in order to be writable in the _next_ lucene version.
     *
     * It ignores searchable snapshots as they are not writable.
     */
    public static Predicate<Index> getReindexRequiredPredicate(Metadata metadata) {
        return index -> {
            IndexMetadata indexMetadata = metadata.index(index);
            return reindexRequired(indexMetadata);
        };
    }

    public static boolean reindexRequired(IndexMetadata indexMetadata) {
        return creationVersionBeforeMinimumWritableVersion(indexMetadata)
            && isNotSearchableSnapshot(indexMetadata)
            && isNotClosed(indexMetadata)
            && isNotVerifiedReadOnly(indexMetadata);
    }

    private static boolean isNotVerifiedReadOnly(IndexMetadata indexMetadata) {
        // no need to check blocks.
        return MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.get(indexMetadata.getSettings()) == false;
    }

    private static boolean isNotSearchableSnapshot(IndexMetadata indexMetadata) {
        return indexMetadata.isSearchableSnapshot() == false;
    }

    private static boolean creationVersionBeforeMinimumWritableVersion(IndexMetadata metadata) {
        return metadata.getCreationVersion().before(MINIMUM_WRITEABLE_VERSION_AFTER_UPGRADE);
    }

    private static boolean isNotClosed(IndexMetadata indexMetadata) {
        return indexMetadata.getState().equals(IndexMetadata.State.CLOSE) == false;
    }

}
