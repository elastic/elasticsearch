/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsPending;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Snapshot utilities
 */
public class SnapshotUtils {

    /**
     * Filters out list of available indices based on the list of selected indices.
     *
     * @param availableIndices list of available indices
     * @param selectedIndices  list of selected indices
     * @param indicesOptions    ignore indices flag
     * @return filtered out indices
     */
    public static List<String> filterIndices(List<String> availableIndices, String[] selectedIndices, IndicesOptions indicesOptions) {
        if (IndexNameExpressionResolver.isAllIndices(Arrays.asList(selectedIndices))) {
            return availableIndices;
        }
        Set<String> result = null;
        for (int i = 0; i < selectedIndices.length; i++) {
            String indexOrPattern = selectedIndices[i];
            boolean add = true;
            if (indexOrPattern.isEmpty() == false) {
                if (availableIndices.contains(indexOrPattern)) {
                    if (result == null) {
                        result = new HashSet<>();
                    }
                    result.add(indexOrPattern);
                    continue;
                }
                if (indexOrPattern.charAt(0) == '+') {
                    add = true;
                    indexOrPattern = indexOrPattern.substring(1);
                    // if its the first, add empty set
                    if (i == 0) {
                        result = new HashSet<>();
                    }
                } else if (indexOrPattern.charAt(0) == '-') {
                    // if its the first, fill it with all the indices...
                    if (i == 0) {
                        result = new HashSet<>(availableIndices);
                    }
                    add = false;
                    indexOrPattern = indexOrPattern.substring(1);
                }
            }
            if (indexOrPattern.isEmpty() || Regex.isSimpleMatchPattern(indexOrPattern) == false) {
                if (availableIndices.contains(indexOrPattern) == false) {
                    if (indicesOptions.ignoreUnavailable() == false) {
                        throw new IndexNotFoundException(indexOrPattern);
                    } else {
                        if (result == null) {
                            // add all the previous ones...
                            result = new HashSet<>(availableIndices.subList(0, i));
                        }
                    }
                } else {
                    if (result != null) {
                        if (add) {
                            result.add(indexOrPattern);
                        } else {
                            result.remove(indexOrPattern);
                        }
                    }
                }
                continue;
            }
            if (result == null) {
                // add all the previous ones...
                result = new HashSet<>(availableIndices.subList(0, i));
            }
            boolean found = false;
            for (String index : availableIndices) {
                if (Regex.simpleMatch(indexOrPattern, index)) {
                    found = true;
                    if (add) {
                        result.add(index);
                    } else {
                        result.remove(index);
                    }
                }
            }
            if (found == false && indicesOptions.allowNoIndices() == false) {
                throw new IndexNotFoundException(indexOrPattern);
            }
        }
        if (result == null) {
            return List.of(selectedIndices);
        }
        return List.copyOf(result);
    }

    static Set<SnapshotId> cloneSources(final ClusterState state) {
        return state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
            .asStream()
            .filter(SnapshotsInProgress.Entry::isClone)
            .map(SnapshotsInProgress.Entry::source)
            .collect(Collectors.toUnmodifiableSet());
    }

    static Set<SnapshotId> restoreSources(final ClusterState state) {
        return StreamSupport.stream(state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).spliterator(), false)
            .map(restore -> restore.snapshot().getSnapshotId())
            .collect(Collectors.toUnmodifiableSet());
    }

    static Set<SnapshotId> deletionsSources(final ClusterState state) {
        return state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
            .getEntries()
            .stream()
            .flatMap(deletion -> deletion.getSnapshots().stream())
            .collect(Collectors.toUnmodifiableSet());
    }

    static void ensureSnapshotNotDeletedOrPendingDeletion(
        final ClusterState currentState,
        final String repositoryName,
        final SnapshotId snapshotId,
        final String reason
    ) {
        final SnapshotDeletionsPending pendingDeletions = currentState.custom(SnapshotDeletionsPending.TYPE);
        if (pendingDeletions != null && pendingDeletions.contains(snapshotId)) {
            throw new ConcurrentSnapshotExecutionException(
                repositoryName,
                snapshotId.getName(),
                "cannot " + reason + " a snapshot already marked as deleted [" + repositoryName + ":" + snapshotId + "]"
            );
        }
        final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
        if (deletionsInProgress != null
            && deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(snapshotId))) {
            throw new ConcurrentSnapshotExecutionException(
                repositoryName,
                snapshotId.getName(),
                "cannot "
                    + reason
                    + " a snapshot while a snapshot deletion is in-progress ["
                    + deletionsInProgress.getEntries().get(0)
                    + "]"
            );
        }
    }
}
