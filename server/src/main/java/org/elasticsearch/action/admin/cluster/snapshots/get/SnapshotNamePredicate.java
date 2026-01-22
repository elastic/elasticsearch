/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.repositories.ResolvedRepositories;
import org.elasticsearch.snapshots.SnapshotMissingException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Represents a filter on snapshots by name, including some special values such as {@code _all} and {@code _current}, as supported by
 * {@link TransportGetSnapshotsAction}.
 */
public interface SnapshotNamePredicate {
    /**
     * @return Whether a snapshot with the given name should be selected.
     */
    boolean test(String snapshotName, boolean isCurrentSnapshot);

    /**
     * @return the snapshot names that must be present in a repository. If one of these snapshots is missing then this repository should
     * yield a {@link SnapshotMissingException} rather than any snapshots.
     */
    Collection<String> requiredNames();

    /**
     * A {@link SnapshotNamePredicate} which matches all snapshots (and requires no specific names).
     */
    SnapshotNamePredicate MATCH_ALL = new SnapshotNamePredicate() {
        @Override
        public boolean test(String snapshotName, boolean isCurrentSnapshot) {
            return true;
        }

        @Override
        public Collection<String> requiredNames() {
            return Collections.emptyList();
        }
    };

    /**
     * A {@link SnapshotNamePredicate} which matches all currently-executing snapshots (and requires no specific names).
     */
    SnapshotNamePredicate MATCH_CURRENT_ONLY = new SnapshotNamePredicate() {
        @Override
        public boolean test(String snapshotName, boolean isCurrentSnapshot) {
            return isCurrentSnapshot;
        }

        @Override
        public Collection<String> requiredNames() {
            return Collections.emptyList();
        }
    };

    /**
     * @return a {@link SnapshotNamePredicate} from the given {@link GetSnapshotsRequest} parameters.
     */
    static SnapshotNamePredicate forSnapshots(boolean ignoreUnavailable, String[] snapshots) {
        if (ResolvedRepositories.isMatchAll(snapshots)) {
            return MATCH_ALL;
        }

        if (snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0])) {
            return MATCH_CURRENT_ONLY;
        }

        final List<String> includesBuilder = new ArrayList<>(snapshots.length);
        final List<String> excludesBuilder = new ArrayList<>(snapshots.length);
        final Set<String> requiredNamesBuilder = ignoreUnavailable ? null : Sets.newHashSetWithExpectedSize(snapshots.length);
        boolean seenCurrent = false;
        boolean seenWildcard = false;
        for (final var snapshot : snapshots) {
            if (seenWildcard && snapshot.length() > 1 && snapshot.startsWith("-")) {
                excludesBuilder.add(snapshot.substring(1));
            } else {
                if (Regex.isSimpleMatchPattern(snapshot)) {
                    seenWildcard = true;
                    includesBuilder.add(snapshot);
                } else if (GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshot)) {
                    seenCurrent = true;
                    seenWildcard = true;
                } else {
                    if (ignoreUnavailable == false) {
                        requiredNamesBuilder.add(snapshot);
                    }
                    includesBuilder.add(snapshot);
                }
            }
        }

        final boolean includeCurrent = seenCurrent;
        final String[] includes = includesBuilder.toArray(Strings.EMPTY_ARRAY);
        final String[] excludes = excludesBuilder.toArray(Strings.EMPTY_ARRAY);
        final Set<String> requiredNames = requiredNamesBuilder == null ? Set.of() : Set.copyOf(requiredNamesBuilder);

        return new SnapshotNamePredicate() {
            @Override
            public boolean test(String snapshotName, boolean isCurrentSnapshot) {
                return ((includeCurrent && isCurrentSnapshot) || Regex.simpleMatch(includes, snapshotName))
                    && (Regex.simpleMatch(excludes, snapshotName) == false);
            }

            @Override
            public Collection<String> requiredNames() {
                return requiredNames;
            }
        };
    }
}
