/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The result of calling {@link #resolve(ClusterState, String[])} to resolve a description of some snapshot repositories (from a path
 * component of a request to the get-repositories or get-snapshots APIs) against the known repositories in the cluster state: the
 * {@link RepositoryMetadata} for the extant repositories that match the description, together with a list of the parts of the description
 * that failed to match any known repository.
 *
 * @param repositoryMetadata The {@link RepositoryMetadata} for the repositories that matched the description.
 * @param missing            The parts of the description which matched no repositories.
 */
public record ResolvedRepositories(List<RepositoryMetadata> repositoryMetadata, List<String> missing) {

    public static final String ALL_PATTERN = "_all";

    public static boolean isMatchAll(String[] patterns) {
        return patterns.length == 0
            || (patterns.length == 1 && (ALL_PATTERN.equalsIgnoreCase(patterns[0]) || Regex.isMatchAllPattern(patterns[0])));
    }

    public static ResolvedRepositories resolve(ClusterState state, String[] patterns) {
        final var repositories = RepositoriesMetadata.get(state);
        if (isMatchAll(patterns)) {
            return new ResolvedRepositories(repositories.repositories(), List.of());
        }

        final List<String> missingRepositories = new ArrayList<>();
        final List<String> includePatterns = new ArrayList<>();
        final List<String> excludePatterns = new ArrayList<>();
        boolean seenWildcard = false;
        for (final var pattern : patterns) {
            if (seenWildcard && pattern.length() > 1 && pattern.startsWith("-")) {
                excludePatterns.add(pattern.substring(1));
            } else {
                if (Regex.isSimpleMatchPattern(pattern)) {
                    seenWildcard = true;
                } else {
                    if (repositories.repository(pattern) == null) {
                        missingRepositories.add(pattern);
                    }
                }
                includePatterns.add(pattern);
            }
        }
        final var excludes = excludePatterns.toArray(Strings.EMPTY_ARRAY);
        final Set<RepositoryMetadata> repositoryListBuilder = new LinkedHashSet<>(); // to keep insertion order
        for (String repositoryOrPattern : includePatterns) {
            for (RepositoryMetadata repository : repositories.repositories()) {
                if (repositoryListBuilder.contains(repository) == false
                    && Regex.simpleMatch(repositoryOrPattern, repository.name())
                    && Regex.simpleMatch(excludes, repository.name()) == false) {
                    repositoryListBuilder.add(repository);
                }
            }
        }
        return new ResolvedRepositories(List.copyOf(repositoryListBuilder), missingRepositories);
    }

    public boolean hasMissingRepositories() {
        return missing.isEmpty() == false;
    }
}
