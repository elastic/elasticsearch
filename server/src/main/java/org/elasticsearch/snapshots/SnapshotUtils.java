/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
            if (!indexOrPattern.isEmpty()) {
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
            if (indexOrPattern.isEmpty() || !Regex.isSimpleMatchPattern(indexOrPattern)) {
                if (!availableIndices.contains(indexOrPattern)) {
                    if (!indicesOptions.ignoreUnavailable()) {
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
            if (!found && !indicesOptions.allowNoIndices()) {
                throw new IndexNotFoundException(indexOrPattern);
            }
        }
        if (result == null) {
            return List.of(selectedIndices);
        }
        return List.copyOf(result);
    }

    /**
     * Finds the {@link IndexId}s in the given source snapshot that match the given index patterns for a snapshot clone operation.
     *
     * @param sourceSnapshotId source snapshot id
     * @param targetSnapshot   target snapshot that the clone operation would create
     * @param repositoryData   repository data of the repository that the clone
     * @param indexPatterns    index patterns to clone from source- to target snapshot
     * @return list of index ids to clone
     * @throws SnapshotException on failure to find concrete request index ids or any index ids
     */
    static List<IndexId> findIndexIdsToClone(SnapshotId sourceSnapshotId, Snapshot targetSnapshot, RepositoryData repositoryData,
                                             String... indexPatterns) {
        final Map<String, IndexId> indicesInSource = repositoryData.getIndices().values()
                .stream()
                .filter(indexId -> repositoryData.getSnapshots(indexId).contains(sourceSnapshotId))
                .collect(Collectors.toMap(IndexId::getName, Function.identity()));
        final List<IndexId> matchingIndices = new ArrayList<>();
        for (String indexNameOrPattern : indexPatterns) {
            if (Regex.isSimpleMatchPattern(indexNameOrPattern)) {
                for (IndexId indexId : indicesInSource.values()) {
                    if (Regex.simpleMatch(indexNameOrPattern, indexId.getName())) {
                        matchingIndices.add(indexId);
                    }
                }
            } else {
                final IndexId foundIndexId = indicesInSource.get(indexNameOrPattern);
                if (foundIndexId == null) {
                    throw new SnapshotException(targetSnapshot, "No index [" + indexNameOrPattern + "] found in the source snapshot ["
                            + sourceSnapshotId + "]");
                }
                matchingIndices.add(foundIndexId);
            }
        }
        if (matchingIndices.isEmpty()) {
            throw new SnapshotException(targetSnapshot, "No indices in the source snapshot [" + sourceSnapshotId +
                    "] matched requested pattern [" + Strings.arrayToCommaDelimitedString(indexPatterns) + "]");
        }
        return matchingIndices;
    }
}
