/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.TaskResultsService;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.elasticsearch.tasks.TaskResultsService.TASK_INDEX;

/**
 * This class holds the {@link SystemIndexDescriptor} objects that represent system indices the
 * node knows about. Methods for determining if an index should be a system index are also provided
 * to reduce the locations within the code that need to deal with {@link SystemIndexDescriptor}s.
 */
public class SystemIndices {
    private static final Map<String, Collection<SystemIndexDescriptor>> SERVER_SYSTEM_INDEX_DESCRIPTORS = Map.of(
        TaskResultsService.class.getName(), List.of(new SystemIndexDescriptor(TASK_INDEX + "*", "Task Result Index"))
    );

    private final CharacterRunAutomaton runAutomaton;
    private final Collection<SystemIndexDescriptor> systemIndexDescriptors;

    public SystemIndices(Map<String, Collection<SystemIndexDescriptor>> pluginAndModulesDescriptors) {
        final Map<String, Collection<SystemIndexDescriptor>> descriptorsMap = buildSystemIndexDescriptorMap(pluginAndModulesDescriptors);
        checkForOverlappingPatterns(descriptorsMap);
        this.systemIndexDescriptors = descriptorsMap.values().stream().flatMap(Collection::stream).collect(Collectors.toUnmodifiableList());
        checkForDuplicateAliases(this.systemIndexDescriptors);
        this.runAutomaton = buildCharacterRunAutomaton(systemIndexDescriptors);
    }

    private void checkForDuplicateAliases(Collection<SystemIndexDescriptor> descriptors) {
        final Map<String, Integer> aliasCounts = new HashMap<>();

        for (SystemIndexDescriptor descriptor : descriptors) {
            final String aliasName = descriptor.getAliasName();
            if (aliasName != null) {
                aliasCounts.compute(aliasName, (alias, existingCount) -> 1 + (existingCount == null ? 0 : existingCount));
            }
        }

        final List<String> duplicateAliases = aliasCounts.entrySet()
            .stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Map.Entry::getKey)
            .sorted()
            .collect(Collectors.toList());

        if (duplicateAliases.isEmpty() == false) {
            throw new IllegalStateException("Found aliases associated with multiple system index descriptors: " + duplicateAliases + "");
        }
    }

    /**
     * Determines whether a given index is a system index by comparing its name to the collection of loaded {@link SystemIndexDescriptor}s
     * @param index the {@link Index} object to check against loaded {@link SystemIndexDescriptor}s
     * @return true if the {@link Index}'s name matches a pattern from a {@link SystemIndexDescriptor}
     */
    public boolean isSystemIndex(Index index) {
        return isSystemIndex(index.getName());
    }

    /**
     * Determines whether a given index is a system index by comparing its name to the collection of loaded {@link SystemIndexDescriptor}s
     * @param indexName the index name to check against loaded {@link SystemIndexDescriptor}s
     * @return true if the index name matches a pattern from a {@link SystemIndexDescriptor}
     */
    public boolean isSystemIndex(String indexName) {
        return runAutomaton.run(indexName);
    }

    /**
     * Finds a single matching {@link SystemIndexDescriptor}, if any, for the given index name.
     * @param name the name of the index
     * @return The matching {@link SystemIndexDescriptor} or {@code null} if no descriptor is found
     * @throws IllegalStateException if multiple descriptors match the name
     */
    public @Nullable SystemIndexDescriptor findMatchingDescriptor(String name) {
        final List<SystemIndexDescriptor> matchingDescriptors = systemIndexDescriptors.stream()
            .filter(descriptor -> descriptor.matchesIndexPattern(name))
            .collect(toUnmodifiableList());

        if (matchingDescriptors.isEmpty()) {
            return null;
        } else if (matchingDescriptors.size() == 1) {
            return matchingDescriptors.get(0);
        } else {
            // This should be prevented by failing on overlapping patterns at startup time, but is here just in case.
            StringBuilder errorMessage = new StringBuilder()
                .append("index name [")
                .append(name)
                .append("] is claimed as a system index by multiple system index patterns: [")
                .append(matchingDescriptors.stream()
                    .map(descriptor -> "pattern: [" + descriptor.getIndexPattern() +
                        "], description: [" + descriptor.getDescription() + "]").collect(Collectors.joining("; ")));
            // Throw AssertionError if assertions are enabled, or a regular exception otherwise:
            assert false : errorMessage.toString();
            throw new IllegalStateException(errorMessage.toString());
        }
    }

    private static CharacterRunAutomaton buildCharacterRunAutomaton(Collection<SystemIndexDescriptor> descriptors) {
        Optional<Automaton> automaton = descriptors.stream()
            .map(descriptor -> SystemIndexDescriptor.buildAutomaton(descriptor.getIndexPattern(), descriptor.getAliasName()))
            .reduce(Operations::union);
        return new CharacterRunAutomaton(MinimizationOperations.minimize(automaton.orElse(Automata.makeEmpty()), Integer.MAX_VALUE));
    }

    /**
     * Given a collection of {@link SystemIndexDescriptor}s and their sources, checks to see if the index patterns of the listed
     * descriptors overlap with any of the other patterns. If any do, throws an exception.
     *
     * @param sourceToDescriptors A map of source (plugin) names to the SystemIndexDescriptors they provide.
     * @throws IllegalStateException Thrown if any of the index patterns overlaps with another.
     */
    static void checkForOverlappingPatterns(Map<String, Collection<SystemIndexDescriptor>> sourceToDescriptors) {
        List<Tuple<String, SystemIndexDescriptor>> sourceDescriptorPair = sourceToDescriptors.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream().map(descriptor -> new Tuple<>(entry.getKey(), descriptor)))
            .sorted(Comparator.comparing(d -> d.v1() + ":" + d.v2().getIndexPattern())) // Consistent ordering -> consistent error message
            .collect(Collectors.toUnmodifiableList());

        // This is O(n^2) with the number of system index descriptors, and each check is quadratic with the number of states in the
        // automaton, but the absolute number of system index descriptors should be quite small (~10s at most), and the number of states
        // per pattern should be low as well. If these assumptions change, this might need to be reworked.
        sourceDescriptorPair.forEach(descriptorToCheck -> {
            List<Tuple<String, SystemIndexDescriptor>> descriptorsMatchingThisPattern = sourceDescriptorPair.stream()

                .filter(d -> descriptorToCheck.v2() != d.v2()) // Exclude the pattern currently being checked
                .filter(d -> overlaps(descriptorToCheck.v2(), d.v2()))
                .collect(Collectors.toUnmodifiableList());
            if (descriptorsMatchingThisPattern.isEmpty() == false) {
                throw new IllegalStateException("a system index descriptor [" + descriptorToCheck.v2() + "] from [" +
                    descriptorToCheck.v1() + "] overlaps with other system index descriptors: [" +
                    descriptorsMatchingThisPattern.stream()
                        .map(descriptor -> descriptor.v2() + " from [" + descriptor.v1() + "]")
                        .collect(Collectors.joining(", ")));
            }
        });
    }

    private static boolean overlaps(SystemIndexDescriptor a1, SystemIndexDescriptor a2) {
        Automaton a1Automaton = SystemIndexDescriptor.buildAutomaton(a1.getIndexPattern(), null);
        Automaton a2Automaton = SystemIndexDescriptor.buildAutomaton(a2.getIndexPattern(), null);
        return Operations.isEmpty(Operations.intersection(a1Automaton, a2Automaton)) == false;
    }

    private static Map<String, Collection<SystemIndexDescriptor>> buildSystemIndexDescriptorMap(
        Map<String, Collection<SystemIndexDescriptor>> pluginAndModulesMap) {
        final Map<String, Collection<SystemIndexDescriptor>> map =
            new HashMap<>(pluginAndModulesMap.size() + SERVER_SYSTEM_INDEX_DESCRIPTORS.size());
        map.putAll(pluginAndModulesMap);
        // put the server items last since we expect less of them
        SERVER_SYSTEM_INDEX_DESCRIPTORS.forEach((source, descriptors) -> {
            if (map.putIfAbsent(source, descriptors) != null) {
                throw new IllegalArgumentException("plugin or module attempted to define the same source [" + source +
                    "] as a built-in system index");
            }
        });
        return Map.copyOf(map);
    }

    Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return this.systemIndexDescriptors;
    }
}
