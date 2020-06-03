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
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.Index;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toUnmodifiableList;

public class SystemIndices {

    private final CharacterRunAutomaton runAutomaton;
    private final Collection<SystemIndexDescriptor> systemIndexDescriptors;

    public SystemIndices(Map<String, Collection<SystemIndexDescriptor>> systemIndexDescriptorMap) {
        this.systemIndexDescriptors = systemIndexDescriptorMap.values()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableList());
        this.runAutomaton = buildCharacterRunAutomaton(systemIndexDescriptors);
    }

    public boolean isSystemIndex(Index index) {
        return runAutomaton.run(index.getName());
    }

    public Collection<SystemIndexDescriptor> findMatchingDescriptors(String name) {
        return systemIndexDescriptors.stream()
            .filter(descriptor -> descriptor.matchesIndexPattern(name))
            .collect(toUnmodifiableList());
    }

    private static CharacterRunAutomaton buildCharacterRunAutomaton(Collection<SystemIndexDescriptor> descriptors) {
        Optional<Automaton> automaton = descriptors.stream()
            .map(descriptor -> Regex.simpleMatchToAutomaton(descriptor.getIndexPattern()))
            .reduce(Operations::union);
        // TODO make it minimal?
        return new CharacterRunAutomaton(automaton.orElse(Automata.makeEmpty()));
    }

    /**
     * Given a collection of {@link SystemIndexDescriptor}s and their sources, checks to see if the index patterns of the listed
     * descriptors overlap with any of the other patterns. If any do, throws an exception.
     *
     * @param sourceToDescriptors A map of source (plugin) names to the SystemIndexDescriptors they provide.
     * @throws IllegalStateException Thrown if any of the index patterns overlaps with another.
     */
    public static void checkForOverlappingPatterns(Map<String, Collection<SystemIndexDescriptor>> sourceToDescriptors) {
        List<Tuple<String, SystemIndexDescriptor>> sourceDescriptorPair = sourceToDescriptors.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream().map(descriptor -> new Tuple<>(entry.getKey(), descriptor)))
            .sorted(Comparator.comparing(d -> d.v1() + ":" + d.v2().getIndexPattern())) // Consistent ordering -> consistent error message
            .collect(Collectors.toList());

        // This is O(n^2) with the number of system index descriptors, and each check is quadratic with the number of states in the
        // automaton, but the absolute number of system index descriptors should be quite small (~10s at most), and the number of states
        // per pattern should be low as well. If these assumptions change, this might need to be reworked.
        sourceDescriptorPair.forEach(descriptorToCheck -> {
            List<Tuple<String, SystemIndexDescriptor>> descriptorsMatchingThisPattern = sourceDescriptorPair.stream()

                .filter(d -> descriptorToCheck.v2() != d.v2()) // Exclude the pattern currently being checked
                .filter(d -> overlaps(descriptorToCheck.v2(), d.v2()))
                .collect(Collectors.toList());
            if (descriptorsMatchingThisPattern.isEmpty() == false) {
                StringBuilder errorMessage = new StringBuilder();
                errorMessage.append("a system index descriptor [")
                    .append(descriptorToCheck.v2())
                    .append("] from plugin [")
                    .append(descriptorToCheck.v1())
                    .append("] overlaps with other system index descriptors: [")
                    .append(descriptorsMatchingThisPattern.stream()
                        .map(descriptor -> descriptor.v2() + " from plugin [" + descriptor.v1() + "]")
                        .collect(Collectors.joining(", ")));
                throw new IllegalStateException(errorMessage.toString());
            }
        });
    }

    private static boolean overlaps(SystemIndexDescriptor a1, SystemIndexDescriptor a2) {
        Automaton a1Automaton = Regex.simpleMatchToAutomaton(a1.getIndexPattern());
        Automaton a2Automaton = Regex.simpleMatchToAutomaton(a2.getIndexPattern());
        return Operations.isEmpty(Operations.intersection(a1Automaton, a2Automaton)) == false;
    }
}
