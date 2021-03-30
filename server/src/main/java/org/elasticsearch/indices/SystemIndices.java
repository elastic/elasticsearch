/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse.ResetFeatureStateStatus;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.elasticsearch.tasks.TaskResultsService.TASKS_DESCRIPTOR;
import static org.elasticsearch.tasks.TaskResultsService.TASKS_FEATURE_NAME;

/**
 * This class holds the {@link SystemIndexDescriptor} objects that represent system indices the
 * node knows about. Methods for determining if an index should be a system index are also provided
 * to reduce the locations within the code that need to deal with {@link SystemIndexDescriptor}s.
 */
public class SystemIndices {
    private static final Map<String, Feature> SERVER_SYSTEM_INDEX_DESCRIPTORS = Map.of(
        TASKS_FEATURE_NAME, new Feature(TASKS_FEATURE_NAME, "Manages task results", List.of(TASKS_DESCRIPTOR))
    );

    private final CharacterRunAutomaton runAutomaton;
    private final Map<String, Feature> featureDescriptors;
    private final Map<String, CharacterRunAutomaton> productToSystemIndicesMatcher;

    /**
     * Initialize the SystemIndices object
     * @param pluginAndModulesDescriptors A map of this node's feature names to
     *                                    feature objects.
     */
    public SystemIndices(Map<String, Feature> pluginAndModulesDescriptors) {
        featureDescriptors = buildSystemIndexDescriptorMap(pluginAndModulesDescriptors);
        checkForOverlappingPatterns(featureDescriptors);
        checkForDuplicateAliases(this.getSystemIndexDescriptors());
        this.runAutomaton = buildCharacterRunAutomaton(featureDescriptors);
        this.productToSystemIndicesMatcher = getProductToSystemIndicesMap(this.getSystemIndexDescriptors());
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

    private static Map<String, CharacterRunAutomaton> getProductToSystemIndicesMap(Collection<SystemIndexDescriptor> descriptors) {
        Map<String, Automaton> map = descriptors.stream()
            .filter(SystemIndexDescriptor::isExternal)
            .flatMap(descriptor -> descriptor.getAllowedElasticProductOrigins().stream().map(product -> new Tuple<>(product, descriptor)))
            .collect(Collectors.toUnmodifiableMap(Tuple::v1, tuple -> {
                SystemIndexDescriptor descriptor = tuple.v2();
                return SystemIndexDescriptor.buildAutomaton(descriptor.getIndexPattern(), descriptor.getAliasName());
            }, Operations::union));

        return map.entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(Entry::getKey, entry ->
                new CharacterRunAutomaton(MinimizationOperations.minimize(entry.getValue(), Integer.MAX_VALUE))));
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
        final List<SystemIndexDescriptor> matchingDescriptors = featureDescriptors.values().stream()
            .flatMap(feature -> feature.getIndexDescriptors().stream())
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

    /**
     * Builds a predicate that tests if a system index should be accessible based on the provided product name
     * @param product the name of the product that is attempting to access an external system index
     * @return Predicate to check external system index metadata with
     */
    public Predicate<IndexMetadata> getProductSystemIndexMetadataPredicate(String product) {
        final CharacterRunAutomaton automaton = productToSystemIndicesMatcher.get(product);
        if (automaton == null) {
            return indexMetadata -> false;
        }
        return indexMetadata -> automaton.run(indexMetadata.getIndex().getName());
    }

    /**
     * Builds a predicate that tests if a system index name should be accessible based on the provided product name
     * @param product the name of the product that is attempting to access an external system index
     * @return Predicate to check external system index names with
     */
    public Predicate<String> getProductSystemIndexNamePredicate(String product) {
        final CharacterRunAutomaton automaton = productToSystemIndicesMatcher.get(product);
        if (automaton == null) {
            return name -> false;
        }
        return automaton::run;
    }

    public Map<String, Feature> getFeatures() {
        return featureDescriptors;
    }

    private static CharacterRunAutomaton buildCharacterRunAutomaton(Map<String, Feature> descriptors) {
        Optional<Automaton> automaton = descriptors.values().stream()
            .flatMap(feature -> feature.getIndexDescriptors().stream())
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
    static void checkForOverlappingPatterns(Map<String, Feature> sourceToDescriptors) {
        List<Tuple<String, SystemIndexDescriptor>> sourceDescriptorPair = sourceToDescriptors.entrySet().stream()
            .flatMap(entry -> entry.getValue().getIndexDescriptors().stream().map(descriptor -> new Tuple<>(entry.getKey(), descriptor)))
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

    private static Map<String, Feature> buildSystemIndexDescriptorMap(Map<String, Feature> featuresMap) {
        final Map<String, Feature> map = new HashMap<>(featuresMap.size() + SERVER_SYSTEM_INDEX_DESCRIPTORS.size());
        map.putAll(featuresMap);
        // put the server items last since we expect less of them
        SERVER_SYSTEM_INDEX_DESCRIPTORS.forEach((source, feature) -> {
            if (map.putIfAbsent(source, feature) != null) {
                throw new IllegalArgumentException("plugin or module attempted to define the same source [" + source +
                    "] as a built-in system index");
            }
        });
        return Map.copyOf(map);
    }

    Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return this.featureDescriptors.values().stream()
            .flatMap(f -> f.getIndexDescriptors().stream())
            .collect(Collectors.toList());
    }

    /**
     * Check that a feature name is not reserved
     * @param name Name of feature
     * @param plugin Name of plugin providing the feature
     */
    public static void validateFeatureName(String name, String plugin) {
        if (SnapshotsService.NO_FEATURE_STATES_VALUE.equalsIgnoreCase(name)) {
            throw new IllegalArgumentException("feature name cannot be reserved name [\"" + SnapshotsService.NO_FEATURE_STATES_VALUE +
                "\"], but was for plugin [" + plugin + "]");
        }
    }

    /**
     * Class holding a description of a stateful feature.
     */
    public static class Feature {
        private final String description;
        private final Collection<SystemIndexDescriptor> indexDescriptors;
        private final Collection<String> associatedIndexPatterns;
        private final TriConsumer<ClusterService, Client, ActionListener<ResetFeatureStateStatus>> cleanUpFunction;

        /**
         * Construct a Feature with a custom cleanup function
         * @param description Description of the feature
         * @param indexDescriptors Patterns describing system indices for this feature
         * @param associatedIndexPatterns Patterns describing associated indices
         * @param cleanUpFunction A function that will clean up the feature's state
         */
        public Feature(
            String description,
            Collection<SystemIndexDescriptor> indexDescriptors,
            Collection<String> associatedIndexPatterns,
            TriConsumer<ClusterService, Client, ActionListener<ResetFeatureStateStatus>> cleanUpFunction) {
            this.description = description;
            this.indexDescriptors = indexDescriptors;
            this.associatedIndexPatterns = associatedIndexPatterns;
            this.cleanUpFunction = cleanUpFunction;
        }

        /**
         * Construct a Feature using the default clean-up function
         * @param name Name of the feature, used in logging
         * @param description Description of the feature
         * @param indexDescriptors Patterns describing system indices for this feature
         */
        public Feature(String name, String description, Collection<SystemIndexDescriptor> indexDescriptors) {
            this(description, indexDescriptors, Collections.emptyList(),
                (clusterService, client, listener) ->
                    cleanUpFeature(indexDescriptors, Collections.emptyList(), name, clusterService, client, listener)
            );
        }

        public String getDescription() {
            return description;
        }

        public Collection<SystemIndexDescriptor> getIndexDescriptors() {
            return indexDescriptors;
        }

        public Collection<String> getAssociatedIndexPatterns() {
            return associatedIndexPatterns;
        }

        public TriConsumer<ClusterService, Client, ActionListener<ResetFeatureStateStatus>> getCleanUpFunction() {
            return cleanUpFunction;
        }

        /**
         * Clean up the state of a feature
         * @param indexDescriptors List of descriptors of a feature's system indices
         * @param associatedIndexPatterns List of patterns of a feature's associated indices
         * @param name Name of the feature, used in logging
         * @param clusterService A clusterService, for retrieving cluster metadata
         * @param client A client, for issuing delete requests
         * @param listener A listener to return success or failure of cleanup
         */
        public static void cleanUpFeature(
            Collection<SystemIndexDescriptor> indexDescriptors,
            Collection<String> associatedIndexPatterns,
            String name,
            ClusterService clusterService,
            Client client,
            ActionListener<ResetFeatureStateStatus> listener) {
            Stream<String> systemIndices = indexDescriptors.stream()
                .map(sid -> sid.getMatchingIndices(clusterService.state().getMetadata()))
                .flatMap(List::stream);

            List<String> allIndices = Stream.concat(systemIndices, associatedIndexPatterns.stream())
                .collect(Collectors.toList());

            if (allIndices.isEmpty()) {
                // if no actual indices match the pattern, we can stop here
                listener.onResponse(new ResetFeatureStateStatus(name, "SUCCESS"));
                return;
            }

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
            deleteIndexRequest.indices(allIndices.toArray(Strings.EMPTY_ARRAY));
            client.execute(DeleteIndexAction.INSTANCE, deleteIndexRequest, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    listener.onResponse(new ResetFeatureStateStatus(name, "SUCCESS"));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onResponse(new ResetFeatureStateStatus(name, "FAILURE: " + e.getMessage()));
                }
            });
        }
    }
}
