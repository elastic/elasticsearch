/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.tasks.TaskResultsService.TASKS_DESCRIPTOR;
import static org.elasticsearch.tasks.TaskResultsService.TASKS_FEATURE_NAME;

/**
 * This class holds the {@link SystemIndexDescriptor} objects that represent system indices the
 * node knows about. Methods for determining if an index should be a system index are also provided
 * to reduce the locations within the code that need to deal with {@link SystemIndexDescriptor}s.
 */
public class SystemIndices {
    public static final String SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY = "_system_index_access_allowed";
    public static final String EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY = "_external_system_index_access_origin";
    public static final String UPGRADED_INDEX_SUFFIX = "-reindexed-for-8";

    private static final Automaton EMPTY = Automata.makeEmpty();

    /**
     * This is the source for non-plugin system features.
     */
    private static final Map<String, Feature> SERVER_SYSTEM_FEATURE_DESCRIPTORS = singletonMap(
        TASKS_FEATURE_NAME,
        new Feature(TASKS_FEATURE_NAME, "Manages task results", singletonList(TASKS_DESCRIPTOR))
    );

    /**
     * The node's full list of system features is stored here. The map is keyed
     * on the value of {@link Feature#getName()}, and is used for fast lookup of
     * feature objects via {@link #getFeature(String)}.
     */
    private final Map<String, Feature> featureDescriptors;

    private final CharacterRunAutomaton systemIndexAutomaton;
    private final CharacterRunAutomaton systemDataStreamIndicesAutomaton;
    private final CharacterRunAutomaton netNewSystemIndexAutomaton;
    private final Predicate<String> systemDataStreamAutomaton;
    private final Map<String, CharacterRunAutomaton> productToSystemIndicesMatcher;
    private final ExecutorSelector executorSelector;

    /**
     * Initialize the SystemIndices object
     * @param pluginAndModuleFeatures A list of features from which we will load system indices.
     *                                These features come from plugins and modules. Non-plugin system
     *                                features such as Tasks will be added automatically.
     */
    public SystemIndices(List<Feature> pluginAndModuleFeatures) {
        featureDescriptors = buildFeatureMap(pluginAndModuleFeatures);
        checkForOverlappingPatterns(featureDescriptors);
        ensurePatternsAllowSuffix(featureDescriptors);
        checkForDuplicateAliases(this.getSystemIndexDescriptors());
        this.systemIndexAutomaton = buildIndexCharacterRunAutomaton(featureDescriptors);
        this.netNewSystemIndexAutomaton = buildNetNewIndexCharacterRunAutomaton(featureDescriptors);
        this.systemDataStreamIndicesAutomaton = buildDataStreamBackingIndicesAutomaton(featureDescriptors);
        this.systemDataStreamAutomaton = buildDataStreamNamePredicate(featureDescriptors);
        this.productToSystemIndicesMatcher = getProductToSystemIndicesMap(featureDescriptors);
        this.executorSelector = new ExecutorSelector(this);
    }

    static void ensurePatternsAllowSuffix(Map<String, Feature> featureDescriptors) {
        String suffixPattern = "*" + UPGRADED_INDEX_SUFFIX;
        final List<String> descriptorsWithNoRoomForSuffix = featureDescriptors.values()
            .stream()
            .flatMap(
                feature -> feature.getIndexDescriptors()
                    .stream()
                    // The below filter & map are inside the enclosing flapMap so we have access to both the feature and the descriptor
                    .filter(descriptor -> overlaps(descriptor.getIndexPattern(), suffixPattern) == false)
                    .map(
                        descriptor -> new ParameterizedMessage(
                            "pattern [{}] from feature [{}]",
                            descriptor.getIndexPattern(),
                            feature.getName()
                        ).getFormattedMessage()
                    )
            )
            .collect(Collectors.toList());
        if (descriptorsWithNoRoomForSuffix.isEmpty() == false) {
            throw new IllegalStateException(
                new ParameterizedMessage(
                    "the following system index patterns do not allow suffix [{}] required to allow upgrades: [{}]",
                    UPGRADED_INDEX_SUFFIX,
                    descriptorsWithNoRoomForSuffix
                ).getFormattedMessage()
            );
        }
    }

    private static void checkForDuplicateAliases(Collection<SystemIndexDescriptor> descriptors) {
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

    private static Map<String, CharacterRunAutomaton> getProductToSystemIndicesMap(Map<String, Feature> featureDescriptors) {
        Map<String, Automaton> productToSystemIndicesMap = new HashMap<>();
        for (Feature feature : featureDescriptors.values()) {
            feature.getIndexDescriptors().forEach(systemIndexDescriptor -> {
                if (systemIndexDescriptor.isExternal()) {
                    systemIndexDescriptor.getAllowedElasticProductOrigins()
                        .forEach(origin -> productToSystemIndicesMap.compute(origin, (key, value) -> {
                            Automaton automaton = SystemIndexDescriptor.buildAutomaton(
                                systemIndexDescriptor.getIndexPattern(),
                                systemIndexDescriptor.getAliasName()
                            );
                            return value == null ? automaton : Operations.union(value, automaton);
                        }));
                }
            });
            feature.getDataStreamDescriptors().forEach(dataStreamDescriptor -> {
                if (dataStreamDescriptor.isExternal()) {
                    dataStreamDescriptor.getAllowedElasticProductOrigins()
                        .forEach(origin -> productToSystemIndicesMap.compute(origin, (key, value) -> {
                            Automaton automaton = SystemIndexDescriptor.buildAutomaton(
                                dataStreamDescriptor.getBackingIndexPattern(),
                                dataStreamDescriptor.getDataStreamName()
                            );
                            return value == null ? automaton : Operations.union(value, automaton);
                        }));
                }
            });
        }

        return unmodifiableMap(
            productToSystemIndicesMap.entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        Entry::getKey,
                        entry -> new CharacterRunAutomaton(MinimizationOperations.minimize(entry.getValue(), Integer.MAX_VALUE))
                    )
                )
        );
    }

    /**
     * Checks whether the given name matches a reserved name or pattern that is intended for use by a system component. The name
     * is checked against index names, aliases, data stream names, and the names of indices that back a system data stream.
     */
    public boolean isSystemName(String name) {
        return isSystemIndex(name) || isSystemDataStream(name) || isSystemIndexBackingDataStream(name);
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
     * Determines whether a given index is a system index by comparing its name to the collection of loaded {@link SystemIndexDescriptor}s.
     * This will also match alias names that belong to system indices.
     * @param indexName the index name to check against loaded {@link SystemIndexDescriptor}s
     * @return true if the index name matches a pattern from a {@link SystemIndexDescriptor}
     */
    public boolean isSystemIndex(String indexName) {
        return systemIndexAutomaton.run(indexName);
    }

    /**
     * Determines whether the provided name matches that of a system data stream that has been defined by a
     * {@link SystemDataStreamDescriptor}
     */
    public boolean isSystemDataStream(String name) {
        return systemDataStreamAutomaton.test(name);
    }

    /**
     * Determines whether the provided name matches that of an index that backs a system data stream.
     */
    public boolean isSystemIndexBackingDataStream(String name) {
        return systemDataStreamIndicesAutomaton.run(name);
    }

    /**
     * Checks whether an index is a net-new system index, meaning we can apply non-BWC behavior to it.
     * @param indexName The index name to check.
     * @return {@code true} if the given index is covered by a net-new system index descriptor, {@code false} otherwise.
     */
    public boolean isNetNewSystemIndex(String indexName) {
        return netNewSystemIndexAutomaton.run(indexName);
    }

    /**
     * Used to determine which executor should be used for operations on this index. See {@link ExecutorSelector} docs for
     * details.
     */
    public ExecutorSelector getExecutorSelector() {
        return executorSelector;
    }

    /**
     * Finds a single matching {@link SystemIndexDescriptor}, if any, for the given index name.
     * @param name the name of the index
     * @return The matching {@link SystemIndexDescriptor} or {@code null} if no descriptor is found
     * @throws IllegalStateException if multiple descriptors match the name
     */
    public @Nullable SystemIndexDescriptor findMatchingDescriptor(String name) {
        final List<SystemIndexDescriptor> matchingDescriptors = featureDescriptors.values()
            .stream()
            .flatMap(feature -> feature.getIndexDescriptors().stream())
            .filter(descriptor -> descriptor.matchesIndexPattern(name))
            .collect(Collectors.toList());

        if (matchingDescriptors.isEmpty()) {
            return null;
        } else if (matchingDescriptors.size() == 1) {
            return matchingDescriptors.get(0);
        } else {
            // This should be prevented by failing on overlapping patterns at startup time, but is here just in case.
            StringBuilder errorMessage = new StringBuilder().append("index name [")
                .append(name)
                .append("] is claimed as a system index by multiple system index patterns: [")
                .append(
                    matchingDescriptors.stream()
                        .map(
                            descriptor -> "pattern: ["
                                + descriptor.getIndexPattern()
                                + "], description: ["
                                + descriptor.getDescription()
                                + "]"
                        )
                        .collect(Collectors.joining("; "))
                );
            // Throw AssertionError if assertions are enabled, or a regular exception otherwise:
            assert false : errorMessage.toString();
            throw new IllegalStateException(errorMessage.toString());
        }
    }

    /**
     * Finds a single matching {@link SystemDataStreamDescriptor}, if any, for the given DataStream name.
     * @param name the name of the DataStream
     * @return The matching {@link SystemDataStreamDescriptor} or {@code null} if no descriptor is found
     * @throws IllegalStateException if multiple descriptors match the name
     */
    public @Nullable SystemDataStreamDescriptor findMatchingDataStreamDescriptor(String name) {
        final List<SystemDataStreamDescriptor> matchingDescriptors = featureDescriptors.values()
            .stream()
            .flatMap(feature -> feature.getDataStreamDescriptors().stream())
            .filter(descriptor -> descriptor.getDataStreamName().equals(name))
            .collect(Collectors.toList());

        if (matchingDescriptors.isEmpty()) {
            return null;
        } else if (matchingDescriptors.size() == 1) {
            return matchingDescriptors.get(0);
        } else {
            // This should be prevented by failing on overlapping patterns at startup time, but is here just in case.
            StringBuilder errorMessage = new StringBuilder().append("DataStream name [")
                .append(name)
                .append("] is claimed as a system data stream by multiple descriptors: [")
                .append(
                    matchingDescriptors.stream()
                        .map(
                            descriptor -> "name: ["
                                + descriptor.getDataStreamName()
                                + "], description: ["
                                + descriptor.getDescription()
                                + "]"
                        )
                        .collect(Collectors.joining("; "))
                );
            // Throw AssertionError if assertions are enabled, or a regular exception otherwise:
            assert false : errorMessage.toString();
            throw new IllegalStateException(errorMessage.toString());
        }
    }

    /**
     * Builds a predicate that tests if a system index should be accessible based on the provided product name
     * contained in headers.
     * @param threadContext the threadContext containing headers used for system index access
     * @return Predicate to check external system index metadata with
     */
    public Predicate<IndexMetadata> getProductSystemIndexMetadataPredicate(ThreadContext threadContext) {
        final String product = threadContext.getHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY);
        if (product == null) {
            return indexMetadata -> false;
        }
        final CharacterRunAutomaton automaton = productToSystemIndicesMatcher.get(product);
        if (automaton == null) {
            return indexMetadata -> false;
        }
        return indexMetadata -> automaton.run(indexMetadata.getIndex().getName());
    }

    /**
     * Builds a predicate that tests if a system index name should be accessible based on the provided product name
     * contained in headers.
     * @param threadContext the threadContext containing headers used for system index access
     * @return Predicate to check external system index names with
     */
    public Predicate<String> getProductSystemIndexNamePredicate(ThreadContext threadContext) {
        final String product = threadContext.getHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY);
        if (product == null) {
            return name -> false;
        }
        final CharacterRunAutomaton automaton = productToSystemIndicesMatcher.get(product);
        if (automaton == null) {
            return name -> false;
        }
        return automaton::run;
    }

    /**
     * Get a set of feature names. This is useful for checking whether particular
     * features are present on the node.
     * @return A set of all feature names
     */
    public Set<String> getFeatureNames() {
        return Collections.unmodifiableSet(featureDescriptors.keySet());
    }

    /**
     * Get a feature by name.
     * @param name Name of a feature.
     * @return The corresponding feature if it exists on this node, null otherwise.
     */
    public Feature getFeature(String name) {
        return featureDescriptors.get(name);
    }

    /**
     * Get a collection of the Features this SystemIndices object is managing.
     * @return A collection of Features.
     */
    public Collection<Feature> getFeatures() {
        return Collections.unmodifiableCollection(featureDescriptors.values());
    }

    private static CharacterRunAutomaton buildIndexCharacterRunAutomaton(Map<String, Feature> descriptors) {
        Optional<Automaton> automaton = descriptors.values().stream().map(SystemIndices::featureToIndexAutomaton).reduce(Operations::union);
        return new CharacterRunAutomaton(MinimizationOperations.minimize(automaton.orElse(EMPTY), Integer.MAX_VALUE));
    }

    private static CharacterRunAutomaton buildNetNewIndexCharacterRunAutomaton(Map<String, Feature> featureDescriptors) {
        Optional<Automaton> automaton = featureDescriptors.values()
            .stream()
            .flatMap(feature -> feature.getIndexDescriptors().stream())
            .filter(SystemIndexDescriptor::isNetNew)
            .map(descriptor -> SystemIndexDescriptor.buildAutomaton(descriptor.getIndexPattern(), descriptor.getAliasName()))
            .reduce(Operations::union);
        return new CharacterRunAutomaton(MinimizationOperations.minimize(automaton.orElse(EMPTY), Integer.MAX_VALUE));
    }

    private static Automaton featureToIndexAutomaton(Feature feature) {
        Optional<Automaton> systemIndexAutomaton = feature.getIndexDescriptors()
            .stream()
            .map(descriptor -> SystemIndexDescriptor.buildAutomaton(descriptor.getIndexPattern(), descriptor.getAliasName()))
            .reduce(Operations::union);

        return systemIndexAutomaton.orElse(EMPTY);
    }

    private static Predicate<String> buildDataStreamNamePredicate(Map<String, Feature> descriptors) {
        Set<String> systemDataStreamNames = descriptors.values()
            .stream()
            .flatMap(feature -> feature.getDataStreamDescriptors().stream())
            .map(SystemDataStreamDescriptor::getDataStreamName)
            .collect(Collectors.toSet());
        return systemDataStreamNames::contains;
    }

    private static CharacterRunAutomaton buildDataStreamBackingIndicesAutomaton(Map<String, Feature> descriptors) {
        Optional<Automaton> automaton = descriptors.values()
            .stream()
            .map(SystemIndices::featureToDataStreamBackingIndicesAutomaton)
            .reduce(Operations::union);
        return new CharacterRunAutomaton(automaton.orElse(EMPTY));
    }

    private static Automaton featureToDataStreamBackingIndicesAutomaton(Feature feature) {
        Optional<Automaton> systemDataStreamAutomaton = feature.getDataStreamDescriptors()
            .stream()
            .map(descriptor -> SystemIndexDescriptor.buildAutomaton(descriptor.getBackingIndexPattern(), null))
            .reduce(Operations::union);
        return systemDataStreamAutomaton.orElse(EMPTY);
    }

    public SystemDataStreamDescriptor validateDataStreamAccess(String dataStreamName, ThreadContext threadContext) {
        if (systemDataStreamAutomaton.test(dataStreamName)) {
            SystemDataStreamDescriptor dataStreamDescriptor = featureDescriptors.values()
                .stream()
                .flatMap(feature -> feature.getDataStreamDescriptors().stream())
                .filter(descriptor -> descriptor.getDataStreamName().equals(dataStreamName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("system data stream descriptor not found for [" + dataStreamName + "]"));
            if (dataStreamDescriptor.isExternal()) {
                final SystemIndexAccessLevel accessLevel = getSystemIndexAccessLevel(threadContext);
                assert accessLevel != SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY : "BACKWARDS_COMPATIBLE access level is leaking";
                if (accessLevel == SystemIndexAccessLevel.NONE) {
                    throw dataStreamAccessException(null, dataStreamName);
                } else if (accessLevel == SystemIndexAccessLevel.RESTRICTED) {
                    if (getProductSystemIndexNamePredicate(threadContext).test(dataStreamName) == false) {
                        throw dataStreamAccessException(
                            threadContext.getHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY),
                            dataStreamName
                        );
                    } else {
                        return dataStreamDescriptor;
                    }
                } else {
                    assert accessLevel == SystemIndexAccessLevel.ALL || accessLevel == SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY;
                    return dataStreamDescriptor;
                }
            } else {
                return dataStreamDescriptor;
            }
        } else {
            return null;
        }
    }

    public IllegalArgumentException dataStreamAccessException(ThreadContext threadContext, Collection<String> names) {
        return dataStreamAccessException(
            threadContext.getHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY),
            names.toArray(Strings.EMPTY_ARRAY)
        );
    }

    public IllegalArgumentException netNewSystemIndexAccessException(ThreadContext threadContext, Collection<String> names) {
        final String product = threadContext.getHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY);
        if (product == null) {
            return new IllegalArgumentException(
                "Indices " + Arrays.toString(names.toArray(Strings.EMPTY_ARRAY)) + " use and access is reserved for system operations"
            );
        } else {
            return new IllegalArgumentException(
                "Indices " + Arrays.toString(names.toArray(Strings.EMPTY_ARRAY)) + " use and access is reserved for system operations"
            );
        }
    }

    IllegalArgumentException dataStreamAccessException(@Nullable String product, String... dataStreamNames) {
        if (product == null) {
            return new IllegalArgumentException(
                "Data stream(s) " + Arrays.toString(dataStreamNames) + " use and access is reserved for system operations"
            );
        } else {
            return new IllegalArgumentException(
                "Data stream(s) " + Arrays.toString(dataStreamNames) + " may not be accessed by product [" + product + "]"
            );
        }
    }

    /**
     * Determines what level of system index access should be allowed in the current context.
     *
     * @return {@link SystemIndexAccessLevel#ALL} if unrestricted system index access should be allowed,
     * {@link SystemIndexAccessLevel#RESTRICTED} if a subset of system index access should be allowed, or
     * {@link SystemIndexAccessLevel#NONE} if no system index access should be allowed.
     */
    public SystemIndexAccessLevel getSystemIndexAccessLevel(ThreadContext threadContext) {
        // This method intentionally cannot return BACKWARDS_COMPATIBLE_ONLY - that access level should only be used manually
        // in known special cases.
        final String headerValue = threadContext.getHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY);
        final String productHeaderValue = threadContext.getHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY);

        final boolean allowed = Booleans.parseBoolean(headerValue, true);
        if (allowed) {
            if (productHeaderValue != null) {
                return SystemIndexAccessLevel.RESTRICTED;
            } else {
                return SystemIndexAccessLevel.ALL;
            }
        } else {
            return SystemIndexAccessLevel.NONE;
        }
    }

    public enum SystemIndexAccessLevel {
        ALL,
        NONE,
        RESTRICTED,
        /**
         * This value exists because there was a desire for "net-new" system indices to opt in to the post-8.0 behavior of having
         * access blocked in most cases, but this caused problems with certain APIs
         * (see https://github.com/elastic/elasticsearch/issues/74687), so this access level was added as a workaround. Once we no longer
         * have to support accessing existing system indices, this can and should be removed, along with the net-new property of
         * system indices in general.
         */
        BACKWARDS_COMPATIBLE_ONLY
    }

    /**
     * Given a collection of {@link SystemIndexDescriptor}s and their sources, checks to see if the index patterns of the listed
     * descriptors overlap with any of the other patterns. If any do, throws an exception.
     *
     * @param featureDescriptors A map of feature names to the Features that will provide SystemIndexDescriptors
     * @throws IllegalStateException Thrown if any of the index patterns overlaps with another.
     */
    static void checkForOverlappingPatterns(Map<String, Feature> featureDescriptors) {
        List<Tuple<String, SystemIndexDescriptor>> sourceDescriptorPair = featureDescriptors.values()
            .stream()
            .flatMap(feature -> feature.getIndexDescriptors().stream().map(descriptor -> new Tuple<>(feature.getName(), descriptor)))
            .sorted(Comparator.comparing(d -> d.v1() + ":" + d.v2().getIndexPattern())) // Consistent ordering -> consistent error message
            .collect(Collectors.toList());
        List<Tuple<String, SystemDataStreamDescriptor>> sourceDataStreamDescriptorPair = featureDescriptors.values()
            .stream()
            .filter(feature -> feature.getDataStreamDescriptors().isEmpty() == false)
            .flatMap(feature -> feature.getDataStreamDescriptors().stream().map(descriptor -> new Tuple<>(feature.getName(), descriptor)))
            .sorted(Comparator.comparing(d -> d.v1() + ":" + d.v2().getDataStreamName())) // Consistent ordering -> consistent error message
            .collect(Collectors.toList());

        // This is O(n^2) with the number of system index descriptors, and each check is quadratic with the number of states in the
        // automaton, but the absolute number of system index descriptors should be quite small (~10s at most), and the number of states
        // per pattern should be low as well. If these assumptions change, this might need to be reworked.
        sourceDescriptorPair.forEach(descriptorToCheck -> {
            List<Tuple<String, SystemIndexDescriptor>> descriptorsMatchingThisPattern = sourceDescriptorPair.stream()
                .filter(d -> descriptorToCheck.v2() != d.v2()) // Exclude the pattern currently being checked
                .filter(
                    d -> overlaps(descriptorToCheck.v2(), d.v2())
                        || (d.v2().getAliasName() != null && descriptorToCheck.v2().matchesIndexPattern(d.v2().getAliasName()))
                )
                .collect(Collectors.toList());
            if (descriptorsMatchingThisPattern.isEmpty() == false) {
                throw new IllegalStateException(
                    "a system index descriptor ["
                        + descriptorToCheck.v2()
                        + "] from ["
                        + descriptorToCheck.v1()
                        + "] overlaps with other system index descriptors: ["
                        + descriptorsMatchingThisPattern.stream()
                            .map(descriptor -> descriptor.v2() + " from [" + descriptor.v1() + "]")
                            .collect(Collectors.joining(", "))
                );
            }

            List<Tuple<String, SystemDataStreamDescriptor>> dataStreamsMatching = sourceDataStreamDescriptorPair.stream()
                .filter(
                    dsTuple -> descriptorToCheck.v2().matchesIndexPattern(dsTuple.v2().getDataStreamName())
                        || overlaps(descriptorToCheck.v2().getIndexPattern(), dsTuple.v2().getBackingIndexPattern())
                )
                .collect(Collectors.toList());
            if (dataStreamsMatching.isEmpty() == false) {
                throw new IllegalStateException(
                    "a system index descriptor ["
                        + descriptorToCheck.v2()
                        + "] from ["
                        + descriptorToCheck.v1()
                        + "] overlaps with one or more data stream descriptors: ["
                        + dataStreamsMatching.stream()
                            .map(descriptor -> descriptor.v2() + " from [" + descriptor.v1() + "]")
                            .collect(Collectors.joining(", "))
                );
            }
        });
    }

    private static boolean overlaps(SystemIndexDescriptor a1, SystemIndexDescriptor a2) {
        return overlaps(a1.getIndexPattern(), a2.getIndexPattern());
    }

    private static boolean overlaps(String pattern1, String pattern2) {
        Automaton a1Automaton = SystemIndexDescriptor.buildAutomaton(pattern1, null);
        Automaton a2Automaton = SystemIndexDescriptor.buildAutomaton(pattern2, null);
        return Operations.isEmpty(Operations.intersection(a1Automaton, a2Automaton)) == false;
    }

    private static Map<String, Feature> buildFeatureMap(List<Feature> features) {
        final Map<String, Feature> map = new HashMap<>(features.size() + SERVER_SYSTEM_FEATURE_DESCRIPTORS.size());
        features.forEach(feature -> map.put(feature.getName(), feature));
        // put the server items last since we expect less of them
        SERVER_SYSTEM_FEATURE_DESCRIPTORS.forEach((source, feature) -> {
            if (map.putIfAbsent(source, feature) != null) {
                throw new IllegalArgumentException(
                    "plugin or module attempted to define the same source [" + source + "] as a built-in system index"
                );
            }
        });
        return unmodifiableMap(map);
    }

    Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return this.featureDescriptors.values().stream().flatMap(f -> f.getIndexDescriptors().stream()).collect(Collectors.toList());
    }

    /**
     * Check that a feature name is not reserved
     * @param name Name of feature
     * @param plugin Name of plugin providing the feature
     */
    public static void validateFeatureName(String name, String plugin) {
        if (SnapshotsService.NO_FEATURE_STATES_VALUE.equalsIgnoreCase(name)) {
            throw new IllegalArgumentException(
                "feature name cannot be reserved name [\""
                    + SnapshotsService.NO_FEATURE_STATES_VALUE
                    + "\"], but was for plugin ["
                    + plugin
                    + "]"
            );
        }
    }

    /**
     * Class holding a description of a stateful feature.
     */
    public static class Feature {
        private final String name;
        private final String description;
        private final Collection<SystemIndexDescriptor> indexDescriptors;
        private final Collection<SystemDataStreamDescriptor> dataStreamDescriptors;
        private final Collection<AssociatedIndexDescriptor> associatedIndexDescriptors;
        private final TriConsumer<ClusterService, Client, ActionListener<ResetFeatureStateStatus>> cleanUpFunction;
        private final MigrationPreparationHandler preMigrationFunction;
        private final MigrationCompletionHandler postMigrationFunction;

        /**
         * Construct a Feature with a custom cleanup function
         * @param name The name of the feature
         * @param description Description of the feature
         * @param indexDescriptors Collection of objects describing system indices for this feature
         * @param dataStreamDescriptors Collection of objects describing system data streams for this feature
         * @param associatedIndexDescriptors Collection of objects describing associated indices for this feature
         * @param cleanUpFunction A function that will clean up the feature's state
         * @param preMigrationFunction A function that will be called prior to upgrading any of this plugin's system indices
         * @param postMigrationFunction A function that will be called after upgrading all of this plugin's system indices
         */
        public Feature(
            String name,
            String description,
            Collection<SystemIndexDescriptor> indexDescriptors,
            Collection<SystemDataStreamDescriptor> dataStreamDescriptors,
            Collection<AssociatedIndexDescriptor> associatedIndexDescriptors,
            TriConsumer<ClusterService, Client, ActionListener<ResetFeatureStateStatus>> cleanUpFunction,
            MigrationPreparationHandler preMigrationFunction,
            MigrationCompletionHandler postMigrationFunction
        ) {
            this.name = name;
            this.description = description;
            this.indexDescriptors = indexDescriptors;
            this.dataStreamDescriptors = dataStreamDescriptors;
            this.associatedIndexDescriptors = associatedIndexDescriptors;
            this.cleanUpFunction = cleanUpFunction;
            this.preMigrationFunction = preMigrationFunction;
            this.postMigrationFunction = postMigrationFunction;
        }

        /**
         * Construct a Feature using the default clean-up function
         * @param name Name of the feature, used in logging
         * @param description Description of the feature
         * @param indexDescriptors Patterns describing system indices for this feature
         */
        public Feature(String name, String description, Collection<SystemIndexDescriptor> indexDescriptors) {
            this(
                name,
                description,
                indexDescriptors,
                Collections.emptyList(),
                Collections.emptyList(),
                (clusterService, client, listener) -> cleanUpFeature(
                    indexDescriptors,
                    Collections.emptyList(),
                    name,
                    clusterService,
                    client,
                    listener
                ),
                Feature::noopPreMigrationFunction,
                Feature::noopPostMigrationFunction
            );
        }

        /**
         * Construct a Feature using the default clean-up function
         * @param name Name of the feature, used in logging
         * @param description Description of the feature
         * @param indexDescriptors Patterns describing system indices for this feature
         * @param dataStreamDescriptors Collection of objects describing system data streams for this feature
         */
        public Feature(
            String name,
            String description,
            Collection<SystemIndexDescriptor> indexDescriptors,
            Collection<SystemDataStreamDescriptor> dataStreamDescriptors
        ) {
            this(
                name,
                description,
                indexDescriptors,
                dataStreamDescriptors,
                Collections.emptyList(),
                (clusterService, client, listener) -> cleanUpFeature(
                    indexDescriptors,
                    Collections.emptyList(),
                    name,
                    clusterService,
                    client,
                    listener
                ),
                Feature::noopPreMigrationFunction,
                Feature::noopPostMigrationFunction
            );
        }

        /**
         * Creates a {@link Feature} from a {@link SystemIndexPlugin}.
         * @param plugin The {@link SystemIndexPlugin} that adds this feature.
         * @param settings Node-level settings, as this may impact the descriptors returned by the plugin.
         * @return A {@link Feature} which represents the feature added by the given plugin.
         */
        public static Feature fromSystemIndexPlugin(SystemIndexPlugin plugin, Settings settings) {
            return new Feature(
                plugin.getFeatureName(),
                plugin.getFeatureDescription(),
                plugin.getSystemIndexDescriptors(settings),
                plugin.getSystemDataStreamDescriptors(),
                plugin.getAssociatedIndexDescriptors(),
                plugin::cleanUpFeature,
                plugin::prepareForIndicesMigration,
                plugin::indicesMigrationComplete
            );
        }

        public String getDescription() {
            return description;
        }

        public Collection<SystemIndexDescriptor> getIndexDescriptors() {
            return indexDescriptors;
        }

        public Collection<SystemDataStreamDescriptor> getDataStreamDescriptors() {
            return dataStreamDescriptors;
        }

        public Collection<AssociatedIndexDescriptor> getAssociatedIndexDescriptors() {
            return associatedIndexDescriptors;
        }

        public TriConsumer<ClusterService, Client, ActionListener<ResetFeatureStateStatus>> getCleanUpFunction() {
            return cleanUpFunction;
        }

        public String getName() {
            return name;
        }

        public MigrationPreparationHandler getPreMigrationFunction() {
            return preMigrationFunction;
        }

        public MigrationCompletionHandler getPostMigrationFunction() {
            return postMigrationFunction;
        }

        /**
         * Clean up the state of a feature
         * @param indexDescriptors List of descriptors of a feature's system indices
         * @param associatedIndexDescriptors List of descriptors of a feature's associated indices
         * @param name Name of the feature, used in logging
         * @param clusterService A clusterService, for retrieving cluster metadata
         * @param client A client, for issuing delete requests
         * @param listener A listener to return success or failure of cleanup
         */
        public static void cleanUpFeature(
            Collection<? extends IndexPatternMatcher> indexDescriptors,
            Collection<? extends IndexPatternMatcher> associatedIndexDescriptors,
            String name,
            ClusterService clusterService,
            Client client,
            ActionListener<ResetFeatureStateStatus> listener
        ) {
            Metadata metadata = clusterService.state().getMetadata();

            List<String> allIndices = Stream.concat(indexDescriptors.stream(), associatedIndexDescriptors.stream())
                .map(descriptor -> descriptor.getMatchingIndices(metadata))
                .flatMap(List::stream)
                .collect(Collectors.toList());

            if (allIndices.isEmpty()) {
                // if no actual indices match the pattern, we can stop here
                listener.onResponse(ResetFeatureStateStatus.success(name));
                return;
            }

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
            deleteIndexRequest.indices(allIndices.toArray(Strings.EMPTY_ARRAY));
            client.execute(DeleteIndexAction.INSTANCE, deleteIndexRequest, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    listener.onResponse(ResetFeatureStateStatus.success(name));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onResponse(ResetFeatureStateStatus.failure(name, e));
                }
            });
        }

        // No-op pre-migration function to be used as the default in case none are provided.
        private static void noopPreMigrationFunction(
            ClusterService clusterService,
            Client client,
            ActionListener<Map<String, Object>> listener
        ) {
            listener.onResponse(Collections.emptyMap());
        }

        // No-op pre-migration function to be used as the default in case none are provided.
        private static void noopPostMigrationFunction(
            Map<String, Object> preUpgradeMetadata,
            ClusterService clusterService,
            Client client,
            ActionListener<Boolean> listener
        ) {
            listener.onResponse(true);
        }

        /**
         * Type for the handler that's invoked prior to migrating a Feature's system indices.
         * See {@link SystemIndexPlugin#prepareForIndicesMigration(ClusterService, Client, ActionListener)}.
         */
        @FunctionalInterface
        public interface MigrationPreparationHandler {
            void prepareForIndicesMigration(ClusterService clusterService, Client client, ActionListener<Map<String, Object>> listener);
        }

        /**
         * Type for the handler that's invoked when all of a feature's system indices have been migrated.
         * See {@link SystemIndexPlugin#indicesMigrationComplete(Map, ClusterService, Client, ActionListener)}.
         */
        @FunctionalInterface
        public interface MigrationCompletionHandler {
            void indicesMigrationComplete(
                Map<String, Object> preUpgradeMetadata,
                ClusterService clusterService,
                Client client,
                ActionListener<Boolean> listener
            );
        }

        public static Feature pluginToFeature(SystemIndexPlugin plugin, Settings settings) {
            return new Feature(
                plugin.getFeatureName(),
                plugin.getFeatureDescription(),
                plugin.getSystemIndexDescriptors(settings),
                plugin.getSystemDataStreamDescriptors(),
                plugin.getAssociatedIndexDescriptors(),
                plugin::cleanUpFeature,
                plugin::prepareForIndicesMigration,
                plugin::indicesMigrationComplete
            );
        }
    }
}
