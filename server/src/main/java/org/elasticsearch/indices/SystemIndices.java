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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.TaskResultsService;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.tasks.TaskResultsService.TASK_INDEX;

/**
 * This class holds the {@link SystemIndexDescriptor} objects that represent system indices the
 * node knows about. Methods for determining if an index should be a system index are also provided
 * to reduce the locations within the code that need to deal with {@link SystemIndexDescriptor}s.
 */
public class SystemIndices implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(SystemIndices.class);

    private static final Map<String, Collection<SystemIndexDescriptor>> SERVER_SYSTEM_INDEX_DESCRIPTORS = Map.of(
        TaskResultsService.class.getName(),
        List.of(SystemIndexDescriptor.builder().setIndexPattern(TASK_INDEX + "*")
            .setDescription("Task Result Index")
            .build())
    );

    private final CharacterRunAutomaton runAutomaton;
    private final Collection<SystemIndexDescriptor> systemIndexDescriptors;
    private final Client client;

    public SystemIndices(Map<String, Collection<SystemIndexDescriptor>> pluginAndModulesDescriptors, Client client) {
        this.client = client;
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

    private static CharacterRunAutomaton buildCharacterRunAutomaton(Collection<SystemIndexDescriptor> descriptors) {
        Optional<Automaton> automaton = descriptors.stream()
            .map(descriptor -> {
                if (descriptor.getAliasName() != null) {
                    return Regex.simpleMatchToAutomaton(descriptor.getIndexPattern(), descriptor.getAliasName());
                }
                return Regex.simpleMatchToAutomaton(descriptor.getIndexPattern());
            })
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
        List<Tuple<String, SystemIndexDescriptor>> sourceDescriptorPair = sourceToDescriptors.entrySet()
            .stream()
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
        });
    }

    private static boolean overlaps(SystemIndexDescriptor a1, SystemIndexDescriptor a2) {
        Automaton a1Automaton = Regex.simpleMatchToAutomaton(a1.getIndexPattern());
        Automaton a2Automaton = Regex.simpleMatchToAutomaton(a2.getIndexPattern());
        return Operations.isEmpty(Operations.intersection(a1Automaton, a2Automaton)) == false;
    }

    private static Map<String, Collection<SystemIndexDescriptor>> buildSystemIndexDescriptorMap(
        Map<String, Collection<SystemIndexDescriptor>> pluginAndModulesMap
    ) {
        final Map<String, Collection<SystemIndexDescriptor>> map = new HashMap<>(
            pluginAndModulesMap.size() + SERVER_SYSTEM_INDEX_DESCRIPTORS.size()
        );
        map.putAll(pluginAndModulesMap);
        // put the server items last since we expect less of them
        SERVER_SYSTEM_INDEX_DESCRIPTORS.forEach((source, descriptors) -> {
            if (map.putIfAbsent(source, descriptors) != null) {
                throw new IllegalArgumentException(
                    "plugin or module attempted to define the same source [" + source + "] as a built-in system index"
                );
            }
        });
        return Map.copyOf(map);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we may think we don't have some
            // indices but they may not have been restored from the cluster state on disk
            logger.debug("system indices manager waiting until state has been recovered");
            return;
        }

        for (SystemIndexDescriptor descriptor : this.systemIndexDescriptors) {
            if (descriptor.shouldAutoCreate() == false) {
                continue;
            }
            upgradeIndexMetadataIfNecessary(state, descriptor);
        }
    }

    private void upgradeIndexMetadataIfNecessary(ClusterState state, SystemIndexDescriptor descriptor) {
        final Metadata metadata = state.metadata();
        final String indexName = descriptor.getIndexPattern();

        if (metadata.hasIndex(indexName) == false) {
            // System indices are created on-demand
            return;
        }

        final State indexState = calculateIndexState(state, descriptor);

        if (indexState.isIndexUpToDate == false) {
            logger.debug(
                "Index [{}] is not on the current version. Features relying on the index will not be available until the index is upgraded",
                indexState.concreteIndexName
            );
        } else if (indexState.mappingUpToDate == false) {
            logger.info(
                "Index [{}] (alias [{}]) is not up to date. Updating mapping",
                indexState.concreteIndexName,
                descriptor.getAliasName()
            );
            PutMappingRequest request = new PutMappingRequest(indexState.concreteIndexName).source(
                descriptor.getMappings(),
                XContentType.JSON
            );

            final OriginSettingClient originSettingClient = new OriginSettingClient(this.client, descriptor.getOrigin());

            originSettingClient.admin().indices().putMapping(request, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged() == false) {
                        logger.error("Put mapping request for [{}] was not acknowledged", indexState.concreteIndexName);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Put mapping request for [" + indexState.concreteIndexName + "] failed", e);
                }
            });
        } else {
            logger.trace("Index [{}] is up-to-date, no action required", indexState.concreteIndexName);
        }
    }

    private State calculateIndexState(ClusterState state, SystemIndexDescriptor descriptor) {
        String aliasOrIndexName = descriptor.getAliasName();
        if (aliasOrIndexName == null) {
            aliasOrIndexName = descriptor.getIndexPattern();
        }

        final IndexMetadata indexMetadata = resolveConcreteIndex(state.metadata(), aliasOrIndexName);
        assert indexMetadata != null;

        final boolean isIndexUpToDate = INDEX_FORMAT_SETTING.get(indexMetadata.getSettings()) == descriptor.getIndexFormat();

        final boolean isMappingIsUpToDate = checkIndexMappingUpToDate(descriptor, indexMetadata);
        final String concreteIndexName = indexMetadata.getIndex().getName();

        if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
            logger.warn("Index [{}] is closed. This is likely to prevent some features from functioning correctly", concreteIndexName);
        }

        return new State(isIndexUpToDate, isMappingIsUpToDate, concreteIndexName);
    }

    /**
     * Resolves a concrete index name or alias to a {@link IndexMetadata} instance.  Requires
     * that if supplied with an alias, the alias resolves to at most one concrete index.
     */
    private IndexMetadata resolveConcreteIndex(final Metadata metadata, final String aliasOrIndexName) {
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(aliasOrIndexName);
        if (indexAbstraction != null) {
            final List<IndexMetadata> indices = indexAbstraction.getIndices();
            if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && indices.size() > 1) {
                throw new IllegalStateException(
                    "Alias ["
                        + aliasOrIndexName
                        + "] points to more than one index: "
                        + indices.stream().map(imd -> imd.getIndex().getName()).collect(Collectors.toList())
                );
            }
            return indices.get(0);
        }
        return null;
    }

    private boolean checkIndexMappingUpToDate(SystemIndexDescriptor descriptor, IndexMetadata indexMetadata) {
        final MappingMetadata mappingMetadata = indexMetadata.mapping();
        Set<Version> versions = mappingMetadata == null ? Set.of() : Set.of(readMappingVersion(descriptor, mappingMetadata));
        return versions.stream().allMatch(Version.CURRENT::equals);
    }

    @SuppressWarnings("unchecked")
    private Version readMappingVersion(SystemIndexDescriptor descriptor, MappingMetadata mappingMetadata) {
        final String indexName = descriptor.getIndexPattern();
        try {
            Map<String, Object> meta = (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
            if (meta == null) {
                logger.info("Missing _meta field in mapping [{}] of index [{}]", mappingMetadata.type(), indexName);
                throw new IllegalStateException("Cannot read version string in index " + indexName);
            }
            return Version.fromString((String) meta.get(descriptor.getVersionMetaKey()));
        } catch (ElasticsearchParseException e) {
            logger.error(new ParameterizedMessage("Cannot parse the mapping for index [{}]", indexName), e);
            throw new ElasticsearchException("Cannot parse the mapping for index [{}]", e, indexName);
        }
    }

    private static class State {
        final boolean isIndexUpToDate;
        final boolean mappingUpToDate;
        final String concreteIndexName;

        State(boolean isIndexUpToDate, boolean mappingUpToDate, String concreteIndexName) {
            this.isIndexUpToDate = isIndexUpToDate;
            this.mappingUpToDate = mappingUpToDate;
            this.concreteIndexName = concreteIndexName;
        }
    }
}
