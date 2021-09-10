/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.elasticsearch.tools.launchers.JvmOption.isInitialHeapSpecified;
import static org.elasticsearch.tools.launchers.JvmOption.isMaxHeapSpecified;
import static org.elasticsearch.tools.launchers.JvmOption.isMinHeapSpecified;

/**
 * Determines optimal default heap settings based on available system memory and assigned node roles.
 */
public final class MachineDependentHeap {
    private static final long GB = 1024L * 1024L * 1024L; // 1GB
    private static final long MAX_HEAP_SIZE = GB * 31; // 31GB
    private static final long MAX_ML_HEAP_SIZE = GB * 2; // 2GB
    private static final long MIN_HEAP_SIZE = 1024 * 1024 * 128; // 128MB
    private static final int DEFAULT_HEAP_SIZE_MB = 1024;
    private static final String ELASTICSEARCH_YML = "elasticsearch.yml";

    private final SystemMemoryInfo systemMemoryInfo;

    public MachineDependentHeap(SystemMemoryInfo systemMemoryInfo) {
        this.systemMemoryInfo = systemMemoryInfo;
    }

    /**
     * Calculate heap options.
     *
     * @param configDir path to config directory
     * @param userDefinedJvmOptions JVM arguments provided by the user
     * @return final heap options, or an empty collection if user provided heap options are to be used
     * @throws IOException if unable to load elasticsearch.yml
     */
    public List<String> determineHeapSettings(Path configDir, List<String> userDefinedJvmOptions) throws IOException, InterruptedException {
        // TODO: this could be more efficient, to only parse final options once
        final Map<String, JvmOption> finalJvmOptions = JvmOption.findFinalOptions(userDefinedJvmOptions);
        if (isMaxHeapSpecified(finalJvmOptions) || isMinHeapSpecified(finalJvmOptions) || isInitialHeapSpecified(finalJvmOptions)) {
            // User has explicitly set memory settings so we use those
            return Collections.emptyList();
        }

        Path config = configDir.resolve(ELASTICSEARCH_YML);
        try (InputStream in = Files.newInputStream(config)) {
            return determineHeapSettings(in);
        }
    }

    List<String> determineHeapSettings(InputStream config) {
        MachineNodeRole nodeRole = NodeRoleParser.parse(config);

        try {
            long availableSystemMemory = systemMemoryInfo.availableSystemMemory();
            return options(nodeRole.heap(availableSystemMemory));
        } catch (SystemMemoryInfo.SystemMemoryInfoException e) {
            // If unable to determine system memory (ex: incompatible jdk version) fallback to defaults
            return options(DEFAULT_HEAP_SIZE_MB);
        }
    }

    private static List<String> options(int heapSize) {
        return List.of("-Xms" + heapSize + "m", "-Xmx" + heapSize + "m");
    }

    /**
     * Parses role information from elasticsearch.yml and determines machine node role.
     */
    static class NodeRoleParser {
        private static final Set<String> LEGACY_ROLE_SETTINGS = Set.of(
            "node.master",
            "node.ingest",
            "node.data",
            "node.voting_only",
            "node.ml",
            "node.transform",
            "node.remote_cluster_client"
        );

        @SuppressWarnings("unchecked")
        public static MachineNodeRole parse(InputStream config) {
            Yaml yaml = new Yaml(new SafeConstructor());
            Map<String, Object> root;
            try {
                root = yaml.load(config);
            } catch (YAMLException | ClassCastException ex) {
                // Strangely formatted config, so just return defaults and let startup settings validation catch the problem
                return MachineNodeRole.UNKNOWN;
            }

            if (root != null) {
                Map<String, Object> map = flatten(root, null);

                if (hasLegacySettings(map.keySet())) {
                    // We don't attempt to auto-determine heap if legacy role settings are used
                    return MachineNodeRole.UNKNOWN;
                } else {
                    List<String> roles = null;
                    try {
                        if (map.containsKey("node.roles")) {
                            roles = (List<String>) map.get("node.roles");
                        }
                    } catch (ClassCastException ex) {
                        return MachineNodeRole.UNKNOWN;
                    }

                    if (roles == null || roles.isEmpty()) {
                        // If roles are missing or empty (coordinating node) assume defaults and consider this a data node
                        return MachineNodeRole.DATA;
                    } else if (containsOnly(roles, "master")) {
                        return MachineNodeRole.MASTER_ONLY;
                    } else if (roles.contains("ml") && containsOnly(roles, "ml", "remote_cluster_client")) {
                        return MachineNodeRole.ML_ONLY;
                    } else {
                        return MachineNodeRole.DATA;
                    }
                }
            } else { // if the config is completely empty, then assume defaults and consider this a data node
                return MachineNodeRole.DATA;
            }
        }

        /**
         * Flattens a nested configuration structure. This creates a consistent way of referencing settings from a config file that uses
         * a mix of object and flat setting notation. The returned map is a single-level deep structure of dot-notation property names
         * to values.
         *
         * <p>No attempt is made to deterministically deal with duplicate settings, nor are they explicitly disallowed.
         *
         * @param config nested configuration map
         * @param parentPath parent node path or {@code null} if parsing the root node
         * @return flattened configuration map
         */
        @SuppressWarnings("unchecked")
        private static Map<String, Object> flatten(Map<String, Object> config, String parentPath) {
            Map<String, Object> flatMap = new HashMap<>();
            String prefix = parentPath != null ? parentPath + "." : "";

            for (Map.Entry<String, Object> entry : config.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    flatMap.putAll(flatten((Map<String, Object>) entry.getValue(), prefix + entry.getKey()));
                } else {
                    flatMap.put(prefix + entry.getKey(), entry.getValue());
                }
            }

            return flatMap;
        }

        @SuppressWarnings("unchecked")
        private static <T> boolean containsOnly(Collection<T> collection, T... items) {
            return Arrays.asList(items).containsAll(collection);
        }

        private static boolean hasLegacySettings(Set<String> keys) {
            return LEGACY_ROLE_SETTINGS.stream().anyMatch(keys::contains);
        }
    }

    enum MachineNodeRole {
        /**
         * Master-only node.
         *
         * <p>Heap is computed as 60% of total system memory up to a maximum of 31 gigabytes.
         */
        MASTER_ONLY(m -> mb(min((long) (m * .6), MAX_HEAP_SIZE))),

        /**
         * Machine learning only node.
         *
         * <p>Heap is computed as:
         * <ul>
         *     <li>40% of total system memory when less than 2 gigabytes.</li>
         *     <li>25% of total system memory when greater than 2 gigabytes up to a maximum of 2 gigabytes.</li>
         * </ul>
         */
        ML_ONLY(m -> mb(m < (GB * 2) ? (long) (m * .4) : (long) min(m * .25, MAX_ML_HEAP_SIZE))),

        /**
         * Data node. Essentially any node that isn't a master or ML only node.
         *
         * <p>Heap is computed as:
         * <ul>
         *     <li>40% of total system memory when less than 1 gigabyte with a minimum of 128 megabytes.</li>
         *     <li>50% of total system memory when greater than 1 gigabyte up to a maximum of 31 gigabytes.</li>
         * </ul>
         */
        DATA(m -> mb(m < GB ? max((long) (m * .4), MIN_HEAP_SIZE) : min((long) (m * .5), MAX_HEAP_SIZE))),

        /**
         * Unknown role node.
         *
         * <p>Hard-code heap to a default of 1 gigabyte.
         */
        UNKNOWN(m -> DEFAULT_HEAP_SIZE_MB);

        private final Function<Long, Integer> formula;

        MachineNodeRole(Function<Long, Integer> formula) {
            this.formula = formula;
        }

        /**
         * Determine the appropriate heap size for the given role and available system memory.
         *
         * @param systemMemory total available system memory in bytes
         * @return recommended heap size in megabytes
         */
        public int heap(long systemMemory) {
            return formula.apply(systemMemory);
        }

        private static int mb(long bytes) {
            return (int) (bytes / (1024 * 1024));
        }
    }
}
