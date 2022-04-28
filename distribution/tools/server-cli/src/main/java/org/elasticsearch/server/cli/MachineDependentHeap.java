/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.elasticsearch.server.cli.JvmOption.isInitialHeapSpecified;
import static org.elasticsearch.server.cli.JvmOption.isMaxHeapSpecified;
import static org.elasticsearch.server.cli.JvmOption.isMinHeapSpecified;

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
        long availableSystemMemory = systemMemoryInfo.availableSystemMemory();
        return options(nodeRole.heap(availableSystemMemory));
    }

    private static List<String> options(int heapSize) {
        return List.of("-Xms" + heapSize + "m", "-Xmx" + heapSize + "m");
    }

    /**
     * Parses role information from elasticsearch.yml and determines machine node role.
     */
    static class NodeRoleParser {

        @SuppressWarnings("unchecked")
        public static MachineNodeRole parse(InputStream config) {
            final Settings settings;
            try {
                var parser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, config);
                if (parser.currentToken() == null && parser.nextToken() == null) {
                    settings = null;
                } else {
                    settings = Settings.fromXContent(parser);
                }
            } catch (IOException | ParsingException ex) {
                // Strangely formatted config, so just return defaults and let startup settings validation catch the problem
                return MachineNodeRole.UNKNOWN;
            }

            if (settings != null && settings.isEmpty() == false) {
                List<String> roles = settings.getAsList("node.roles");

                if (roles.isEmpty()) {
                    // If roles are missing or empty (coordinating node) assume defaults and consider this a data node
                    return MachineNodeRole.DATA;
                } else if (containsOnly(roles, "master")) {
                    return MachineNodeRole.MASTER_ONLY;
                } else if (roles.contains("ml") && containsOnly(roles, "ml", "remote_cluster_client")) {
                    return MachineNodeRole.ML_ONLY;
                } else {
                    return MachineNodeRole.DATA;
                }
            } else { // if the config is completely empty, then assume defaults and consider this a data node
                return MachineNodeRole.DATA;
            }
        }

        @SuppressWarnings("unchecked")
        private static <T> boolean containsOnly(Collection<T> collection, T... items) {
            return Arrays.asList(items).containsAll(collection);
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
